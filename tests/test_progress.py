"""Progress-consistency tests.

The monitor page's bars must reflect only verifiably completed work: stage
progress counts succeeded tasks (not running ones), shuffle progress counts
finished partitions (not merely planned ones), task progress never moves
backwards under out-of-order updates, and concurrent jobs never interfere on
a shared worker.
"""

import shutil
import tempfile
import threading
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.metrics import Metrics
from backend.master.registry import WorkerRegistry
from backend.master.scheduler import Scheduler
from backend.master.shuffle import ShuffleCoordinator
from backend.worker.executor import Executor


def _submit(jm, **over):
    payload = {
        "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
        "num_map_tasks": 3, "num_reduce_tasks": 2, "input_rows": 100, "params": {},
    }
    payload.update(over)
    return jm.submit(payload)


class ClusterFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig()
        self.logbus = LogBus(self.storage)
        self.jm = JobManager(self.storage, self.config, self.logbus)
        self.registry = WorkerRegistry(self.storage, self.config)
        self.metrics = Metrics(self.storage)
        self.shuffle = ShuffleCoordinator(self.storage, self.jm, self.registry, self.logbus)
        self.ft = FaultTolerance(self.storage, self.jm, self.config, self.logbus)
        self.sched = Scheduler(self.storage, self.jm, self.registry, self.shuffle,
                               self.ft, self.metrics, self.config, self.logbus)
        self.job = _submit(self.jm)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestStageProgress(ClusterFixture):
    def test_running_tasks_do_not_count_as_done(self):
        for tid in ("m-0000", "m-0001", "m-0002"):
            self.jm.update_task(self.job.job_id, tid, status=C.TASK_RUNNING, progress=0.9)
        sp = self.jm.stage_progress(self.jm.get_job(self.job.job_id))
        self.assertEqual(sp["map"]["done"], 0)
        self.assertEqual(sp["map"]["pct"], 0.0)

    def test_done_matches_succeeded_exactly(self):
        self.jm.update_task(self.job.job_id, "m-0000", status=C.TASK_SUCCEEDED)
        self.jm.update_task(self.job.job_id, "m-0001", status=C.TASK_RUNNING, progress=0.5)
        sp = self.jm.stage_progress(self.jm.get_job(self.job.job_id))
        self.assertEqual(sp["map"]["done"], 1)
        self.assertAlmostEqual(sp["map"]["pct"], 33.3)

    def test_retry_never_decrements_done(self):
        self.jm.update_task(self.job.job_id, "m-0000", status=C.TASK_SUCCEEDED)
        before = self.jm.stage_progress(self.jm.get_job(self.job.job_id))["map"]["done"]
        # A running sibling fails back to RETRYING: done must stay put.
        self.jm.update_task(self.job.job_id, "m-0001", status=C.TASK_RUNNING, progress=0.8)
        self.jm.update_task(self.job.job_id, "m-0001", status=C.TASK_RETRYING, progress=0.0)
        after = self.jm.stage_progress(self.jm.get_job(self.job.job_id))["map"]["done"]
        self.assertEqual(before, after)

    def test_full_completion_is_exactly_100(self):
        for tid in ("m-0000", "m-0001", "m-0002"):
            self.jm.update_task(self.job.job_id, tid, status=C.TASK_SUCCEEDED)
        sp = self.jm.stage_progress(self.jm.get_job(self.job.job_id))
        self.assertEqual(sp["map"]["pct"], 100.0)


class TestTaskStatusUpdates(ClusterFixture):
    def _status(self, task_id, progress, records=0):
        self.sched.on_task_status({
            "job_id": self.job.job_id, "task_id": task_id, "worker_id": "w1",
            "status": C.TASK_RUNNING, "progress": progress,
            "records_processed": records, "records_emitted": 0,
        })

    def test_out_of_order_update_never_regresses(self):
        self._status("m-0000", 0.8, records=80)
        self._status("m-0000", 0.3, records=30)  # stale post arrives late
        task = self.jm.get_task(self.job.job_id, "m-0000")
        self.assertEqual(task.status, C.TASK_RUNNING)
        self.assertEqual(task.progress, 0.8)
        self.assertEqual(task.records_processed, 80)

    def test_late_update_after_completion_is_ignored(self):
        self._status("m-0000", 0.5, records=50)
        self.sched.on_task_complete({
            "job_id": self.job.job_id, "task_id": "m-0000", "worker_id": "w1",
            "kind": C.TASK_MAP, "status": C.TASK_SUCCEEDED,
            "records_processed": 100, "records_emitted": 90, "duration_ms": 5,
            "partition_sizes": {}, "results": [],
        })
        self._status("m-0000", 0.1, records=10)  # duplicate/loser post
        task = self.jm.get_task(self.job.job_id, "m-0000")
        self.assertEqual(task.status, C.TASK_SUCCEEDED)
        self.assertEqual(task.progress, 1.0)
        self.assertEqual(task.records_processed, 100)


class TestShuffleProgress(ClusterFixture):
    def test_ready_partitions_are_not_done(self):
        for p, status in ((0, "ready"), (1, "done")):
            self.storage.write({
                "job_id": self.job.job_id, "partition": p,
                "partition_name": f"part-{p:04d}", "sources": [],
                "num_sources": 0, "total_bytes": 0, "status": status,
            }, "jobs", self.job.job_id, "shuffle", f"part-{p:04d}.json")
        m = self.shuffle.matrix(self.job)
        self.assertEqual(m["num_partitions"], 2)
        self.assertEqual(m["partitions_done"], 1)
        self.assertEqual(m["progress_pct"], 50.0)


class TestExecutorJobIsolation(unittest.TestCase):
    """Two jobs may hold same-named tasks (``m-0000``) on one worker."""

    def test_same_task_id_from_two_jobs_coexists(self):
        tmp = tempfile.mkdtemp()
        try:
            ex = Executor("w1", tmp, "http://127.0.0.1:9", ClusterConfig(),
                          exec_mode="thread")
            release = threading.Event()
            ex._run_thread = lambda key: release.wait(2.0)  # keep handles occupied
            spec_a = {"task_id": "m-0000", "job_id": "jobA", "kind": "map"}
            spec_b = {"task_id": "m-0000", "job_id": "jobB", "kind": "map"}
            self.assertTrue(ex.start_task(spec_a))
            self.assertTrue(ex.start_task(spec_b))         # no cross-job rejection
            self.assertFalse(ex.start_task(dict(spec_a)))  # same job+task still rejected
            self.assertEqual(ex.running_count, 2)
            # Cancellation is scoped to its own job.
            self.assertTrue(ex.cancel("jobA", "m-0000"))
            self.assertFalse(ex.cancel("jobA", "m-0001"))
            self.assertFalse(ex.cancel("jobC", "m-0000"))
            release.set()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
