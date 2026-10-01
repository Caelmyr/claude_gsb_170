"""Tests for progress accounting and execution identity isolation."""

import shutil
import tempfile
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.job_manager import JobManager
from backend.master.scheduler import Scheduler
from backend.worker.executor import Executor


class TestProgress(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.jm = JobManager(self.storage, ClusterConfig(), LogBus(self.storage))
        self.job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 4, "num_reduce_tasks": 2, "input_rows": 100, "params": {},
        })

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_running_tasks_do_not_count_as_stage_completion(self):
        tasks = self.jm.tasks_for(self.job.job_id, C.TASK_MAP)
        self.jm.update_task(self.job.job_id, tasks[0].task_id, status=C.TASK_ASSIGNED)
        self.jm.update_task(self.job.job_id, tasks[1].task_id, status=C.TASK_RUNNING, progress=0.9)
        self.jm.update_task(self.job.job_id, tasks[2].task_id, status=C.TASK_RUNNING, progress=1.0)
        self.jm.update_task(self.job.job_id, tasks[3].task_id, status=C.TASK_RETRYING)

        progress = self.jm.stage_progress(self.jm.get_job(self.job.job_id))["map"]
        self.assertEqual(progress["done"], 0)
        self.assertEqual(progress["total"], 4)
        self.assertEqual(progress["pct"], 0.0)

    def test_stage_progress_only_counts_succeeded_and_snapshot_is_consistent(self):
        tasks = self.jm.tasks_for(self.job.job_id, C.TASK_MAP)
        for task in tasks[:3]:
            self.jm.update_task(self.job.job_id, task.task_id, status=C.TASK_SUCCEEDED, progress=1.0)

        snapshot = self.jm.job_snapshot(self.job.job_id)
        self.assertIsNotNone(snapshot)
        summary, snapshot_tasks = snapshot
        self.assertEqual(len(snapshot_tasks), 6)
        self.assertEqual(summary["stage_progress"]["map"]["done"], 3)
        self.assertEqual(summary["stage_progress"]["map"]["pct"], 75.0)
        self.assertEqual(summary["task_status"][C.TASK_SUCCEEDED], 3)
        self.assertEqual(summary["task_status"][C.TASK_PENDING], 3)

    def test_shuffle_starts_at_zero_until_job_leaves_map(self):
        self.assertEqual(self.jm.stage_progress(self.job)["shuffle"]["pct"], 0.0)
        self.jm.set_job_status(self.job, C.JOB_SHUFFLE)
        shuffle = self.jm.stage_progress(self.jm.get_job(self.job.job_id))["shuffle"]
        self.assertEqual((shuffle["done"], shuffle["total"], shuffle["pct"]), (2, 2, 100.0))


class TestStaleReports(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig(max_attempts=3)
        self.jm = JobManager(self.storage, self.config, LogBus(self.storage))
        self.scheduler = Scheduler(
            self.storage, self.jm, registry=None, shuffle=None,
            fault_tolerance=None, metrics=None, config=self.config,
            logbus=LogBus(self.storage),
        )
        self.job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 2, "num_reduce_tasks": 1, "input_rows": 50, "params": {},
        })
        self.task = self.jm.tasks_for(self.job.job_id, C.TASK_MAP)[0]

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_old_attempt_progress_cannot_move_retry_backwards(self):
        self.jm.update_task(self.job.job_id, self.task.task_id,
                            status=C.TASK_ASSIGNED, attempts=1, worker_id="w1")
        self.scheduler.on_task_status({
            "job_id": self.job.job_id, "task_id": self.task.task_id, "attempt": 1,
            "worker_id": "w1", "progress": 0.8, "records_processed": 80,
        })
        self.jm.update_task(self.job.job_id, self.task.task_id,
                            status=C.TASK_RETRYING, attempts=1, worker_id=None, progress=0.0,
                            records_processed=0, records_emitted=0, started_ms=0)

        # A delayed status from the dead first attempt must be ignored.
        self.scheduler.on_task_status({
            "job_id": self.job.job_id, "task_id": self.task.task_id, "attempt": 1,
            "worker_id": "w1", "progress": 0.9, "records_processed": 90,
        })
        task = self.jm.get_task(self.job.job_id, self.task.task_id)
        self.assertEqual(task.status, C.TASK_RETRYING)
        self.assertEqual(task.progress, 0.0)
        self.assertEqual(task.records_processed, 0)

        self.jm.update_task(self.job.job_id, self.task.task_id,
                            status=C.TASK_ASSIGNED, attempts=2, worker_id="w2")
        self.scheduler.on_task_status({
            "job_id": self.job.job_id, "task_id": self.task.task_id, "attempt": 2,
            "worker_id": "w2", "progress": 0.2, "records_processed": 20,
        })
        task = self.jm.get_task(self.job.job_id, self.task.task_id)
        self.assertEqual(task.status, C.TASK_RUNNING)
        self.assertEqual(task.progress, 0.2)
        self.assertEqual(task.records_processed, 20)


class TestWorkerJobIsolation(unittest.TestCase):
    def test_same_task_id_from_different_jobs_runs_concurrently(self):
        tmp = tempfile.mkdtemp()
        try:
            executor = Executor("w1", tmp, "http://127.0.0.1:0", ClusterConfig(), exec_mode="thread")
            base = {
                "task_id": "m-0000",
                "kind": C.TASK_MAP,
                "mapper": "wordcount_mapper",
                "reducer": "count_reducer",
                "params": {},
                "partition_count": 1,
                "records": [{"text": "hello world"}],
                "attempt": 0,
            }
            self.assertTrue(executor.start_task({**base, "job_id": "job-a"}))
            self.assertTrue(executor.start_task({**base, "job_id": "job-b"}))
            running = executor.running_task_ids()
            self.assertEqual(len(running), 2)
            self.assertEqual({item["job_id"] for item in running}, {"job-a", "job-b"})
            self.assertTrue(executor.cancel("job-a", "m-0000"))
            self.assertTrue(executor.cancel("job-b", "m-0000"))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
