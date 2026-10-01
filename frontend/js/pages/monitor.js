/* 作业监控 Monitor */
Components.init('monitor');
const C = Components;

let currentJob = '';
let kindFilter = '';
let lastData = null;
let renderSeq = 0;

function setupFilters() {
  const kinds = [['', '全部 All'], ['map', 'Map'], ['reduce', 'Reduce']];
  document.getElementById('kind-filter').innerHTML = kinds.map(([k, label]) =>
    `<span class="pill${kindFilter === k ? ' active' : ''}" data-k="${k}">${label}</span>`).join('');
  document.querySelectorAll('#kind-filter .pill').forEach(p => {
    p.addEventListener('click', () => { kindFilter = p.getAttribute('data-k'); setupFilters(); render(); });
  });
}

async function render() {
  if (!currentJob) return;
  const seq = ++renderSeq;
  let data;
  try {
    data = await API.get('/api/jobs/' + currentJob);
  } catch (e) { return; }
  // Polls can finish out of order after switching jobs; never paint stale data.
  if (seq !== renderSeq || data.job.job_id !== currentJob) return;
  lastData = data;
  const job = data.job;
  const tasks = data.tasks || [];

  // Summary + stage progress. Percentages are authoritative server values:
  // done/total is based solely on SUCCEEDED tasks.
  const sp = job.stage_progress || {};
  const stageBar = (key, label) => {
    const st = sp[key] || { done: 0, total: 0, pct: 0 };
    return C.progress(st.pct || 0, `${label} (${st.done || 0}/${st.total || 0})`);
  };
  document.getElementById('summary').innerHTML = `
    <div class="stat-tiles">
      <div class="stat"><div class="label">作业 Job</div><div class="value" style="font-size:20px">${C.esc(job.name)}</div>
        <div class="delta mono">${C.esc(job.job_id)}</div></div>
      <div class="stat"><div class="label">状态 Status</div><div class="value" style="font-size:20px">${C.stateBadge(job.status, true)}</div></div>
      <div class="stat"><div class="label">Map 任务 Tasks</div><div class="value">${job.num_map_tasks}</div></div>
      <div class="stat"><div class="label">Reduce 任务 Tasks</div><div class="value">${job.num_reduce_tasks}</div></div>
      <div class="stat"><div class="label">输入记录 Records</div><div class="value">${C.fmtNum(job.input_rows)}</div></div>
      <div class="stat"><div class="label">故障事件 Faults</div><div class="value ${job.fault_count ? 'bad' : ''}">${job.fault_count || 0}</div></div>
    </div>
    <div class="card mt">
      <div class="grid cols-3">
        <div>${stageBar('map', 'Map')}</div>
        <div>${stageBar('shuffle', 'Shuffle')}</div>
        <div>${stageBar('reduce', 'Reduce')}</div>
      </div>
    </div>`;

  // Tasks table
  const filtered = tasks.filter(t => !kindFilter || t.kind === kindFilter);
  document.getElementById('tasks').innerHTML = filtered.length ? C.table([
    { key: 'task_id', label: '任务 Task', render: r => `<span class="mono">${C.esc(r.task_id)}</span>` },
    { key: 'kind', label: '类型 Kind', render: r => r.kind },
    { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
    { key: 'worker_name', label: 'Worker', render: r => C.esc(r.worker_name || r.worker_id || '-') },
    { key: 'attempts', label: '尝试 Att', render: r => r.attempts, num: true },
    { key: 'progress', label: '进度 Progress', render: r => {
      // The stage bars count only completed tasks. Per-task progress is an
      // execution hint and can never render 100% unless that task is SUCCEEDED.
      const pct = r.status === 'SUCCEEDED' ? 100 : Math.min(99, (r.progress || 0) * 100);
      return `<div style="min-width:120px">${C.progress(pct)}</div>`;
    } },
    { key: 'records_processed', label: '处理 Records', render: r => C.fmtNum(r.records_processed), num: true },
    { key: 'duration_ms', label: '耗时 Dur', render: r => C.fmtDur(r.duration_ms), num: true },
  ], filtered) : C.empty();
}

C.jobPicker('job-picker', (id) => { currentJob = id; render(); });
document.getElementById('refresh').addEventListener('click', render);
setupFilters();
C.poll(render, 2000).start();
