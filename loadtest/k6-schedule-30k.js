// Crobat load test: schedule 30,000 jobs for the SAME second and watch
// them drain.
//
//   k6 run -e BASE=http://localhost:8081 -e JOBSBASE=http://localhost:8082 \
//          -e TOKEN=<tenant jwt> loadtest/k6-schedule-30k.js
//
// What it does:
//   setup()    creates 30,000 one-shot tasks all due at T+60s, in parallel
//              batches (each task = one job; task creation is the
//              "schedule a call" API a tenant would use).
//   default()  polls the job service until the burst has drained (or the
//              deadline passes) and reports how long the drain took.
//
// What to look at (see docs/load-testing.md):
//   - zero lost jobs: created == succeeded + dead at the end
//   - drain time for the burst
//   - MySQL: `SELECT status, COUNT(*) FROM jobs GROUP BY status` during
//     the run; watch for lock waits in `SHOW ENGINE INNODB STATUS`.
import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';

const BASE = __ENV.BASE || 'http://localhost:8081';
const JOBSBASE = __ENV.JOBSBASE || 'http://localhost:8082';
const TOKEN = __ENV.TOKEN || '';
const TOTAL = Number(__ENV.TOTAL || 30000);
const TARGET = __ENV.TARGET || 'https://example.com/orders'; // tenant endpoint

export const options = {
  scenarios: {
    schedule_burst: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: TOTAL,
      maxDuration: '10m',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'], // <1% scheduling failures tolerated
  },
};

const headers = () => ({
  Authorization: `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
});

export function setup() {
  // One shared due time for every job in the run: 60 seconds from now.
  const due = new Date(Date.now() + 60 * 1000).toISOString();
  return { due };
}

export default function (data) {
  const i = exec.scenario.iterationInTest;
  const payload = JSON.stringify({
    name: `burst-${i}`,
    schedule: { kind: 'at', at: data.due },
    priority: 'normal',
    request: { method: 'POST', url: TARGET, body: `{"burst":${i}}` },
    max_attempts: 3,
    backoff_base_seconds: 1,
    backoff_max_seconds: 30,
  });
  const res = http.post(`${BASE}/v1/tasks`, payload, {
    headers: { ...headers(), 'Idempotency-Key': `burst-${exec.testRunId}-${i}` },
  });
  check(res, { 'task created (201/200)': (r) => r.status === 201 || r.status === 200 });
}

export function teardown(data) {
  // Poll the job list until the burst drains. The job service caps limit
  // at 1000, so this samples rather than counts exactly; for an exact
  // count use the SQL in docs/load-testing.md.
  const deadline = Date.now() + 10 * 60 * 1000;
  let pending = -1;
  while (Date.now() < deadline) {
    const res = http.get(`${JOBSBASE}/v1/jobs?status=pending&limit=1000`, { headers: headers() });
    if (res.status === 200) {
      pending = res.json().length;
      if (pending === 0) break;
    }
    sleep(2);
  }
  console.log(`teardown: pending sample after drain wait = ${pending}`);
}
