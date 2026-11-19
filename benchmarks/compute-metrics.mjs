import fs from 'node:fs';
import path from 'node:path';

const rawDir = path.resolve('benchmarks/raw');
const configs = ['baseline', 'pool_only', 'keepalive_only', 'pool_keepalive', 'pool_keepalive_optimized'];
const vus = [20, 50, 100];

const tableAccepted = [];
const tableCompleted = [];
const tableDuring = [];
const tableAfter = [];

for (const config of configs) {
  const rowAccepted = { config };
  const rowCompleted = { config };
  const rowDuring = { config };
  const rowAfter = { config };

  for (const vu of vus) {
    const runs = [];
    for (let r = 1; r <= 3; r++) {
      const jsonFile = path.join(rawDir, `${config}-${vu}vu-run${r}.json`);
      const summaryFile = path.join(rawDir, `summary-${config}.json`);
      const data = JSON.parse(fs.readFileSync(jsonFile, 'utf8'));
      const summary = JSON.parse(fs.readFileSync(summaryFile, 'utf8'));
      const runSummary = summary[vu].runs.find(x => x.run === r);

      const acceptedReqs = data.metrics.http_reqs.count;
      const acceptedRate = data.metrics.http_reqs.rate;
      const runDuration = acceptedReqs / acceptedRate;
      const backlog = runSummary.backlog;
      const drainSec = runSummary.drainSec;

      const completedCount = acceptedReqs;
      const totalTime = runDuration + drainSec;
      const completedRate = completedCount / totalTime;

      const eventsDuringLoad = acceptedReqs - backlog;
      const relayRateDuringLoad = eventsDuringLoad / runDuration;
      const relayRateAfterLoad = drainSec > 0 ? backlog / drainSec : 0;

      runs.push({
        acceptedRate,
        completedRate,
        relayRateDuringLoad,
        relayRateAfterLoad
      });
    }

    const fmt = (arr, prop) => {
      const vals = arr.map(x => x[prop]);
      const mean = (vals.reduce((a, b) => a + b, 0) / vals.length).toFixed(2);
      const min = Math.min(...vals).toFixed(2);
      const max = Math.max(...vals).toFixed(2);
      return `${mean} [${min} – ${max}]`;
    };

    rowAccepted[`${vu} VUs`] = fmt(runs, 'acceptedRate');
    rowCompleted[`${vu} VUs`] = fmt(runs, 'completedRate');
    rowDuring[`${vu} VUs`] = fmt(runs, 'relayRateDuringLoad');
    rowAfter[`${vu} VUs`] = fmt(runs, 'relayRateAfterLoad');
  }

  tableAccepted.push(rowAccepted);
  tableCompleted.push(rowCompleted);
  tableDuring.push(rowDuring);
  tableAfter.push(rowAfter);
}

function toMarkdown(title, table) {
  let md = `### ${title}\n\n`;
  md += `| Config | 20 VUs | 50 VUs | 100 VUs |\n`;
  md += `|---|---|---|---|\n`;
  for (const row of table) {
    md += `| \`${row.config}\` | ${row['20 VUs']} | ${row['50 VUs']} | ${row['100 VUs']} |\n`;
  }
  return md;
}

console.log(toMarkdown('Accepted Throughput (accepted req/s) — mean [min – max]', tableAccepted));
console.log(toMarkdown('Completed Throughput (completed checkouts/s) — mean [min – max]', tableCompleted));
console.log(toMarkdown('Relay Publish Rate DURING Load (events/s) — mean [min – max]', tableDuring));
console.log(toMarkdown('Relay Publish Rate AFTER Load (events/s) — mean [min – max]', tableAfter));
