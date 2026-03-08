import dotenv from 'dotenv';
import { scrapers } from '../src/scraper';

dotenv.config();

const TARGET_COVERAGE = Number(process.env.AMOUNT_MIN_COVERAGE_PCT ?? '95');
const FLOOR = Number(process.env.SCHEDULE_MAX_RECORDS_FLOOR ?? '25');
const CEILING = Number(process.env.SCHEDULE_MAX_RECORDS_CEILING ?? '1000');
const STEP = Number(process.env.CALIBRATION_STEP ?? '25');
const DEADLINE_MINUTES = Number(process.env.CALIBRATION_DEADLINE_MINUTES ?? '240');

function formatDate(d: Date): string {
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function computeRange(): { date_start: string; date_end: string } {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - 7);
  return { date_start: formatDate(start), date_end: formatDate(end) };
}

async function evaluateCap(maxRecords: number): Promise<{ pass: boolean; coverage: number; records: number; durationSec: number }> {
  const scraper = (scrapers as any).ca_sos;
  const started = Date.now();

  const records = await scraper({
    ...computeRange(),
    max_records: maxRecords,
    stop_requested: () => (Date.now() - started) > DEADLINE_MINUTES * 60 * 1000,
  });

  const durationSec = (Date.now() - started) / 1000;
  const amountFound = records.filter((r: any) => Boolean(r.amount)).length;
  const coverage = records.length > 0 ? (amountFound / records.length) * 100 : 0;
  const pass = coverage >= TARGET_COVERAGE && durationSec <= DEADLINE_MINUTES * 60;

  return { pass, coverage, records: records.length, durationSec };
}

async function main() {
  let best = FLOOR;
  let candidate = FLOOR;

  console.log(`Calibration start floor=${FLOOR} ceiling=${CEILING} target_coverage=${TARGET_COVERAGE}%`);

  while (candidate <= CEILING) {
    const result = await evaluateCap(candidate);
    console.log(`step cap=${candidate} pass=${result.pass} coverage=${result.coverage.toFixed(2)} records=${result.records} duration=${result.durationSec.toFixed(1)}s`);

    if (!result.pass) break;
    best = candidate;
    candidate += STEP;
  }

  let low = best;
  let high = Math.min(candidate, CEILING);

  while (high - low > 5) {
    const mid = Math.floor((low + high) / 2);
    const result = await evaluateCap(mid);
    console.log(`binary cap=${mid} pass=${result.pass} coverage=${result.coverage.toFixed(2)} records=${result.records} duration=${result.durationSec.toFixed(1)}s`);

    if (result.pass) {
      low = mid;
    } else {
      high = mid;
    }
  }

  console.log(`Recommended SCHEDULE_MAX_RECORDS=${low}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
