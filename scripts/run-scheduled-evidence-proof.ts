import dotenv from 'dotenv';
import { runScheduledScrape, getNextRuns, getRunHistory, getScheduleState, type Slot } from '../src/scheduler';
import { supportedSites, type SupportedSite } from '../src/sites';

dotenv.config();

interface ScheduledEvidenceProofSummary {
  generated_at: string;
  requested_slots: Slot[];
  runs: Array<{
    site: SupportedSite;
    slot: Slot;
    idempotency_key: string;
    status: string;
    records_scraped: number;
    rows_uploaded: number;
    source_tab_title?: string;
    master_tab_title?: string;
    review_tab_title?: string;
    confidence_status?: string;
    confidence_reasons?: string[];
  }>;
  next_runs: ReturnType<typeof getNextRuns>;
  state: Awaited<ReturnType<typeof getScheduleState>>;
}

function parseSlots(): Slot[] {
  const raw = (process.env.SCHEDULE_PROOF_SLOTS ?? 'morning').trim();
  const parsed = raw
    .split(',')
    .map((value) => value.trim())
    .filter((value): value is Slot => value === 'morning' || value === 'afternoon' || value === 'evening');

  return parsed.length > 0 ? parsed : ['morning'];
}

async function main(): Promise<void> {
  const slots = parseSlots();
  const runs: ScheduledEvidenceProofSummary['runs'] = [];

  for (const site of supportedSites) {
    for (const slot of slots) {
      const idempotencyKey = `${site}:proof:${new Date().toISOString().slice(0, 10)}:${slot}`;
      const result = await runScheduledScrape({
        site,
        slot,
        idempotencyKey,
        triggerSource: 'manual',
      });

      const history = await getRunHistory(10);
      const persisted = history.find((run) => run.idempotency_key === idempotencyKey);
      runs.push({
        site,
        slot,
        idempotency_key: idempotencyKey,
        status: result.status,
        records_scraped: result.records_scraped,
        rows_uploaded: result.rows_uploaded,
        source_tab_title: persisted?.source_tab_title,
        master_tab_title: persisted?.master_tab_title,
        review_tab_title: persisted?.review_tab_title,
        confidence_status: persisted?.confidence?.status,
        confidence_reasons: persisted?.confidence?.reasons,
      });
    }
  }

  const summary: ScheduledEvidenceProofSummary = {
    generated_at: new Date().toISOString(),
    requested_slots: slots,
    runs,
    next_runs: getNextRuns(),
    state: await getScheduleState(),
  };

  console.log(JSON.stringify(summary, null, 2));
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.stack ?? err.message : String(err));
  process.exitCode = 1;
});
