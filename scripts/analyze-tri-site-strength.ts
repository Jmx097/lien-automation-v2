import dotenv from 'dotenv';
import { ScheduledRunStore, type ScheduledRunRecord } from '../src/scheduler/store';
import { supportedSites, type SupportedSite } from '../src/sites';

dotenv.config();

interface SiteStrength {
  site: SupportedSite;
  window_days: number;
  total_runs: number;
  successful_runs: number;
  success_rate_pct: number;
  expected_slots: number;
  observed_success_slots: number;
  cadence_hit_rate_pct: number;
  quality_pass_runs: number;
  quality_pass_rate_pct: number;
  avg_amount_coverage_pct: number;
  avg_ocr_success_pct: number;
  avg_row_fail_pct: number;
  confidence: 'strong' | 'medium' | 'weak' | 'insufficient_data';
}

function round(value: number, digits = 2): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function parseLookbackDays(): number {
  const parsed = Number.parseInt(process.env.TRI_SITE_LOOKBACK_DAYS ?? '14', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 14;
}

function parseLimit(): number {
  const parsed = Number.parseInt(process.env.TRI_SITE_HISTORY_LIMIT ?? '1000', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 1000;
}

function toDateKey(iso: string): string {
  return iso.slice(0, 10);
}

function parseSlot(run: ScheduledRunRecord): 'morning' | 'afternoon' | 'evening' | 'unknown' {
  const key = run.idempotency_key ?? '';
  if (key.endsWith(':morning')) return 'morning';
  if (key.endsWith(':afternoon')) return 'afternoon';
  if (key.endsWith(':evening')) return 'evening';
  if (run.slot_time.includes('morning')) return 'morning';
  if (run.slot_time.includes('afternoon')) return 'afternoon';
  if (run.slot_time.includes('evening')) return 'evening';
  return 'unknown';
}

function expectedWeekdaySlots(lookbackDays: number): number {
  const now = new Date();
  const start = new Date(now);
  start.setUTCDate(now.getUTCDate() - (lookbackDays - 1));

  let weekdays = 0;
  for (let cursor = new Date(start); cursor <= now; cursor.setUTCDate(cursor.getUTCDate() + 1)) {
    const day = cursor.getUTCDay();
    if (day >= 1 && day <= 5) weekdays += 1;
  }

  return weekdays * 3;
}

function buildConfidence(summary: Omit<SiteStrength, 'site' | 'window_days' | 'confidence'>): SiteStrength['confidence'] {
  if (summary.total_runs === 0) return 'insufficient_data';
  if (
    summary.success_rate_pct >= 95 &&
    summary.cadence_hit_rate_pct >= 90 &&
    summary.quality_pass_rate_pct >= 95
  ) {
    return 'strong';
  }

  if (
    summary.success_rate_pct >= 85 &&
    summary.cadence_hit_rate_pct >= 75 &&
    summary.quality_pass_rate_pct >= 80
  ) {
    return 'medium';
  }

  return 'weak';
}

function mean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function printTable(rows: SiteStrength[]): void {
  console.log('site\tsuccess%\tcadence%\tquality%\tavg_amount%\tavg_row_fail%\tconfidence');
  for (const row of rows) {
    console.log(
      [
        row.site,
        row.success_rate_pct.toFixed(2),
        row.cadence_hit_rate_pct.toFixed(2),
        row.quality_pass_rate_pct.toFixed(2),
        row.avg_amount_coverage_pct.toFixed(2),
        row.avg_row_fail_pct.toFixed(2),
        row.confidence,
      ].join('\t')
    );
  }
}

async function main(): Promise<void> {
  const lookbackDays = parseLookbackDays();
  const historyLimit = parseLimit();
  const cutoff = new Date();
  cutoff.setUTCDate(cutoff.getUTCDate() - lookbackDays);

  const store = new ScheduledRunStore();
  try {
    const summaries: SiteStrength[] = [];

    for (const site of supportedSites) {
      const runs = (await store.getRunHistory(historyLimit, site))
        .filter((run) => run.started_at && new Date(run.started_at).getTime() >= cutoff.getTime());

      const totalRuns = runs.length;
      const successfulRuns = runs.filter((run) => run.status === 'success');
      const expectedSlots = expectedWeekdaySlots(lookbackDays);
      const observedSlotKeys = new Set(
        successfulRuns
          .map((run) => {
            const slot = parseSlot(run);
            const day = toDateKey(run.started_at);
            return slot === 'unknown' ? null : `${day}:${slot}`;
          })
          .filter((value): value is string => value !== null)
      );

      const qualityPassRuns = successfulRuns.filter(
        (run) => run.amount_coverage_pct >= 95 && run.row_fail_pct <= 5 && run.retry_exhausted !== 1
      );

      const partial = {
        total_runs: totalRuns,
        successful_runs: successfulRuns.length,
        success_rate_pct: totalRuns > 0 ? round((successfulRuns.length / totalRuns) * 100) : 0,
        expected_slots: expectedSlots,
        observed_success_slots: observedSlotKeys.size,
        cadence_hit_rate_pct: expectedSlots > 0 ? round((observedSlotKeys.size / expectedSlots) * 100) : 0,
        quality_pass_runs: qualityPassRuns.length,
        quality_pass_rate_pct: successfulRuns.length > 0 ? round((qualityPassRuns.length / successfulRuns.length) * 100) : 0,
        avg_amount_coverage_pct: round(mean(successfulRuns.map((run) => run.amount_coverage_pct))),
        avg_ocr_success_pct: round(mean(successfulRuns.map((run) => run.ocr_success_pct))),
        avg_row_fail_pct: round(mean(successfulRuns.map((run) => run.row_fail_pct))),
      };

      summaries.push({
        site,
        window_days: lookbackDays,
        ...partial,
        confidence: buildConfidence(partial),
      });
    }

    const payload = {
      generated_at: new Date().toISOString(),
      lookback_days: lookbackDays,
      history_limit: historyLimit,
      notes: {
        cadence_basis: 'weekday_slots=(Mon-Fri)*3 based on UTC dates in started_at',
        quality_rule: 'success + amount_coverage_pct>=95 + row_fail_pct<=5 + retry_exhausted!=1',
      },
      sites: summaries,
    };

    printTable(summaries);
    console.log('\n' + JSON.stringify(payload, null, 2));
  } finally {
    await store.close();
  }
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.stack ?? err.message : String(err));
  process.exitCode = 1;
});
