import dotenv from 'dotenv';
import { ScheduledRunStore, type ScheduledRunRecord } from '../src/scheduler/store';
import { supportedSites, type SupportedSite } from '../src/sites';

dotenv.config();

const MIN_OBSERVED_RUNS = 15;
const ROLLING_SUCCESSFUL_RUNS = 20;

interface SiteStrength {
  site: SupportedSite;
  window_days: number;
  observed_runs: number;
  successful_runs: number;
  successful_sla_runs: number;
  success_rate_pct: number;
  rolling_successful_runs: number;
  rolling_sla_pass_rate_pct: number;
  expected_weekdays: number;
  cadence_ok_days: number;
  cadence_hit_rate_pct: number;
  avg_sla_score_pct: number;
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

function mean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function parseScheduledSlot(run: ScheduledRunRecord): { dateKey: string; slot: 'morning' | 'afternoon' | 'evening' } | null {
  const sources = [run.idempotency_key, run.slot_time];
  for (const source of sources) {
    const match = source.match(/:(\d{4}-\d{2}-\d{2}):(morning|afternoon|evening)(?::|$)/i);
    if (match) {
      return {
        dateKey: match[1],
        slot: match[2] as 'morning' | 'afternoon' | 'evening',
      };
    }
  }
  return null;
}

function isComplianceEligibleRun(run: ScheduledRunRecord): boolean {
  return parseScheduledSlot(run) !== null && run.partial_reason !== 'debug_bootstrap_only';
}

function expectedWeekdays(lookbackDays: number): number {
  const now = new Date();
  const start = new Date(now);
  start.setUTCDate(now.getUTCDate() - (lookbackDays - 1));

  let weekdays = 0;
  for (const cursor = new Date(start); cursor <= now; cursor.setUTCDate(cursor.getUTCDate() + 1)) {
    const day = cursor.getUTCDay();
    if (day >= 1 && day <= 5) weekdays += 1;
  }

  return weekdays;
}

function buildConfidence(summary: Omit<SiteStrength, 'site' | 'window_days' | 'confidence'>): SiteStrength['confidence'] {
  if (summary.observed_runs < MIN_OBSERVED_RUNS || summary.rolling_successful_runs < ROLLING_SUCCESSFUL_RUNS) {
    return 'insufficient_data';
  }
  if (summary.rolling_sla_pass_rate_pct >= 95 && summary.cadence_hit_rate_pct >= 95) {
    return 'strong';
  }
  if (summary.rolling_sla_pass_rate_pct >= 80 && summary.cadence_hit_rate_pct >= 75) {
    return 'medium';
  }
  return 'weak';
}

function printTable(rows: SiteStrength[]): void {
  console.log('site\tsuccess%\trolling_sla%\tcadence%\tavg_sla%\tavg_amount%\tavg_row_fail%\tconfidence');
  for (const row of rows) {
    console.log(
      [
        row.site,
        row.success_rate_pct.toFixed(2),
        row.rolling_sla_pass_rate_pct.toFixed(2),
        row.cadence_hit_rate_pct.toFixed(2),
        row.avg_sla_score_pct.toFixed(2),
        row.avg_amount_coverage_pct.toFixed(2),
        row.avg_row_fail_pct.toFixed(2),
        row.confidence,
      ].join('\t'),
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
        .filter((run) => run.started_at && new Date(run.started_at).getTime() >= cutoff.getTime())
        .filter(isComplianceEligibleRun);

      const successfulRuns = runs.filter((run) => run.status === 'success');
      const rollingSuccessfulRuns = successfulRuns.slice(0, ROLLING_SUCCESSFUL_RUNS);
      const successfulSlaRuns = successfulRuns.filter((run) => (run.sla_pass ?? 0) === 1);
      const rollingSlaPassRatePct = rollingSuccessfulRuns.length > 0
        ? round((rollingSuccessfulRuns.filter((run) => (run.sla_pass ?? 0) === 1).length / rollingSuccessfulRuns.length) * 100)
        : 0;

      const cadenceByDay = new Map<string, Set<'morning' | 'afternoon' | 'evening'>>();
      for (const run of successfulRuns) {
        if ((run.sla_pass ?? 0) !== 1) continue;
        const slot = parseScheduledSlot(run);
        if (!slot) continue;
        const slots = cadenceByDay.get(slot.dateKey) ?? new Set<'morning' | 'afternoon' | 'evening'>();
        slots.add(slot.slot);
        cadenceByDay.set(slot.dateKey, slots);
      }

      const cadenceOkDays = Array.from(cadenceByDay.values()).filter((slots) => slots.size === 3).length;
      const expectedWeekdayCount = expectedWeekdays(lookbackDays);

      const partial = {
        observed_runs: runs.length,
        successful_runs: successfulRuns.length,
        successful_sla_runs: successfulSlaRuns.length,
        success_rate_pct: runs.length > 0 ? round((successfulRuns.length / runs.length) * 100) : 0,
        rolling_successful_runs: rollingSuccessfulRuns.length,
        rolling_sla_pass_rate_pct: rollingSlaPassRatePct,
        expected_weekdays: expectedWeekdayCount,
        cadence_ok_days: cadenceOkDays,
        cadence_hit_rate_pct: expectedWeekdayCount > 0 ? round((cadenceOkDays / expectedWeekdayCount) * 100) : 0,
        avg_sla_score_pct: round(mean(successfulRuns.map((run) => run.sla_score_pct ?? 0))),
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
      thresholds: {
        minimum_observed_runs: MIN_OBSERVED_RUNS,
        rolling_successful_runs: ROLLING_SUCCESSFUL_RUNS,
      },
      notes: {
        cadence_basis: 'weekday cadence is measured as 3 SLA-passing slots (morning/afternoon/evening) per weekday using scheduled idempotency keys',
        quality_rule: 'persisted sla_pass and sla_score_pct on scheduled runs',
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
