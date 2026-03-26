import fs from 'fs/promises';
import path from 'path';
import {
  getNextRuns,
  getRunHistory,
  getScheduleState,
} from '../scheduler';
import { supportedSites, type SupportedSite } from '../sites';

export interface TriSiteProofRunSummary {
  id: string;
  idempotency_key: string;
  started_at: string;
  finished_at?: string;
  status: string;
  failure_class?: string;
  partial_reason?: string;
  records_scraped: number;
  rows_uploaded: number;
  sla_score_pct?: number;
  sla_pass?: boolean;
}

export interface TriSiteProofSiteSummary {
  site_compliant: boolean;
  rolling_sla_pass_rate_20: number;
  rolling_sla_status: string;
  rolling_sla_pass_count: number;
  rolling_sla_window_successful_runs: number;
  previous_business_day: string;
  previous_business_day_slot_success_count: number;
  previous_business_day_slots_ok: boolean;
  effective_max_records: number;
  connectivity_status: string;
  next_allowed_run_at?: string;
  latest_failure_reason?: string;
  latest_run_ids: {
    latest_run_id?: string;
    latest_successful_run_id?: string;
    latest_error_run_id?: string;
    latest_deferred_run_id?: string;
    latest_sla_breach_run_id?: string;
  };
  recent_runs: TriSiteProofRunSummary[];
}

export interface TriSiteProofSummary {
  generated_at: string;
  proof_date: string;
  output_path: string;
  overall_status: 'compliant' | 'breached' | 'insufficient_data';
  next_runs: ReturnType<typeof getNextRuns>;
  sites: Record<SupportedSite, TriSiteProofSiteSummary>;
}

interface ExportTriSiteProofOptions {
  outputPath?: string;
  historyLimit?: number;
}

function defaultOutputPath(now: Date): string {
  const dateKey = now.toISOString().slice(0, 10);
  return path.resolve(process.cwd(), 'out', 'proof', `tri-site-${dateKey}.json`);
}

function summarizeRun(run: Awaited<ReturnType<typeof getRunHistory>>[number]): TriSiteProofRunSummary {
  return {
    id: run.id,
    idempotency_key: run.idempotency_key,
    started_at: run.started_at,
    finished_at: run.finished_at,
    status: run.status,
    failure_class: run.failure_class,
    partial_reason: run.partial_reason,
    records_scraped: run.records_scraped,
    rows_uploaded: run.rows_uploaded,
    sla_score_pct: run.sla_score_pct,
    sla_pass: run.sla_pass === 1,
  };
}

function resolveOverallStatus(sites: Record<SupportedSite, TriSiteProofSiteSummary>): TriSiteProofSummary['overall_status'] {
  const siteSummaries = Object.values(sites);
  if (siteSummaries.every((site) => site.site_compliant)) {
    return 'compliant';
  }
  if (siteSummaries.some((site) => site.rolling_sla_status === 'insufficient_data')) {
    return 'insufficient_data';
  }
  return 'breached';
}

export async function exportTriSiteProof(
  options: ExportTriSiteProofOptions = {},
): Promise<TriSiteProofSummary> {
  const now = new Date();
  const outputPath = options.outputPath ? path.resolve(options.outputPath) : defaultOutputPath(now);
  const historyLimit = Math.max(50, options.historyLimit ?? Number(process.env.TRI_SITE_PROOF_HISTORY_LIMIT ?? '200'));
  const [nextRuns, state, history] = await Promise.all([
    Promise.resolve(getNextRuns()),
    getScheduleState(),
    getRunHistory(historyLimit),
  ]);

  const sites = Object.fromEntries(
    supportedSites.map((site) => {
      const siteRuns = history.filter((run) => run.site === site);
      const latestRun = siteRuns[0];
      const latestSuccessfulRun = siteRuns.find((run) => run.status === 'success');
      const latestErrorRun = siteRuns.find((run) => run.status === 'error');
      const latestDeferredRun = siteRuns.find((run) => run.status === 'deferred');
      const latestSlaBreachRun = siteRuns.find((run) => run.sla_pass === 0);
      const siteState = state[site];

      return [site, {
        site_compliant: siteState.site_compliant,
        rolling_sla_pass_rate_20: siteState.rolling_sla_pass_rate_20,
        rolling_sla_status: siteState.rolling_sla_status,
        rolling_sla_pass_count: siteState.rolling_sla_pass_count,
        rolling_sla_window_successful_runs: siteState.rolling_sla_window_successful_runs,
        previous_business_day: siteState.previous_business_day,
        previous_business_day_slot_success_count: siteState.previous_business_day_slot_success_count,
        previous_business_day_slots_ok: siteState.previous_business_day_slots_ok,
        effective_max_records: siteState.effective_max_records,
        connectivity_status: siteState.connectivity.status,
        next_allowed_run_at: siteState.connectivity.next_allowed_run_at,
        latest_failure_reason: siteState.connectivity.last_failure_reason ?? latestErrorRun?.failure_class,
        latest_run_ids: {
          latest_run_id: latestRun?.id,
          latest_successful_run_id: latestSuccessfulRun?.id,
          latest_error_run_id: latestErrorRun?.id,
          latest_deferred_run_id: latestDeferredRun?.id,
          latest_sla_breach_run_id: latestSlaBreachRun?.id,
        },
        recent_runs: siteRuns.slice(0, 5).map(summarizeRun),
      } satisfies TriSiteProofSiteSummary] as const;
    }),
  ) as Record<SupportedSite, TriSiteProofSiteSummary>;

  const summary: TriSiteProofSummary = {
    generated_at: now.toISOString(),
    proof_date: now.toISOString().slice(0, 10),
    output_path: outputPath,
    overall_status: resolveOverallStatus(sites),
    next_runs: nextRuns,
    sites,
  };

  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(outputPath, JSON.stringify(summary, null, 2), 'utf8');
  return summary;
}
