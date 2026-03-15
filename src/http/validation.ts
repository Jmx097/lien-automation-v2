import { supportedSites, type SupportedSite } from '../sites';
import type { RetryableScheduledFailureClass, Slot } from '../scheduler';

export interface ValidationIssue {
  field: string;
  message: string;
}

export type ValidationResult<T> = {
  ok: true;
  value: T;
} | {
  ok: false;
  issues: ValidationIssue[];
};

export interface ScrapeRequest {
  site: SupportedSite;
  date_start: string;
  date_end: string;
  max_records?: number;
}

export interface ScheduleRunRequest {
  site?: SupportedSite;
  slot?: Slot;
  idempotency_key?: string;
  test_retry_failure_class?: RetryableScheduledFailureClass;
}

const DATE_RE = /^\d{2}\/\d{2}\/\d{4}$/;
const allowedRetryFailureClasses = new Set<RetryableScheduledFailureClass>([
  'timeout_or_navigation',
  'viewer_roundtrip',
  'token_or_session_state',
  'sheet_export',
]);

function getMaxRecordsBySite(): Record<SupportedSite, number> {
  const ceiling = Number(process.env.SCHEDULE_MAX_RECORDS_CEILING ?? '1000');
  return {
    ca_sos: ceiling,
    maricopa_recorder: ceiling,
    nyc_acris: ceiling,
  };
}

function asObject(input: unknown): Record<string, unknown> {
  return typeof input === 'object' && input !== null ? input as Record<string, unknown> : {};
}

function asTrimmedString(input: unknown): string | undefined {
  return typeof input === 'string' && input.trim() ? input.trim() : undefined;
}

function parseDate(value: string): Date | undefined {
  if (!DATE_RE.test(value)) return undefined;
  const [month, day, year] = value.split('/').map(Number);
  const parsed = new Date(Date.UTC(year, month - 1, day));
  if (
    Number.isNaN(parsed.getTime()) ||
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month - 1 ||
    parsed.getUTCDate() !== day
  ) {
    return undefined;
  }
  return parsed;
}

function parseOptionalMaxRecords(raw: unknown, site: SupportedSite, issues: ValidationIssue[]): number | undefined {
  const maxRecordsBySite = getMaxRecordsBySite();
  if (raw == null || raw === '') return undefined;
  const parsed = typeof raw === 'number' ? raw : Number(String(raw));
  if (!Number.isInteger(parsed) || parsed < 1) {
    issues.push({ field: 'max_records', message: 'max_records must be a positive integer' });
    return undefined;
  }
  if (parsed > maxRecordsBySite[site]) {
    issues.push({ field: 'max_records', message: `max_records must be <= ${maxRecordsBySite[site]} for ${site}` });
    return undefined;
  }
  return parsed;
}

export function validateScrapeRequest(input: unknown): ValidationResult<ScrapeRequest> {
  const body = asObject(input);
  const issues: ValidationIssue[] = [];
  const site = asTrimmedString(body.site);

  if (!site || !supportedSites.includes(site as SupportedSite)) {
    issues.push({ field: 'site', message: `site must be one of: ${supportedSites.join(', ')}` });
  }

  const dateStart = asTrimmedString(body.date_start);
  const dateEnd = asTrimmedString(body.date_end);
  const parsedStart = dateStart ? parseDate(dateStart) : undefined;
  const parsedEnd = dateEnd ? parseDate(dateEnd) : undefined;

  if (!dateStart) issues.push({ field: 'date_start', message: 'date_start is required' });
  else if (!parsedStart) issues.push({ field: 'date_start', message: 'date_start must use MM/DD/YYYY' });

  if (!dateEnd) issues.push({ field: 'date_end', message: 'date_end is required' });
  else if (!parsedEnd) issues.push({ field: 'date_end', message: 'date_end must use MM/DD/YYYY' });

  if (parsedStart && parsedEnd && parsedStart.getTime() > parsedEnd.getTime()) {
    issues.push({ field: 'date_range', message: 'date_start must be on or before date_end' });
  }

  const resolvedSite = (site as SupportedSite | undefined) ?? 'ca_sos';
  const maxRecords = parseOptionalMaxRecords(body.max_records, resolvedSite, issues);

  if (issues.length > 0) return { ok: false, issues };

  return {
    ok: true,
    value: {
      site: resolvedSite,
      date_start: dateStart!,
      date_end: dateEnd!,
      max_records: maxRecords,
    },
  };
}

export function validateScheduleRunRequest(input: unknown): ValidationResult<ScheduleRunRequest> {
  const body = asObject(input);
  const issues: ValidationIssue[] = [];
  const site = asTrimmedString(body.site);
  const slot = asTrimmedString(body.slot);
  const idempotencyKey = asTrimmedString(body.idempotency_key);
  const failureClass = asTrimmedString(body.test_retry_failure_class);

  if (site && !supportedSites.includes(site as SupportedSite)) {
    issues.push({ field: 'site', message: `site must be one of: ${supportedSites.join(', ')}` });
  }

  if (slot && slot !== 'morning' && slot !== 'afternoon' && slot !== 'evening') {
    issues.push({ field: 'slot', message: 'slot must be morning, afternoon, or evening' });
  }

  if (typeof body.idempotency_key === 'string' && !idempotencyKey) {
    issues.push({ field: 'idempotency_key', message: 'idempotency_key must not be blank' });
  }

  if (failureClass && !allowedRetryFailureClasses.has(failureClass as RetryableScheduledFailureClass)) {
    issues.push({
      field: 'test_retry_failure_class',
      message: `test_retry_failure_class must be one of: ${Array.from(allowedRetryFailureClasses).join(', ')}`,
    });
  }

  if (issues.length > 0) return { ok: false, issues };

  return {
    ok: true,
    value: {
      site: site as SupportedSite | undefined,
      slot: slot as Slot | undefined,
      idempotency_key: idempotencyKey,
      test_retry_failure_class: failureClass as RetryableScheduledFailureClass | undefined,
    },
  };
}
