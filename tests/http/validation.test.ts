import { describe, expect, it } from 'vitest';
import { validateNYCAcrisDebugRequest, validateScheduleRunRequest, validateScrapeRequest } from '../../src/http/validation';

describe('HTTP validation', () => {
  it('accepts a well-formed scrape request', () => {
    const result = validateScrapeRequest({
      site: 'maricopa_recorder',
      date_start: '03/01/2026',
      date_end: '03/07/2026',
      max_records: 25,
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.site).toBe('maricopa_recorder');
      expect(result.value.max_records).toBe(25);
    }
  });

  it('rejects invalid date ranges and blank identifiers', () => {
    const scrape = validateScrapeRequest({
      site: 'nyc_acris',
      date_start: '03/08/2026',
      date_end: '03/01/2026',
      max_records: 0,
    });
    expect(scrape.ok).toBe(false);
    if (!scrape.ok) {
      expect(scrape.issues.map((issue) => issue.field)).toEqual(
        expect.arrayContaining(['date_range', 'max_records'])
      );
    }

    const schedule = validateScheduleRunRequest({
      site: 'nyc_acris',
      slot: 'evening',
      idempotency_key: '   ',
      test_retry_failure_class: 'not-real',
    });
    expect(schedule.ok).toBe(false);
    if (!schedule.ok) {
      expect(schedule.issues.map((issue) => issue.field)).toEqual(
        expect.arrayContaining(['idempotency_key', 'test_retry_failure_class'])
      );
    }
  });

  it('accepts the evening slot for scheduled runs', () => {
    const schedule = validateScheduleRunRequest({
      site: 'nyc_acris',
      slot: 'evening',
      idempotency_key: 'nyc_acris:2026-03-10:evening',
    });

    expect(schedule.ok).toBe(true);
    if (schedule.ok) {
      expect(schedule.value.slot).toBe('evening');
    }
  });

  it('accepts NYC bootstrap debug flags and transport override', () => {
    const schedule = validateScheduleRunRequest({
      site: 'nyc_acris',
      slot: 'afternoon',
      idempotency_key: 'nyc_acris:2026-03-10:afternoon:debug',
      debug_bootstrap_only: true,
      transport_mode_override: 'legacy-sbr-cdp',
    });

    expect(schedule.ok).toBe(true);
    if (schedule.ok) {
      expect(schedule.value.debug_bootstrap_only).toBe(true);
      expect(schedule.value.transport_mode_override).toBe('legacy-sbr-cdp');
    }

    const debug = validateNYCAcrisDebugRequest({
      transport_mode_override: 'brightdata-browser-api',
    });
    expect(debug.ok).toBe(true);
  });

  it('rejects invalid transport overrides', () => {
    const schedule = validateScheduleRunRequest({
      site: 'nyc_acris',
      debug_bootstrap_only: 'yes',
      transport_mode_override: 'bad-mode',
    });
    expect(schedule.ok).toBe(false);
    if (!schedule.ok) {
      expect(schedule.issues.map((issue) => issue.field)).toEqual(
        expect.arrayContaining(['debug_bootstrap_only', 'transport_mode_override'])
      );
    }

    const debug = validateNYCAcrisDebugRequest({
      transport_mode_override: 'bad-mode',
    });
    expect(debug.ok).toBe(false);
  });
});
