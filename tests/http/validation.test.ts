import { describe, expect, it } from 'vitest';
import { validateScheduleRunRequest, validateScrapeRequest } from '../../src/http/validation';

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
        expect.arrayContaining(['slot', 'idempotency_key', 'test_retry_failure_class'])
      );
    }
  });
});
