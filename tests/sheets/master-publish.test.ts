import { beforeEach, describe, expect, it, vi } from 'vitest';

type SheetTab = {
  sheetId: number;
  rows: any[][];
  header?: any[][];
};

const workbookState = new Map<string, Map<string, SheetTab>>();
const workbookAccess = new Map<string, { read: boolean; write: boolean }>();
const requestCounts = {
  spreadsheetsGet: 0,
  valuesGet: 0,
  valuesBatchGet: 0,
};

function ensureWorkbook(spreadsheetId: string): Map<string, SheetTab> {
  let workbook = workbookState.get(spreadsheetId);
  if (!workbook) {
    workbook = new Map<string, SheetTab>();
    workbookState.set(spreadsheetId, workbook);
  }
  return workbook;
}

function seedWorkbook(spreadsheetId: string, tabs: Record<string, any[][]>) {
  const workbook = new Map<string, SheetTab>();
  let nextSheetId = 1;
  for (const [title, rows] of Object.entries(tabs)) {
    workbook.set(title, { sheetId: nextSheetId++, rows: rows.map((row) => [...row]) });
  }
  workbookState.set(spreadsheetId, workbook);
}

function parseRange(range: string): { title: string; row: number } {
  const match = range.match(/^'(.+)'!([A-Z]+)(\d+)/);
  if (!match) throw new Error(`Unsupported range: ${range}`);
  return { title: match[1], row: Number(match[3]) };
}

function getWorkbookAccess(spreadsheetId: string) {
  return workbookAccess.get(spreadsheetId) ?? { read: true, write: true };
}

vi.mock('googleapis', () => {
  class GoogleAuth {}

  return {
    google: {
      auth: { GoogleAuth },
      sheets: () => ({
        spreadsheets: {
          get: async ({ spreadsheetId }: { spreadsheetId: string }) => {
            requestCounts.spreadsheetsGet += 1;
            const access = getWorkbookAccess(spreadsheetId);
            if (!access.read) throw new Error(`Access denied to ${spreadsheetId}`);
            const workbook = ensureWorkbook(spreadsheetId);
            return {
              data: {
                sheets: Array.from(workbook.entries()).map(([title, tab]) => ({
                  properties: { title, sheetId: tab.sheetId },
                })),
              },
            };
          },
          batchUpdate: async ({ spreadsheetId, requestBody }: { spreadsheetId: string; requestBody: { requests: any[] } }) => {
            const access = getWorkbookAccess(spreadsheetId);
            if (!access.write) throw new Error(`Access denied to ${spreadsheetId}`);
            const workbook = ensureWorkbook(spreadsheetId);
            const replies: any[] = [];
            for (const request of requestBody.requests) {
              if (request.addSheet) {
                const title = request.addSheet.properties.title;
                if (!workbook.has(title)) {
                  const sheetId = workbook.size + 1;
                  workbook.set(title, { sheetId, rows: [] });
                  replies.push({ addSheet: { properties: { title, sheetId } } });
                }
              }
            }
            return { data: { replies } };
          },
          values: {
            get: async ({ spreadsheetId, range }: { spreadsheetId: string; range: string }) => {
              requestCounts.valuesGet += 1;
              const access = getWorkbookAccess(spreadsheetId);
              if (!access.read) throw new Error(`Access denied to ${spreadsheetId}`);
              const workbook = ensureWorkbook(spreadsheetId);
              const { title, row } = parseRange(range);
              const tab = workbook.get(title);
              const values = row === 1 ? (tab?.header ?? []) : (tab?.rows ?? []);
              return { data: { values: values.map((valueRow) => [...valueRow]) } };
            },
            batchGet: async ({ spreadsheetId, ranges }: { spreadsheetId: string; ranges: string[] }) => {
              requestCounts.valuesBatchGet += 1;
              const access = getWorkbookAccess(spreadsheetId);
              if (!access.read) throw new Error(`Access denied to ${spreadsheetId}`);
              const workbook = ensureWorkbook(spreadsheetId);
              const valueRanges = ranges.map((range) => {
                const { title, row } = parseRange(range);
                const tab = workbook.get(title);
                const values = row === 1 ? (tab?.header ?? []) : (tab?.rows ?? []);
                return {
                  range,
                  values: values.map((valueRow) => [...valueRow]),
                };
              });
              return { data: { valueRanges } };
            },
            update: async ({ spreadsheetId, range, requestBody }: { spreadsheetId: string; range: string; requestBody: { values: any[][] } }) => {
              const access = getWorkbookAccess(spreadsheetId);
              if (!access.write) throw new Error(`Access denied to ${spreadsheetId}`);
              const workbook = ensureWorkbook(spreadsheetId);
              const { title, row } = parseRange(range);
              const tab = workbook.get(title) ?? { sheetId: workbook.size + 1, rows: [] };
              if (row === 1) {
                tab.header = requestBody.values.map((valueRow) => [...valueRow]);
              } else if (row === 2) {
                tab.rows = requestBody.values.map((valueRow) => [...valueRow]);
              }
              workbook.set(title, tab);
              return {};
            },
            clear: async ({ spreadsheetId, range }: { spreadsheetId: string; range: string }) => {
              const access = getWorkbookAccess(spreadsheetId);
              if (!access.write) throw new Error(`Access denied to ${spreadsheetId}`);
              const workbook = ensureWorkbook(spreadsheetId);
              const { title } = parseRange(range);
              const tab = workbook.get(title);
              if (tab) tab.rows = [];
              return {};
            },
            append: async () => {
              throw new Error('append should not be used in this test');
            },
          },
        },
      }),
    },
  };
});

describe('syncMasterSheetTab', () => {
  beforeEach(() => {
    vi.resetModules();
    workbookState.clear();
    workbookAccess.clear();
    requestCounts.spreadsheetsGet = 0;
    requestCounts.valuesGet = 0;
    requestCounts.valuesBatchGet = 0;
    process.env.SHEETS_KEY = JSON.stringify({
      client_email: 'svc@example.com',
      private_key: '-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n',
    });
    process.env.SHEET_ID = 'source-sheet';
    process.env.MERGED_SHEET_ID = 'target-sheet';
    process.env.REVIEW_QUEUE_RETENTION_DAYS = '7';
    delete process.env.DIRECTOR_MIN_CONFIDENCE_ACCEPT;
    delete process.env.DIRECTOR_MIN_CONFIDENCE_REVIEW;
    workbookAccess.set('source-sheet', { read: true, write: true });
    workbookAccess.set('target-sheet', { read: true, write: true });
  });

  function sourceRow(overrides: Partial<Record<number, any>> = {}): any[] {
    const row = [
      12,
      '03/12/2026',
      '100',
      'Lien',
      '777',
      'IRS',
      'Business',
      'ACME LLC',
      '',
      '',
      '123 Main St',
      'New York',
      'NY',
      '10001',
      0.95,
      'nyc_acris',
      'file-1',
      '0',
    ];
    for (const [index, value] of Object.entries(overrides)) {
      row[Number(index)] = value;
    }
    return row;
  }

  function mergedMasterRow(
    overrides: Partial<Record<number, any>> = {},
    meta: { sourceTab?: string; scheduledRunId?: string } = {}
  ): any[] {
    const row = sourceRow(overrides);
    return [
      ...row.slice(0, 18),
      meta.sourceTab ?? '',
      meta.scheduledRunId ?? '',
      '',
      '',
    ];
  }

  function reviewQueueRow(
    overrides: Partial<Record<number, any>> = {},
    meta: { sourceTab?: string; scheduledRunId?: string; reviewReason: string; conflictType?: string }
  ): any[] {
    const row = sourceRow(overrides);
    return [
      ...row.slice(0, 18),
      meta.sourceTab ?? '',
      meta.scheduledRunId ?? '',
      meta.reviewReason,
      meta.conflictType ?? '',
    ];
  }

  it('publishes merged scheduled rows into the destination workbook', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_CA: [
        sourceRow({ 0: 20, 15: 'ca_sos', 16: 'ca-file-1' }),
        sourceRow({ 0: 20, 2: '250', 14: 0.91, 15: 'ca_sos', 16: 'ca-file-2' }),
      ],
      Scheduled_NYC: [
        sourceRow({ 16: 'ny-file-1', 2: '300', 14: 0.96 }),
      ],
      Master: [Array(15).fill('old-master')],
    });
    seedWorkbook('target-sheet', {
      Master: [Array(15).fill('stale-target')],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 3,
      source_tabs: 2,
      target_spreadsheet_id: 'target-sheet',
      fallback_used: false,
      quarantined_row_count: 0,
      review_tab_title: 'Review_Queue',
      review_summary: expect.objectContaining({
        accepted_row_count: 3,
        quarantined_row_count: 0,
        review_reason_counts: {},
      }),
    }));
    expect(ensureWorkbook('target-sheet').get('Master')?.rows).toEqual([
      mergedMasterRow({ 0: 20, 15: 'ca_sos', 16: 'ca-file-1' }, { sourceTab: 'Scheduled_CA' }),
      mergedMasterRow({ 0: 20, 2: '250', 14: 0.91, 15: 'ca_sos', 16: 'ca-file-2' }, { sourceTab: 'Scheduled_CA' }),
      mergedMasterRow({ 16: 'ny-file-1', 2: '300', 14: 0.96 }, { sourceTab: 'Scheduled_NYC' }),
    ]);
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows).toEqual([]);
    expect(ensureWorkbook('target-sheet').get('Master')?.header?.[0]).toEqual([
      'Site Id',
      'LienOrReceiveDate',
      'Amount',
      'LeadType',
      'LeadSource',
      'LiabilityType',
      'BusinessPersonal',
      'Company',
      'FirstName',
      'LastName',
      'Street',
      'City',
      'State',
      'Zip',
      'ConfidenceScore',
      'RecordSource',
      'FileNumber',
      'RunPartial',
      'SourceTab',
      'ScheduledRunId',
      'ReviewReason',
      'ConflictType',
    ]);
    expect(ensureWorkbook('source-sheet').get('Master')?.rows).toEqual([Array(15).fill('old-master')]);
  });

  it('uses batched scheduled-tab reads and workbook metadata caching during merge sync', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_CA: [
        sourceRow({ 0: 20, 15: 'ca_sos', 16: 'ca-file-1' }),
      ],
      Scheduled_NYC: [
        sourceRow({ 16: 'ny-file-1', 2: '300', 14: 0.96 }),
      ],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    await syncMasterSheetTab();

    expect(requestCounts.valuesBatchGet).toBe(2);
    expect(requestCounts.spreadsheetsGet).toBe(2);
    expect(requestCounts.valuesGet).toBe(1);
  });

  it('falls back to the source workbook when the destination sheet is not writable', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_CA: [sourceRow({ 15: 'ca_sos', 16: 'ca-file-1' })],
      Scheduled_NYC: [sourceRow({ 16: 'ny-file-1', 17: '1' })],
      Master: [Array(15).fill('old-master')],
    });
    seedWorkbook('target-sheet', {
      Master: [Array(15).fill('stale-target')],
    });
    workbookAccess.set('target-sheet', { read: true, write: false });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 2,
      source_tabs: 2,
      target_spreadsheet_id: 'source-sheet',
      fallback_used: true,
      quarantined_row_count: 0,
    }));
    expect(ensureWorkbook('source-sheet').get('Master')?.rows).toEqual([
      mergedMasterRow({ 15: 'ca_sos', 16: 'ca-file-1' }, { sourceTab: 'Scheduled_CA' }),
      mergedMasterRow({ 16: 'ny-file-1', 17: '1' }, { sourceTab: 'Scheduled_NYC' }),
    ]);
    expect(ensureWorkbook('source-sheet').get('Review_Queue')?.rows).toEqual([]);
    expect(ensureWorkbook('target-sheet').get('Master')?.rows).toEqual([Array(15).fill('stale-target')]);
  });

  it('quarantines low-confidence rows instead of publishing them to Master', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_NYC: [sourceRow({ 14: 0.72, 16: 'ny-low-confidence' })],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 0,
      quarantined_row_count: 1,
      review_summary: expect.objectContaining({
        review_reason_counts: expect.objectContaining({
          low_confidence: 1,
        }),
      }),
    }));
    expect(ensureWorkbook('target-sheet').get('Master')?.rows).toEqual([]);
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows?.[0]?.[20]).toContain('low_confidence');
  });

  it('accepts otherwise-clean mid-confidence rows when they are above the review threshold', async () => {
    process.env.DIRECTOR_MIN_CONFIDENCE_ACCEPT = '0.85';
    process.env.DIRECTOR_MIN_CONFIDENCE_REVIEW = '0.75';
    seedWorkbook('source-sheet', {
      Scheduled_NYC: [sourceRow({ 14: 0.8, 16: 'ny-mid-confidence' })],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 1,
      quarantined_row_count: 0,
      review_summary: expect.objectContaining({
        accepted_row_count: 1,
      }),
    }));
    expect(ensureWorkbook('target-sheet').get('Master')?.rows).toEqual([
      mergedMasterRow({ 14: 0.8, 16: 'ny-mid-confidence' }, { sourceTab: 'Scheduled_NYC' }),
    ]);
  });

  it('still quarantines mid-confidence rows when another soft flag is present', async () => {
    process.env.DIRECTOR_MIN_CONFIDENCE_ACCEPT = '0.85';
    process.env.DIRECTOR_MIN_CONFIDENCE_REVIEW = '0.75';
    seedWorkbook('source-sheet', {
      Scheduled_NYC: [sourceRow({ 14: 0.8, 16: 'ny-mid-confidence-partial', 17: '1' })],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 0,
      quarantined_row_count: 1,
      review_summary: expect.objectContaining({
        review_reason_counts: expect.objectContaining({
          low_confidence: 1,
          partial_run: 1,
        }),
      }),
    }));
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows).toEqual([
      reviewQueueRow(
        { 14: 0.8, 16: 'ny-mid-confidence-partial', 17: '1' },
        { sourceTab: 'Scheduled_NYC', reviewReason: 'partial_run|low_confidence' }
      ),
    ]);
  });

  it('still quarantines partial-run rows when they also have a hard validation issue', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_NYC: [
        sourceRow({ 10: '', 11: '', 13: '', 16: 'ny-partial-hard-fail', 17: '1' }),
      ],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 0,
      quarantined_row_count: 1,
      review_summary: expect.objectContaining({
        review_reason_counts: expect.objectContaining({
          missing_required_fields: 1,
          address_incomplete: 1,
          partial_run: 1,
        }),
      }),
    }));
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows?.[0]?.[20]).toContain('missing_required_fields');
  });

  it('keeps only the highest-confidence duplicate row in Master and quarantines the other copy', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_NYC: [
        sourceRow({ 2: '100', 14: 0.91, 16: 'duplicate-file' }),
        sourceRow({ 2: '125', 14: 0.98, 16: 'duplicate-file' }),
      ],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 1,
      quarantined_row_count: 1,
    }));
    expect(ensureWorkbook('target-sheet').get('Master')?.rows).toEqual([
      mergedMasterRow({ 2: '125', 14: 0.98, 16: 'duplicate-file' }, { sourceTab: 'Scheduled_NYC' }),
    ]);
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows?.[0]?.[20]).toBe('conflict_lower_confidence');
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows?.[0]?.[21]).toBe('lower_ranked_loser_against_accepted_candidate');
  });

  it('prefers a non-partial duplicate over an equally confident partial duplicate', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_NYC: [
        sourceRow({ 2: '100', 14: 0.9, 16: 'duplicate-partial-file', 17: '1' }),
        sourceRow({ 2: '100', 14: 0.9, 16: 'duplicate-partial-file', 17: '0' }),
      ],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 1,
      quarantined_row_count: 1,
    }));
    expect(ensureWorkbook('target-sheet').get('Master')?.rows).toEqual([
      mergedMasterRow({ 2: '100', 14: 0.9, 16: 'duplicate-partial-file', 17: '0' }, { sourceTab: 'Scheduled_NYC' }),
    ]);
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows).toEqual([
      reviewQueueRow(
        { 2: '100', 14: 0.9, 16: 'duplicate-partial-file', 17: '1' },
        { sourceTab: 'Scheduled_NYC', reviewReason: 'partial_run', conflictType: 'lower_ranked_loser_against_accepted_candidate' }
      ),
    ]);
  });

  it('prefers the newer duplicate when confidence and review flags are otherwise tied', async () => {
    seedWorkbook('source-sheet', {
      'Scheduled_NYC_03-12-2026_to_03-12-2026_20260312T010101_Pacific': [
        sourceRow({ 2: '100', 14: 0.9, 16: 'duplicate-newer-file' }),
      ],
      'Scheduled_NYC_03-13-2026_to_03-13-2026_20260313T010101_Pacific': [
        sourceRow({ 2: '125', 14: 0.9, 16: 'duplicate-newer-file' }),
      ],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 1,
      quarantined_row_count: 1,
      review_summary: expect.objectContaining({
        review_reason_counts: expect.objectContaining({
          conflict_lower_confidence: 1,
        }),
      }),
    }));
    expect(ensureWorkbook('target-sheet').get('Master')?.rows).toEqual([
      mergedMasterRow(
        { 2: '125', 14: 0.9, 16: 'duplicate-newer-file' },
        { sourceTab: 'Scheduled_NYC_03-13-2026_to_03-13-2026_20260313T010101_Pacific' }
      ),
    ]);
  });

  it('quarantines truly ambiguous duplicates when ranking still ties', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_NYC_A: [
        sourceRow({ 2: '100', 14: 0.9, 16: 'duplicate-ambiguous-file' }),
      ],
      Scheduled_NYC_B: [
        sourceRow({ 2: '100', 14: 0.9, 16: 'duplicate-ambiguous-file' }),
      ],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 0,
      quarantined_row_count: 2,
      review_summary: expect.objectContaining({
        review_reason_counts: expect.objectContaining({
          conflict_ambiguous: 2,
        }),
      }),
    }));
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows?.map((row) => row[20])).toEqual([
      'conflict_ambiguous',
      'conflict_ambiguous',
    ]);
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows?.map((row) => row[21])).toEqual([
      'ambiguous_tie_against_retained_review',
      'ambiguous_tie_against_retained_review',
    ]);
  });

  it('prefers the current run when an otherwise-identical accepted row already exists in retained history', async () => {
    seedWorkbook('source-sheet', {
      'Scheduled_old_sched_ca_sos_1773620000000_deadbe_03-09-2026_to_03-16-2026': [
        sourceRow({ 0: 20, 2: '100', 14: 0.98, 15: 'ca_sos', 16: 'duplicate-current-preferred' }),
      ],
      'Scheduled_current_sched_ca_sos_1773625523337_53ff21_03-09-2026_to_03-16-2026': [
        sourceRow({ 0: 20, 2: '100', 14: 0.98, 15: 'ca_sos', 16: 'duplicate-current-preferred' }),
      ],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab({
      currentSourceTab: 'Scheduled_current_sched_ca_sos_1773625523337_53ff21_03-09-2026_to_03-16-2026',
    });

    expect(result).toEqual(expect.objectContaining({
      row_count: 1,
      quarantined_row_count: 1,
      current_run_conflict_row_count: 0,
      review_summary: expect.objectContaining({
        accepted_row_count: 1,
        review_reason_counts: expect.objectContaining({
          conflict_lower_confidence: 1,
          lower_ranked_loser_against_accepted_candidate: 1,
        }),
      }),
    }));
    expect(ensureWorkbook('target-sheet').get('Master')?.rows).toEqual([
      mergedMasterRow(
        { 0: 20, 2: '100', 14: 0.98, 15: 'ca_sos', 16: 'duplicate-current-preferred' },
        {
          sourceTab: 'Scheduled_current_sched_ca_sos_1773625523337_53ff21_03-09-2026_to_03-16-2026',
          scheduledRunId: 'sched_ca_sos_1773625523337_53ff21',
        }
      ),
    ]);
  });

  it('reports current-run quarantine, conflict, and retained prior review counts separately', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_old: [
        sourceRow({ 14: 0.72, 16: 'old-review-file' }),
        sourceRow({ 2: '200', 14: 0.98, 16: 'shared-duplicate-file' }),
      ],
      Scheduled_current: [
        sourceRow({ 14: 0.72, 16: 'current-review-file' }),
        sourceRow({ 2: '100', 14: 0.91, 16: 'shared-duplicate-file' }),
      ],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab({ currentSourceTab: 'Scheduled_current' });

    expect(result).toEqual(expect.objectContaining({
      quarantined_row_count: 3,
      current_run_quarantined_row_count: 1,
      current_run_conflict_row_count: 1,
      retained_prior_review_row_count: 1,
      review_summary: expect.objectContaining({
        current_run_quarantined_row_count: 1,
        current_run_conflict_row_count: 1,
        retained_prior_review_row_count: 1,
      }),
    }));
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows?.map((row) => row[20]).sort()).toEqual([
      'conflict_lower_confidence',
      'low_confidence',
      'low_confidence',
    ]);
  });

  it('keeps the timestamp suffix when long run labels are truncated for sheet titles', async () => {
    const { formatRunTabName } = await import('../../src/sheets/push');
    const tabName = formatRunTabName(
      'Scheduled_maricopa_recorder_evening_sched_maricopa_recorder_1773625728891_03a3db',
      '03/09/2026',
      '03/16/2026',
      new Date('2026-03-15T22:15:30.000Z')
    );

    expect(tabName.length).toBeLessThanOrEqual(100);
    expect(tabName).toContain('_03-09-2026_to_03-16-2026_');
    expect(tabName.endsWith('_Pacific')).toBe(true);
  });

  it('tags current-run duplicates against retained review rows with explicit conflict provenance', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_old: [
        sourceRow({ 14: 0.72, 16: 'retained-review-duplicate' }),
      ],
      Scheduled_current: [
        sourceRow({ 14: 0.72, 16: 'retained-review-duplicate' }),
      ],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab({ currentSourceTab: 'Scheduled_current' });

    expect(result).toEqual(expect.objectContaining({
      row_count: 0,
      quarantined_row_count: 2,
      current_run_conflict_row_count: 1,
      review_summary: expect.objectContaining({
        review_reason_counts: expect.objectContaining({
          duplicate_against_retained_review: 2,
        }),
      }),
    }));
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows?.map((row) => row[21])).toEqual([
      'duplicate_against_retained_review',
      'duplicate_against_retained_review',
    ]);
  });

  it('tags current-run losers against accepted retained candidates distinctly from current-run clashes', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_old: [
        sourceRow({ 2: '200', 14: 0.98, 16: 'accepted-retained-winner' }),
      ],
      Scheduled_current: [
        sourceRow({ 2: '100', 14: 0.91, 16: 'accepted-retained-winner' }),
      ],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab({ currentSourceTab: 'Scheduled_current' });

    expect(result).toEqual(expect.objectContaining({
      row_count: 1,
      quarantined_row_count: 1,
      current_run_conflict_row_count: 1,
      review_summary: expect.objectContaining({
        review_reason_counts: expect.objectContaining({
          lower_ranked_loser_against_accepted_candidate: 1,
        }),
      }),
    }));
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows?.[0]?.[21]).toBe('lower_ranked_loser_against_accepted_candidate');
  });

  it('normalizes legacy scheduled tabs that already include merged provenance columns', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_legacy: [],
    });
    const legacyTab = ensureWorkbook('source-sheet').get('Scheduled_legacy');
    if (!legacyTab) throw new Error('legacy tab missing');
    legacyTab.header = [[
      'Site Id',
      'LienOrReceiveDate',
      'Amount',
      'LeadType',
      'LeadSource',
      'LiabilityType',
      'BusinessPersonal',
      'Company',
      'FirstName',
      'LastName',
      'Street',
      'City',
      'State',
      'Zip',
      'ConfidenceScore',
      'RecordSource',
      'FileNumber',
      'RunPartial',
      'SourceTab',
      'ScheduledRunId',
      'ReviewReason',
      'ConflictType',
    ]];
    legacyTab.rows = [
      reviewQueueRow(
        { 14: 0.72, 15: 'ca_sos', 16: 'legacy-file-1' },
        {
          sourceTab: 'Scheduled_ca_sos_evening_sched_ca_sos_legacy_03-01-2026_to_03-08-2026',
          scheduledRunId: 'sched_ca_sos_legacy',
          reviewReason: 'low_confidence',
          conflictType: 'duplicate_against_retained_review',
        }
      ),
    ];

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    await syncMasterSheetTab();

    const row = ensureWorkbook('target-sheet').get('Review_Queue')?.rows?.[0];
    expect(row?.[16]).toBe('legacy-file-1');
    expect(row?.[17]).toBe('0');
    expect(row?.[18]).toBe('Scheduled_legacy');
    expect(row?.[20]).toBe('low_confidence');
    expect(row?.[21]).toBe('');
  });

  it('purges quarantined rows older than the review retention window while keeping recent ones', async () => {
    seedWorkbook('source-sheet', {
      'Scheduled_old_01-01-2000_to_01-01-2000_20000101': [
        sourceRow({ 14: 0.72, 16: 'old-review-file' }),
      ],
      'Scheduled_new_01-01-2099_to_01-01-2099_20990101': [
        sourceRow({ 14: 0.72, 16: 'new-review-file' }),
      ],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 0,
      quarantined_row_count: 1,
      purged_review_row_count: 1,
    }));
    expect(ensureWorkbook('target-sheet').get('Review_Queue')?.rows).toEqual([
      reviewQueueRow(
        { 14: 0.72, 16: 'new-review-file' },
        { sourceTab: 'Scheduled_new_01-01-2099_to_01-01-2099_20990101', reviewReason: 'low_confidence' }
      ),
    ]);
  });
});
