import { beforeEach, describe, expect, it, vi } from 'vitest';

type SheetTab = {
  sheetId: number;
  rows: any[][];
  header?: any[][];
};

const workbookState = new Map<string, Map<string, SheetTab>>();
const workbookAccess = new Map<string, { read: boolean; write: boolean }>();

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
            for (const request of requestBody.requests) {
              if (request.addSheet) {
                const title = request.addSheet.properties.title;
                if (!workbook.has(title)) {
                  workbook.set(title, { sheetId: workbook.size + 1, rows: [] });
                }
              }
            }
            return {};
          },
          values: {
            get: async ({ spreadsheetId, range }: { spreadsheetId: string; range: string }) => {
              const access = getWorkbookAccess(spreadsheetId);
              if (!access.read) throw new Error(`Access denied to ${spreadsheetId}`);
              const workbook = ensureWorkbook(spreadsheetId);
              const { title, row } = parseRange(range);
              const tab = workbook.get(title);
              const values = row === 1 ? (tab?.header ?? []) : (tab?.rows ?? []);
              return { data: { values: values.map((valueRow) => [...valueRow]) } };
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
    process.env.SHEETS_KEY = JSON.stringify({
      client_email: 'svc@example.com',
      private_key: '-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n',
    });
    process.env.SHEET_ID = 'source-sheet';
    process.env.MERGED_SHEET_ID = 'target-sheet';
    workbookAccess.set('source-sheet', { read: true, write: true });
    workbookAccess.set('target-sheet', { read: true, write: true });
  });

  it('publishes merged scheduled rows into the destination workbook', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_CA: [['ca-1'], ['ca-2']],
      Scheduled_NYC: [['nyc-1']],
      Master: [['old-master']],
    });
    seedWorkbook('target-sheet', {
      Master: [['stale-target']],
    });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 3,
      source_tabs: 2,
      target_spreadsheet_id: 'target-sheet',
      fallback_used: false,
    }));
    expect(ensureWorkbook('target-sheet').get('Master')?.rows).toEqual([['ca-1'], ['ca-2'], ['nyc-1']]);
    expect(ensureWorkbook('source-sheet').get('Master')?.rows).toEqual([['old-master']]);
  });

  it('falls back to the source workbook when the destination sheet is not writable', async () => {
    seedWorkbook('source-sheet', {
      Scheduled_CA: [['ca-1']],
      Scheduled_NYC: [['nyc-1']],
      Master: [['old-master']],
    });
    seedWorkbook('target-sheet', {
      Master: [['stale-target']],
    });
    workbookAccess.set('target-sheet', { read: true, write: false });

    const { syncMasterSheetTab } = await import('../../src/sheets/push');
    const result = await syncMasterSheetTab();

    expect(result).toEqual(expect.objectContaining({
      row_count: 2,
      source_tabs: 2,
      target_spreadsheet_id: 'source-sheet',
      fallback_used: true,
    }));
    expect(ensureWorkbook('source-sheet').get('Master')?.rows).toEqual([['ca-1'], ['nyc-1']]);
    expect(ensureWorkbook('target-sheet').get('Master')?.rows).toEqual([['stale-target']]);
  });
});
