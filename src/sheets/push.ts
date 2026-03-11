import { google } from 'googleapis';
import { LienRecord } from '../types';
import { log } from '../utils/logger';
import { siteExportConfig, type SupportedSite } from '../sites';

type BusinessFlag = 'Business' | 'Personal';

const INVALID_SHEET_TITLE_CHARS_REGEX = /[[\]/?*:]/g;
const SHEET_HEADERS = [
  'site_id',
  'filing_date',
  'amount',
  'confidence_score',
  'lead_type',
  'lead_source',
  'liability_type',
  'business_personal',
  'business_name',
  'first_name',
  'last_name',
  'street',
  'city',
  'state',
  'zip',
];

function getPacificTimestampForTab(d: Date): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Los_Angeles',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(d);

  const get = (type: string) => parts.find(p => p.type === type)?.value ?? '';
  const yyyy = get('year');
  const mm = get('month');
  const dd = get('day');
  const hh = get('hour');
  const min = get('minute');
  const ss = get('second');

  return `${yyyy}${mm}${dd}T${hh}${min}${ss}`;
}

function classifyBusinessPersonal(name: string): BusinessFlag {
  const upper = name.toUpperCase();
  const businessKeywords = [
    ' INC',
    ' LLC',
    ' LLP',
    ' LP',
    ' LTD',
    ' CO ',
    ' CO.',
    ' COMPANY',
    ' CORPORATION',
    ' CORP',
    ' PLLC',
    ' PC',
    ' GROUP',
    ' HOLDINGS',
  ];
  if (businessKeywords.some(k => upper.includes(k.trim()))) return 'Business';
  return 'Personal';
}

function splitPersonalName(name: string): { firstName: string; lastName: string } {
  const andSplit = name.split(/\band\b|&/i);
  const segment = andSplit[andSplit.length - 1].trim();

  const cleaned = segment
    .replace(/[,]/g, '')
    .replace(/\b(JR|SR|II|III|IV|V)\b\.?/gi, '')
    .trim();

  const parts = cleaned.split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { firstName: '', lastName: '' };
  if (parts.length === 1) return { firstName: parts[0], lastName: '' };
  return {
    firstName: parts[0],
    lastName: parts[parts.length - 1],
  };
}

function normalizeAddressForParsing(raw: string): string {
  const collapsed = raw
    .replace(/\s+/g, ' ')
    .replace(/\s+([,.;:])/g, '$1')
    .replace(/([,.;:])(?=[A-Za-z])/g, '$1 ')
    .replace(/,\s*,+/g, ',')
    .trim();

  const zipPattern = /\b\d{5}(?:-\d{4})?\b/g;
  let match: RegExpExecArray | null;
  let cleaned = collapsed;

  while ((match = zipPattern.exec(collapsed)) !== null) {
    const zipEnd = match.index + match[0].length;
    const suffix = collapsed.slice(zipEnd).trim();
    if (!suffix || /^[.,;:|/\\()\-[\]\s]*\d{0,2}[A-Za-z]?$/.test(suffix)) {
      cleaned = collapsed.slice(0, zipEnd);
    }
  }

  return cleaned.replace(/[.;:,]+$/g, '').trim();
}

function parseAddress(
  raw: string,
  stateFallback: string
): {
  street: string;
  city: string;
  state: string;
  zip: string;
} {
  let street = raw.trim();
  let city = '';
  let state = stateFallback;
  let zip = '';
  const normalizedRaw = normalizeAddressForParsing(raw);

  const zipMatch = normalizedRaw.match(/(\d{5})(?:-\d{4})?$/);
  if (zipMatch) {
    zip = zipMatch[1];
  }

  const stateMatch = normalizedRaw.match(/\b([A-Z]{2})\s+\d{5}(?:-\d{4})?$/);
  if (stateMatch) {
    state = stateMatch[1];
  }

  const cityMatch = normalizedRaw.match(/,\s*([^,]+),\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?$/);
  if (cityMatch) {
    city = cityMatch[1].trim();
  }

  if (city) {
    street = normalizedRaw.split(',')[0].trim();
    return { street, city, state, zip };
  }

  const normalized = normalizedRaw.replace(/\s+/g, ' ').trim();
  const stateZipMatch = normalized.match(/^(.*)\s+([A-Z]{2})\s+(\d{5})(?:-\d{4})?$/);
  if (stateZipMatch) {
    const [, beforeStateZip, parsedState, parsedZip] = stateZipMatch;
    const cleanedBeforeStateZip = beforeStateZip.replace(/[;,]+$/g, '').trim();
    const streetSuffixMatch = cleanedBeforeStateZip.match(
      /^(.*\b(?:ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|BLVD|BOULEVARD|LN|LANE|CT|COURT|PL|PLACE|PKWY|PARKWAY|WAY|TER|TERRACE|CIR|CIRCLE|HTS|HEIGHTS)\b(?:\s+(?:APT|UNIT|FL|FLOOR|SUITE|STE|#)\s*[A-Z0-9-]+)?)\s+(.+)$/i
    );
    if (streetSuffixMatch) {
      const [, parsedStreet, parsedCity] = streetSuffixMatch;
      street = parsedStreet.trim().replace(/[;,]+$/g, '');
      city = parsedCity.trim().replace(/[;,]+$/g, '');
      state = parsedState;
      zip = parsedZip;
    }
  }

  return { street, city, state, zip };
}

function getRequiredEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing ${name} environment variable`);
  return v;
}

function getSheetsClient() {
  const sanitizedKey = getRequiredEnv('SHEETS_KEY').replace(/^'+|'+$/g, '');
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(sanitizedKey),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  return google.sheets({ version: 'v4', auth });
}

function sanitizeSheetTitle(input: string): string {
  const trimmed = input.trim();
  const noInvalid = trimmed.replace(INVALID_SHEET_TITLE_CHARS_REGEX, '-');
  const noApostrophes = noInvalid.replace(/'/g, '');
  const collapsed = noApostrophes.replace(/\s+/g, ' ');
  return collapsed.slice(0, 100);
}

export function formatRunTabName(
  label: string,
  dateStart: string,
  dateEnd: string,
  runStartedAt: Date
): string {
  const safeLabel = (label || 'Run').trim().replace(/\s+/g, '_');
  const start = (dateStart || '').trim().replace(/\//g, '-');
  const end = (dateEnd || '').trim().replace(/\//g, '-');
  const ts = getPacificTimestampForTab(runStartedAt);
  const raw = `${safeLabel}_${start}_to_${end}_${ts}_Pacific`;
  return sanitizeSheetTitle(raw);
}

async function listSheetTitles(
  sheets: ReturnType<typeof google.sheets>,
  spreadsheetId: string
): Promise<string[]> {
  const res = await sheets.spreadsheets.get({ spreadsheetId });
  const titles =
    res.data.sheets
      ?.map(s => s.properties?.title)
      .filter((t): t is string => typeof t === 'string') ?? [];
  return titles;
}

async function ensureSheetTabExists(
  sheets: ReturnType<typeof google.sheets>,
  spreadsheetId: string,
  requestedTitle: string
): Promise<string> {
  const baseTitle = sanitizeSheetTitle(requestedTitle) || 'Run';
  const existing = new Set(await listSheetTitles(sheets, spreadsheetId));

  if (existing.has(baseTitle)) return baseTitle;

  let title = baseTitle;
  for (let i = 2; existing.has(title) && i < 1000; i++) {
    const suffix = `_${i}`;
    title =
      baseTitle.length + suffix.length <= 100
        ? `${baseTitle}${suffix}`
        : `${baseTitle.slice(0, 100 - suffix.length)}${suffix}`;
  }

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [{ addSheet: { properties: { title } } }],
    },
  });

  return title;
}

async function getSheetIdByTitle(
  sheets: ReturnType<typeof google.sheets>,
  spreadsheetId: string,
  title: string
): Promise<number> {
  const res = await sheets.spreadsheets.get({ spreadsheetId });
  const sheet = res.data.sheets?.find((s) => s.properties?.title === title);
  const sheetId = sheet?.properties?.sheetId;

  if (typeof sheetId !== 'number') {
    throw new Error(`Unable to resolve sheet id for tab: ${title}`);
  }

  return sheetId;
}

async function initializeSheetHeaderRow(
  sheets: ReturnType<typeof google.sheets>,
  spreadsheetId: string,
  tabTitle: string
): Promise<void> {
  const sheetId = await getSheetIdByTitle(sheets, spreadsheetId, tabTitle);

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `'${tabTitle}'!A1:O1`,
    valueInputOption: 'RAW',
    requestBody: { values: [SHEET_HEADERS] },
  });

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [
        {
          updateSheetProperties: {
            properties: {
              sheetId,
              gridProperties: { frozenRowCount: 1 },
            },
            fields: 'gridProperties.frozenRowCount',
          },
        },
        {
          repeatCell: {
            range: {
              sheetId,
              startRowIndex: 0,
              endRowIndex: 1,
              startColumnIndex: 0,
              endColumnIndex: SHEET_HEADERS.length,
            },
            cell: {
              userEnteredFormat: {
                textFormat: { bold: true },
              },
            },
            fields: 'userEnteredFormat.textFormat.bold',
          },
        },
      ],
    },
  });
}

export function buildRowValues(rows: LienRecord[]) {
  return rows.map(r => {
    const siteKey = (r.source in siteExportConfig ? r.source : 'ca_sos') as SupportedSite;
    const exportConfig = siteExportConfig[siteKey];
    const businessPersonal = classifyBusinessPersonal(r.debtor_name);
    const nameParts =
      businessPersonal === 'Personal'
        ? splitPersonalName(r.debtor_name)
        : { firstName: '', lastName: '' };

    const addrParts = parseAddress(r.debtor_address, r.state);

    return [
      exportConfig.siteId,
      r.filing_date,
      r.amount ?? '',
      r.confidence_score ?? r.amount_confidence ?? '',
      r.lead_type ?? 'Lien',
      exportConfig.leadSource,
      exportConfig.liabilityType,
      businessPersonal,
      businessPersonal === 'Business' ? r.debtor_name : '',
      nameParts.firstName,
      nameParts.lastName,
      addrParts.street,
      addrParts.city,
      addrParts.state,
      addrParts.zip,
    ];
  });
}

export async function pushToSheets(rows: LienRecord[]): Promise<{ uploaded: number }> {
  if (!process.env.SHEETS_KEY) {
    throw new Error('Missing SHEETS_KEY environment variable');
  }
  if (!process.env.SHEET_ID) {
    throw new Error('Missing SHEET_ID environment variable');
  }

  const sheets = getSheetsClient();
  const values = buildRowValues(rows);

  const spreadsheetId = process.env.SHEET_ID;
  log({
    stage: 'sheets_append_records',
    spreadsheet_id_suffix: spreadsheetId?.slice(-6),
    target_range: 'Records!A2',
    row_count: values.length,
  });

  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: 'Records!A2',
    valueInputOption: 'USER_ENTERED',
    requestBody: { values },
  });

  log({
    stage: 'sheets_append_records_complete',
    spreadsheet_id_suffix: spreadsheetId?.slice(-6),
    row_count: values.length,
  });

  return { uploaded: rows.length };
}

export async function pushToSheetsForTab(
  rows: LienRecord[],
  requestedTabTitle: string
): Promise<{ uploaded: number; tab_title: string }> {
  const spreadsheetId = getRequiredEnv('SHEET_ID');
  const sheets = getSheetsClient();

  const tabTitle = await ensureSheetTabExists(sheets, spreadsheetId, requestedTabTitle);
  await initializeSheetHeaderRow(sheets, spreadsheetId, tabTitle);
  const values = buildRowValues(rows);

  const range = `'${tabTitle}'!A2`;

  log({
    stage: 'sheets_append_tab',
    spreadsheet_id_suffix: spreadsheetId.slice(-6),
    tab_title: tabTitle,
    target_range: range,
    row_count: values.length,
  });

  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values },
  });

  log({
    stage: 'sheets_append_tab_complete',
    spreadsheet_id_suffix: spreadsheetId.slice(-6),
    tab_title: tabTitle,
    row_count: values.length,
  });

  return { uploaded: rows.length, tab_title: tabTitle };
}

export async function pushRunToNewSheetTab(
  rows: LienRecord[],
  options: {
    label: string;
    date_start: string;
    date_end: string;
    run_started_at?: Date;
  }
): Promise<{ uploaded: number; tab_title: string }> {
  const tabTitle = formatRunTabName(
    options.label,
    options.date_start,
    options.date_end,
    options.run_started_at ?? new Date()
  );

  return pushToSheetsForTab(rows, tabTitle);
}
