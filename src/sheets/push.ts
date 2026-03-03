import { google } from 'googleapis';
import { LienRecord } from '../types';
import { log } from '../utils/logger';

const SITE_ID_CA_SOS = 11; // from your Lien Sites sheet for CA SOS
const LEAD_SOURCE = '777';
const LIABILITY_TYPE = 'IRS';

type BusinessFlag = 'Business' | 'Personal';

const INVALID_SHEET_TITLE_CHARS_REGEX = /[\\/?*\[\]:]/g;

function getEasternTimestampForTab(d: Date): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(d);

  const get = (type: string) => parts.find(p => p.type === type)?.value ?? '';
  const yyyy = get('year');
  const mm = get('month');
  const dd = get('day');
  const hh = get('hour');
  const min = get('minute');

  return `${yyyy}-${mm}-${dd} ${hh}:${min}`;
}

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

  const zipMatch = raw.match(/(\d{5})(?:-\d{4})?$/);
  if (zipMatch) {
    zip = zipMatch[1];
  }

  const stateMatch = raw.match(/\b([A-Z]{2})\s+\d{5}(?:-\d{4})?$/);
  if (stateMatch) {
    state = stateMatch[1];
  }

  const cityMatch = raw.match(/,\s*([^,]+),\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?$/);
  if (cityMatch) {
    city = cityMatch[1].trim();
  }

  if (city) {
    street = raw.split(',')[0].trim();
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

export function buildCaliforniaRunTabTitle(runStartedAt: Date): string {
  const ts = getEasternTimestampForTab(runStartedAt);
  const raw = `California ${ts}`;
  return sanitizeSheetTitle(raw);
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

function buildRowValues(rows: LienRecord[]) {
  return rows.map(r => {
    const businessPersonal = classifyBusinessPersonal(r.debtor_name);
    const nameParts =
      businessPersonal === 'Personal'
        ? splitPersonalName(r.debtor_name)
        : { firstName: '', lastName: '' };

    const addrParts = parseAddress(r.debtor_address, r.state);

    return [
      SITE_ID_CA_SOS,
      r.filing_date,
      r.amount ?? '',
      r.lead_type ?? 'Lien',
      LEAD_SOURCE,
      LIABILITY_TYPE,
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
