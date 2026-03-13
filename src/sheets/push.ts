import { google } from 'googleapis';
import { LienRecord } from '../types';
import { log } from '../utils/logger';
import { siteExportConfig, type SupportedSite } from '../sites';

type BusinessFlag = 'Business' | 'Personal';

const INVALID_SHEET_TITLE_CHARS_REGEX = /[[\]/?*:]/g;
const DEFAULT_MERGED_SHEET_ID = '1qa32AEUMC4TYHh4G6AV4msRS4GjQQZFTNPpYHMh4n5A';
const DEFAULT_MASTER_TAB_TITLE = 'Master';
const DEFAULT_REVIEW_TAB_TITLE = 'Review_Queue';
const DEFAULT_DIRECTOR_MIN_CONFIDENCE_ACCEPT = 0.85;
const DEFAULT_DIRECTOR_MIN_CONFIDENCE_REVIEW = 0.75;
const DEFAULT_REVIEW_QUEUE_RETENTION_DAYS = 7;

export const DIRECTOR_SHEET_HEADERS = [
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
] as const;

export const FROZEN_SHEET_HEADERS = [
  ...DIRECTOR_SHEET_HEADERS,
  'RecordSource',
  'FileNumber',
  'RunPartial',
] as const;

export const REVIEW_QUEUE_HEADERS = [
  ...DIRECTOR_SHEET_HEADERS,
  'RecordSource',
  'FileNumber',
  'ReviewReason',
] as const;

const SOURCE_SHEET_COLUMN_COUNT = FROZEN_SHEET_HEADERS.length;
const SOURCE_HEADER_END_COLUMN = String.fromCharCode('A'.charCodeAt(0) + SOURCE_SHEET_COLUMN_COUNT - 1);
const DIRECTOR_SHEET_COLUMN_COUNT = DIRECTOR_SHEET_HEADERS.length;
const DIRECTOR_HEADER_END_COLUMN = String.fromCharCode('A'.charCodeAt(0) + DIRECTOR_SHEET_COLUMN_COUNT - 1);
const REVIEW_SHEET_COLUMN_COUNT = REVIEW_QUEUE_HEADERS.length;
const REVIEW_HEADER_END_COLUMN = String.fromCharCode('A'.charCodeAt(0) + REVIEW_SHEET_COLUMN_COUNT - 1);

const DIRECTOR_COLUMN = {
  siteId: 0,
  filingDate: 1,
  amount: 2,
  leadType: 3,
  leadSource: 4,
  liabilityType: 5,
  businessPersonal: 6,
  company: 7,
  firstName: 8,
  lastName: 9,
  street: 10,
  city: 11,
  state: 12,
  zip: 13,
  confidenceScore: 14,
} as const;

const SOURCE_METADATA_COLUMN = {
  recordSource: 15,
  fileNumber: 16,
  runPartial: 17,
} as const;

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

  const parts = cleaned
    .split(/\s+/)
    .map((part) => part.replace(/^[^A-Za-z0-9]+|[^A-Za-z0-9]+$/g, ''))
    .filter((part) => /[A-Za-z0-9]/.test(part));
  if (parts.length === 0) return { firstName: '', lastName: '' };
  if (parts.length === 1) return { firstName: parts[0], lastName: '' };
  return {
    firstName: parts[0],
    lastName: parts[parts.length - 1],
  };
}

function normalizeAddressForParsing(raw: string): string {
  const collapsed = raw
    .replace(/_+/g, ' ')
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
    if (!suffix || /^[._,;:|/\\()\-[\]\s]*\d{0,2}[A-Za-z]?$/.test(suffix)) {
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

function getSourceSpreadsheetId(): string {
  return getRequiredEnv('SHEET_ID');
}

function getMergedSpreadsheetId(): string {
  const configured = process.env.MERGED_SHEET_ID?.trim();
  return configured || DEFAULT_MERGED_SHEET_ID;
}

function getSheetsClient() {
  const sanitizedKey = getRequiredEnv('SHEETS_KEY').replace(/^'+|'+$/g, '');
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(sanitizedKey),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  return google.sheets({ version: 'v4', auth });
}

export interface SpreadsheetAccessResult {
  ok: boolean;
  detail?: string;
}

export interface MergedSheetTargetConfig {
  source_spreadsheet_id: string;
  target_spreadsheet_id: string;
  fallback_tab_title: string;
  target_tab_title: string;
  review_tab_title: string;
  default_target_used: boolean;
}

export interface MasterSheetSyncResult {
  tab_title: string;
  row_count: number;
  source_tabs: number;
  target_spreadsheet_id: string;
  fallback_used: boolean;
  quarantined_row_count: number;
  review_tab_title: string;
  new_master_row_count: number;
  purged_review_row_count: number;
  review_summary: ReviewClassificationSummary;
}

type BuildRowOptions = {
  runPartial?: boolean;
};

type ReviewReason =
  | 'missing_required_fields'
  | 'missing_name_fields'
  | 'name_conflict'
  | 'address_incomplete'
  | 'legacy_missing_metadata'
  | 'partial_run'
  | 'low_confidence';

type ReviewReasonCategory = 'hard' | 'soft';

type DirectorCandidateDisposition = 'accepted' | 'quarantined';

type ReviewReasonCounts = Partial<Record<ReviewReason | 'conflict_lower_confidence' | 'conflict_ambiguous' | 'missing_identity' | 'quarantined', number>>;

export interface ReviewClassificationSummary {
  accepted_row_count: number;
  quarantined_row_count: number;
  purged_review_row_count: number;
  review_reason_counts: ReviewReasonCounts;
}

type DirectorCandidate = {
  directorRow: any[];
  recordSource: string;
  fileNumber: string;
  runPartial: boolean;
  confidenceScore: number;
  reviewReasons: ReviewReason[];
  hardReasons: ReviewReason[];
  softReasons: ReviewReason[];
  disposition: DirectorCandidateDisposition;
  sourceTabCapturedAt?: Date;
};

type CollectedSourceRow = {
  row: any[];
  sourceTab: string;
  sourceTabCapturedAt?: Date;
};

function directorHeadersEndColumn(headers: readonly string[]): string {
  return String.fromCharCode('A'.charCodeAt(0) + headers.length - 1);
}

function getReviewQueueRetentionDays(): number {
  const configured = Number.parseInt(process.env.REVIEW_QUEUE_RETENTION_DAYS ?? '', 10);
  if (!Number.isFinite(configured) || configured < 0) return DEFAULT_REVIEW_QUEUE_RETENTION_DAYS;
  return configured;
}

function getDirectorMinConfidenceAccept(): number {
  const configured = Number.parseFloat(process.env.DIRECTOR_MIN_CONFIDENCE_ACCEPT ?? '');
  if (!Number.isFinite(configured)) return DEFAULT_DIRECTOR_MIN_CONFIDENCE_ACCEPT;
  return Math.max(0, Math.min(configured, 1));
}

function getDirectorMinConfidenceReview(): number {
  const configured = Number.parseFloat(process.env.DIRECTOR_MIN_CONFIDENCE_REVIEW ?? '');
  if (!Number.isFinite(configured)) return DEFAULT_DIRECTOR_MIN_CONFIDENCE_REVIEW;
  return Math.max(0, Math.min(configured, 1));
}

function getReviewQueueRetentionCutoff(now = new Date()): Date | undefined {
  const retentionDays = getReviewQueueRetentionDays();
  if (retentionDays <= 0) return undefined;

  const cutoff = new Date(now);
  cutoff.setUTCDate(cutoff.getUTCDate() - retentionDays);
  return cutoff;
}

function parseSourceTabCapturedAt(sourceTab: string): Date | undefined {
  const pacificTimestamp = sourceTab.match(/_(\d{8}T\d{6})(?:_Pacific)?$/);
  if (pacificTimestamp) {
    const [, compact] = pacificTimestamp;
    const parsed = new Date(`${compact.slice(0, 4)}-${compact.slice(4, 6)}-${compact.slice(6, 8)}T${compact.slice(9, 11)}:${compact.slice(11, 13)}:${compact.slice(13, 15)}-08:00`);
    return Number.isNaN(parsed.getTime()) ? undefined : parsed;
  }

  const dateStamp = sourceTab.match(/_(\d{8})(?:_Pacific)?$/);
  if (dateStamp) {
    const [, compact] = dateStamp;
    const parsed = new Date(`${compact.slice(0, 4)}-${compact.slice(4, 6)}-${compact.slice(6, 8)}T12:00:00Z`);
    return Number.isNaN(parsed.getTime()) ? undefined : parsed;
  }

  const dateRange = sourceTab.match(/_(\d{2})-(\d{2})-(\d{4})_to_(\d{2})-(\d{2})-(\d{4})(?:_|$)/);
  if (dateRange) {
    const [, , , , endMonth, endDay, endYear] = dateRange;
    const parsed = new Date(`${endYear}-${endMonth}-${endDay}T12:00:00Z`);
    return Number.isNaN(parsed.getTime()) ? undefined : parsed;
  }

  return undefined;
}

function serializeDirectorRow(row: any[]): string {
  return JSON.stringify(row.map((value) => stringValue(value)));
}

export function getMergedSheetTargetConfig(): MergedSheetTargetConfig {
  const source_spreadsheet_id = getSourceSpreadsheetId();
  const configured = process.env.MERGED_SHEET_ID?.trim();
  const target_spreadsheet_id = configured || DEFAULT_MERGED_SHEET_ID;

  return {
    source_spreadsheet_id,
    target_spreadsheet_id,
    fallback_tab_title: DEFAULT_MASTER_TAB_TITLE,
    target_tab_title: DEFAULT_MASTER_TAB_TITLE,
    review_tab_title: DEFAULT_REVIEW_TAB_TITLE,
    default_target_used: !configured,
  };
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

export async function checkSpreadsheetAccess(spreadsheetId: string): Promise<SpreadsheetAccessResult> {
  try {
    const sheets = getSheetsClient();
    await sheets.spreadsheets.get({ spreadsheetId, fields: 'spreadsheetId' });
    return { ok: true };
  } catch (err: any) {
    return {
      ok: false,
      detail: String(err?.message ?? err),
    };
  }
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
  tabTitle: string,
  headers: readonly string[]
): Promise<void> {
  const sheetId = await getSheetIdByTitle(sheets, spreadsheetId, tabTitle);
  const endColumn = directorHeadersEndColumn(headers);

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `'${tabTitle}'!A1:${endColumn}1`,
    valueInputOption: 'RAW',
    requestBody: { values: [Array.from(headers)] },
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
              endColumnIndex: headers.length,
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

function normalizeConfidenceScore(score?: number): number {
  if (typeof score !== 'number' || Number.isNaN(score)) return 0;
  return Number(score.toFixed(2));
}

function buildDirectorRowValue(r: LienRecord): any[] {
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
    normalizeConfidenceScore(r.confidence_score ?? r.amount_confidence),
  ];
}

export function buildRowValues(rows: LienRecord[], options: BuildRowOptions = {}) {
  return rows.map((row) => {
    const directorRow = buildDirectorRowValue(row);
    return [
      ...directorRow,
      row.source ?? '',
      row.file_number ?? '',
      options.runPartial ? '1' : '0',
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
  await initializeSheetHeaderRow(sheets, process.env.SHEET_ID, 'Records', FROZEN_SHEET_HEADERS);
  const values = buildRowValues(rows);

  const spreadsheetId = process.env.SHEET_ID;
  log({
    stage: 'sheets_append_records',
    spreadsheet_id_suffix: spreadsheetId?.slice(-6),
    target_range: `Records!A2:${SOURCE_HEADER_END_COLUMN}`,
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
  requestedTabTitle: string,
  options: BuildRowOptions = {}
): Promise<{ uploaded: number; tab_title: string }> {
  const spreadsheetId = getSourceSpreadsheetId();
  const sheets = getSheetsClient();

  const tabTitle = await ensureSheetTabExists(sheets, spreadsheetId, requestedTabTitle);
  await initializeSheetHeaderRow(sheets, spreadsheetId, tabTitle, FROZEN_SHEET_HEADERS);
  const values = buildRowValues(rows, options);

  const range = `'${tabTitle}'!A2`;

  log({
    stage: 'sheets_append_tab',
    spreadsheet_id_suffix: spreadsheetId.slice(-6),
    tab_title: tabTitle,
    target_range: `${range}:${SOURCE_HEADER_END_COLUMN}`,
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

async function collectRowsFromSourceTabs(
  sheets: ReturnType<typeof google.sheets>,
  spreadsheetId: string,
  includePrefixes: string[],
  masterTabTitle: string,
  reviewTabTitle: string
): Promise<{ rows: CollectedSourceRow[]; sourceTabs: string[] }> {
  const existingTitles = await listSheetTitles(sheets, spreadsheetId);
  const sourceTabs = existingTitles.filter((title) =>
    title !== masterTabTitle &&
    title !== reviewTabTitle &&
    includePrefixes.some((prefix) => title.startsWith(prefix))
  );

  const rows: CollectedSourceRow[] = [];
  for (const sourceTab of sourceTabs) {
    const range = `'${sourceTab}'!A2:${SOURCE_HEADER_END_COLUMN}`;
    const response = await sheets.spreadsheets.values.get({ spreadsheetId, range });
    const values = response.data.values ?? [];
    const sourceTabCapturedAt = parseSourceTabCapturedAt(sourceTab);
    for (const valueRow of values) rows.push({ row: valueRow, sourceTab, sourceTabCapturedAt });
  }

  return { rows, sourceTabs };
}

function stringValue(value: unknown): string {
  return typeof value === 'string' ? value.trim() : String(value ?? '').trim();
}

function parseRunPartial(raw: unknown): boolean {
  return stringValue(raw) === '1';
}

function parseConfidence(raw: unknown): number {
  const numeric = Number.parseFloat(stringValue(raw));
  if (Number.isNaN(numeric)) return 0;
  return numeric;
}

function directorRowFromSourceRow(row: any[]): any[] {
  return row.slice(0, DIRECTOR_SHEET_HEADERS.length);
}

function reviewReasonCategory(reason: ReviewReason): ReviewReasonCategory {
  switch (reason) {
    case 'missing_required_fields':
    case 'missing_name_fields':
    case 'name_conflict':
    case 'address_incomplete':
    case 'legacy_missing_metadata':
      return 'hard';
    case 'partial_run':
    case 'low_confidence':
      return 'soft';
  }
}

function categorizeReviewReasons(reviewReasons: ReviewReason[]): { hardReasons: ReviewReason[]; softReasons: ReviewReason[] } {
  const hardReasons: ReviewReason[] = [];
  const softReasons: ReviewReason[] = [];

  for (const reason of reviewReasons) {
    if (reviewReasonCategory(reason) === 'hard') {
      hardReasons.push(reason);
    } else {
      softReasons.push(reason);
    }
  }

  return { hardReasons, softReasons };
}

function resolveCandidateDisposition(hardReasons: ReviewReason[], softReasons: ReviewReason[], confidenceScore: number): DirectorCandidateDisposition {
  if (hardReasons.length > 0) return 'quarantined';

  const reviewThreshold = getDirectorMinConfidenceReview();
  const softTolerance = confidenceScore < reviewThreshold ? 0 : 1;

  return softReasons.length > softTolerance ? 'quarantined' : 'accepted';
}

function buildCandidate(sourceRow: CollectedSourceRow): DirectorCandidate {
  const row = sourceRow.row;
  const directorRow = directorRowFromSourceRow(row);
  const recordSource = stringValue(row[SOURCE_METADATA_COLUMN.recordSource]);
  const fileNumber = stringValue(row[SOURCE_METADATA_COLUMN.fileNumber]);
  const runPartial = parseRunPartial(row[SOURCE_METADATA_COLUMN.runPartial]);
  const confidenceScore = parseConfidence(directorRow[DIRECTOR_COLUMN.confidenceScore]);
  const reviewReasons: ReviewReason[] = [];

  const requiredValues = [
    directorRow[DIRECTOR_COLUMN.siteId],
    directorRow[DIRECTOR_COLUMN.filingDate],
    directorRow[DIRECTOR_COLUMN.amount],
    directorRow[DIRECTOR_COLUMN.leadType],
    directorRow[DIRECTOR_COLUMN.leadSource],
    directorRow[DIRECTOR_COLUMN.liabilityType],
    directorRow[DIRECTOR_COLUMN.businessPersonal],
    directorRow[DIRECTOR_COLUMN.street],
    directorRow[DIRECTOR_COLUMN.city],
    directorRow[DIRECTOR_COLUMN.state],
    directorRow[DIRECTOR_COLUMN.zip],
  ].map(stringValue);

  if (requiredValues.some((value) => !value)) {
    reviewReasons.push('missing_required_fields');
  }

  const company = stringValue(directorRow[DIRECTOR_COLUMN.company]);
  const firstName = stringValue(directorRow[DIRECTOR_COLUMN.firstName]);
  const lastName = stringValue(directorRow[DIRECTOR_COLUMN.lastName]);
  const businessPersonal = stringValue(directorRow[DIRECTOR_COLUMN.businessPersonal]);

  if (!company && !(firstName && lastName)) {
    reviewReasons.push('missing_name_fields');
  }

  if (
    (businessPersonal === 'Business' && ((!company) || Boolean(firstName || lastName))) ||
    (businessPersonal === 'Personal' && (Boolean(company) || !(firstName && lastName)))
  ) {
    reviewReasons.push('name_conflict');
  }

  if (!stringValue(directorRow[DIRECTOR_COLUMN.street]) || !stringValue(directorRow[DIRECTOR_COLUMN.city]) || !stringValue(directorRow[DIRECTOR_COLUMN.state]) || !stringValue(directorRow[DIRECTOR_COLUMN.zip])) {
    reviewReasons.push('address_incomplete');
  }

  if (runPartial) {
    reviewReasons.push('partial_run');
  }

  if (confidenceScore < getDirectorMinConfidenceAccept()) {
    reviewReasons.push('low_confidence');
  }

  if (!recordSource || !fileNumber) {
    reviewReasons.push('legacy_missing_metadata');
  }

  const dedupedReviewReasons = Array.from(new Set(reviewReasons));
  const { hardReasons, softReasons } = categorizeReviewReasons(dedupedReviewReasons);

  return {
    directorRow,
    recordSource,
    fileNumber,
    runPartial,
    confidenceScore,
    reviewReasons: dedupedReviewReasons,
    hardReasons,
    softReasons,
    disposition: resolveCandidateDisposition(hardReasons, softReasons, confidenceScore),
    sourceTabCapturedAt: sourceRow.sourceTabCapturedAt,
  };
}

function buildReviewRow(candidate: DirectorCandidate, reason: string): any[] {
  return [
    ...candidate.directorRow,
    candidate.recordSource,
    candidate.fileNumber,
    reason,
  ];
}

function shouldRetainReviewCandidate(candidate: DirectorCandidate, cutoff?: Date): boolean {
  if (!cutoff || !candidate.sourceTabCapturedAt) return true;
  return candidate.sourceTabCapturedAt >= cutoff;
}

function candidateSortValue(candidate: DirectorCandidate): [number, number, number, number, number] {
  return [
    candidate.hardReasons.length,
    candidate.reviewReasons.length,
    -candidate.confidenceScore,
    candidate.runPartial ? 1 : 0,
    -(candidate.sourceTabCapturedAt?.getTime() ?? 0),
  ];
}

function compareCandidatePreference(left: DirectorCandidate, right: DirectorCandidate): number {
  const leftValue = candidateSortValue(left);
  const rightValue = candidateSortValue(right);

  for (let index = 0; index < leftValue.length; index += 1) {
    const delta = leftValue[index] - rightValue[index];
    if (delta !== 0) return delta;
  }

  return 0;
}

function buildQuarantineReason(candidate: DirectorCandidate, fallbackReason: 'conflict_lower_confidence' | 'conflict_ambiguous' | 'missing_identity' | 'quarantined'): string {
  return candidate.reviewReasons.join('|') || fallbackReason;
}

function accumulateReviewReasonCounts(counts: ReviewReasonCounts, reasonText: string): void {
  const reasons = reasonText
    .split('|')
    .map((reason) => reason.trim())
    .filter(Boolean) as Array<keyof ReviewReasonCounts>;

  for (const reason of reasons) {
    counts[reason] = (counts[reason] ?? 0) + 1;
  }
}

function quarantineCandidate(
  candidate: DirectorCandidate,
  quarantinedRows: any[][],
  reviewReasonCounts: ReviewReasonCounts,
  cutoff: Date | undefined,
  fallbackReason: 'conflict_lower_confidence' | 'conflict_ambiguous' | 'missing_identity' | 'quarantined'
): boolean {
  const reason = buildQuarantineReason(candidate, fallbackReason);
  if (!shouldRetainReviewCandidate(candidate, cutoff)) {
    return false;
  }

  quarantinedRows.push(buildReviewRow(candidate, reason));
  accumulateReviewReasonCounts(reviewReasonCounts, reason);
  return true;
}

export function classifyMergedRows(
  rows: CollectedSourceRow[],
  options: { reviewRetentionCutoff?: Date } = {}
): {
  acceptedRows: any[][];
  quarantinedRows: any[][];
  purgedReviewRowCount: number;
  reviewSummary: ReviewClassificationSummary;
} {
  const candidates = rows.map(buildCandidate);
  const acceptedRows: any[][] = [];
  const quarantinedRows: any[][] = [];
  let purgedReviewRowCount = 0;
  const reviewReasonCounts: ReviewReasonCounts = {};
  const grouped = new Map<string, DirectorCandidate[]>();

  for (const candidate of candidates) {
    const key = candidate.recordSource && candidate.fileNumber
      ? `${candidate.recordSource}::${candidate.fileNumber}`
      : '';

    if (!key) {
      const retained = quarantineCandidate(
        candidate,
        quarantinedRows,
        reviewReasonCounts,
        options.reviewRetentionCutoff,
        'missing_identity'
      );
      if (!retained) {
        purgedReviewRowCount += 1;
      }
      continue;
    }

    const bucket = grouped.get(key) ?? [];
    bucket.push(candidate);
    grouped.set(key, bucket);
  }

  for (const bucket of grouped.values()) {
    const acceptedCandidates = bucket.filter((candidate) => candidate.disposition === 'accepted');

    if (acceptedCandidates.length === 0) {
      for (const candidate of bucket) {
        const retained = quarantineCandidate(
          candidate,
          quarantinedRows,
          reviewReasonCounts,
          options.reviewRetentionCutoff,
          'quarantined'
        );
        if (!retained) purgedReviewRowCount += 1;
      }
      continue;
    }

    const sortedAccepted = [...acceptedCandidates].sort(compareCandidatePreference);
    const preferredCandidate = sortedAccepted[0];
    const equallyPreferred = sortedAccepted.filter((candidate) => compareCandidatePreference(candidate, preferredCandidate) === 0);

    if (equallyPreferred.length !== 1) {
      for (const candidate of bucket) {
        const retained = quarantineCandidate(
          candidate,
          quarantinedRows,
          reviewReasonCounts,
          options.reviewRetentionCutoff,
          'conflict_ambiguous'
        );
        if (!retained) purgedReviewRowCount += 1;
      }
      continue;
    }

    acceptedRows.push(preferredCandidate.directorRow);
    for (const candidate of bucket.filter((entry) => entry !== preferredCandidate)) {
      const retained = quarantineCandidate(
        candidate,
        quarantinedRows,
        reviewReasonCounts,
        options.reviewRetentionCutoff,
        'conflict_lower_confidence'
      );
      if (!retained) purgedReviewRowCount += 1;
    }
  }

  return {
    acceptedRows,
    quarantinedRows,
    purgedReviewRowCount,
    reviewSummary: {
      accepted_row_count: acceptedRows.length,
      quarantined_row_count: quarantinedRows.length,
      purged_review_row_count: purgedReviewRowCount,
      review_reason_counts: reviewReasonCounts,
    },
  };
}

async function getExistingDirectorRows(
  sheets: ReturnType<typeof google.sheets>,
  spreadsheetId: string,
  tabTitle: string
): Promise<any[][]> {
  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `'${tabTitle}'!A2:${DIRECTOR_HEADER_END_COLUMN}`,
    });
    return response.data.values ?? [];
  } catch {
    return [];
  }
}

async function writeRowsToTab(
  sheets: ReturnType<typeof google.sheets>,
  spreadsheetId: string,
  tabTitle: string,
  headers: readonly string[],
  endColumn: string,
  rows: any[][]
): Promise<string> {
  const ensuredTabTitle = await ensureSheetTabExists(sheets, spreadsheetId, tabTitle);
  await initializeSheetHeaderRow(sheets, spreadsheetId, ensuredTabTitle, headers);

  await sheets.spreadsheets.values.clear({
    spreadsheetId,
    range: `'${ensuredTabTitle}'!A2:${endColumn}`,
  });

  if (rows.length > 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `'${ensuredTabTitle}'!A2`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: rows },
    });
  }

  return ensuredTabTitle;
}

async function writeMergedRowsToWorkbook(
  sheets: ReturnType<typeof google.sheets>,
  spreadsheetId: string,
  masterTabTitle: string,
  reviewTabTitle: string,
  acceptedRows: any[][],
  quarantinedRows: any[][]
): Promise<{ masterTabTitle: string; reviewTabTitle: string }> {
  const ensuredMasterTabTitle = await writeRowsToTab(
    sheets,
    spreadsheetId,
    masterTabTitle,
    DIRECTOR_SHEET_HEADERS,
    DIRECTOR_HEADER_END_COLUMN,
    acceptedRows
  );
  const ensuredReviewTabTitle = await writeRowsToTab(
    sheets,
    spreadsheetId,
    reviewTabTitle,
    REVIEW_QUEUE_HEADERS,
    REVIEW_HEADER_END_COLUMN,
    quarantinedRows
  );

  return {
    masterTabTitle: ensuredMasterTabTitle,
    reviewTabTitle: ensuredReviewTabTitle,
  };
}

export async function syncMasterSheetTab(options: {
  tabTitle?: string;
  reviewTabTitle?: string;
  includePrefixes?: string[];
  sourceSpreadsheetId?: string;
  targetSpreadsheetId?: string;
} = {}): Promise<MasterSheetSyncResult> {
  const sourceSpreadsheetId = options.sourceSpreadsheetId ?? getSourceSpreadsheetId();
  const configuredTargetSpreadsheetId = options.targetSpreadsheetId ?? getMergedSpreadsheetId();
  const sheets = getSheetsClient();
  const tabTitle = options.tabTitle ?? DEFAULT_MASTER_TAB_TITLE;
  const reviewTabTitle = options.reviewTabTitle ?? DEFAULT_REVIEW_TAB_TITLE;
  const includePrefixes = options.includePrefixes ?? ['Scheduled_'];
  const { rows, sourceTabs } = await collectRowsFromSourceTabs(sheets, sourceSpreadsheetId, includePrefixes, tabTitle, reviewTabTitle);
  const { acceptedRows, quarantinedRows, purgedReviewRowCount, reviewSummary } = classifyMergedRows(rows, {
    reviewRetentionCutoff: getReviewQueueRetentionCutoff(),
  });

  const finalize = async (targetSpreadsheetId: string, fallbackUsed: boolean) => {
    const existingMasterRows = await getExistingDirectorRows(sheets, targetSpreadsheetId, tabTitle);
    const existingMasterKeys = new Set(existingMasterRows.map(serializeDirectorRow));
    const newMasterRowCount = acceptedRows.filter((row) => !existingMasterKeys.has(serializeDirectorRow(row))).length;
    const { masterTabTitle, reviewTabTitle: ensuredReviewTabTitle } = await writeMergedRowsToWorkbook(
      sheets,
      targetSpreadsheetId,
      tabTitle,
      reviewTabTitle,
      acceptedRows,
      quarantinedRows
    );

    log({
      stage: 'sheets_master_sync_complete',
      source_spreadsheet_id_suffix: sourceSpreadsheetId.slice(-6),
      target_spreadsheet_id_suffix: targetSpreadsheetId.slice(-6),
      tab_title: masterTabTitle,
      review_tab_title: ensuredReviewTabTitle,
      source_tabs: sourceTabs.length,
      row_count: acceptedRows.length,
      quarantined_row_count: quarantinedRows.length,
      accepted_row_count: reviewSummary.accepted_row_count,
      review_reason_counts: reviewSummary.review_reason_counts,
      new_master_row_count: newMasterRowCount,
      purged_review_row_count: purgedReviewRowCount,
      fallback_used: fallbackUsed,
    });

    return {
      tab_title: masterTabTitle,
      row_count: acceptedRows.length,
      source_tabs: sourceTabs.length,
      target_spreadsheet_id: targetSpreadsheetId,
      fallback_used: fallbackUsed,
      quarantined_row_count: quarantinedRows.length,
      review_tab_title: ensuredReviewTabTitle,
      new_master_row_count: newMasterRowCount,
      purged_review_row_count: purgedReviewRowCount,
      review_summary: reviewSummary,
    };
  };

  try {
    return await finalize(configuredTargetSpreadsheetId, false);
  } catch (err: any) {
    if (configuredTargetSpreadsheetId === sourceSpreadsheetId) {
      throw err;
    }

    log({
      stage: 'sheets_master_sync_fallback',
      source_spreadsheet_id_suffix: sourceSpreadsheetId.slice(-6),
      target_spreadsheet_id_suffix: configuredTargetSpreadsheetId.slice(-6),
      fallback_spreadsheet_id_suffix: sourceSpreadsheetId.slice(-6),
      reason: String(err?.message ?? err),
    });

    return finalize(sourceSpreadsheetId, true);
  }
}

export async function pushRunToNewSheetTab(
  rows: LienRecord[],
  options: {
    label: string;
    date_start: string;
    date_end: string;
    run_started_at?: Date;
    run_partial?: boolean;
  }
): Promise<{ uploaded: number; tab_title: string }> {
  const tabTitle = formatRunTabName(
    options.label,
    options.date_start,
    options.date_end,
    options.run_started_at ?? new Date()
  );

  return pushToSheetsForTab(rows, tabTitle, {
    runPartial: options.run_partial,
  });
}
