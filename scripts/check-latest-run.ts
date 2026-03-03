import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { google } from 'googleapis';

dotenv.config();

function getLatestCheckpoint(): { filePath: string; key: string } | null {
  const dir = path.join(process.cwd(), 'data/checkpoints');
  if (!fs.existsSync(dir)) return null;

  const files = fs
    .readdirSync(dir)
    .filter(f => f.startsWith('ca_sos_') && f.endsWith('.json'))
    .map(f => ({
      name: f,
      fullPath: path.join(dir, f),
      mtimeMs: fs.statSync(path.join(dir, f)).mtimeMs,
    }))
    .sort((a, b) => b.mtimeMs - a.mtimeMs);

  if (files.length === 0) return null;

  const latest = files[0];
  const key = latest.name.replace(/^ca_sos_/, '').replace(/\.json$/, '');
  return { filePath: latest.fullPath, key };
}

function getSheetsClient() {
  const rawKey = process.env.SHEETS_KEY;
  if (!rawKey) {
    throw new Error('Missing SHEETS_KEY environment variable');
  }
  const sanitizedKey = rawKey.replace(/^'+|'+$/g, '');
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(sanitizedKey),
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });

  return google.sheets({ version: 'v4', auth });
}

async function fetchLatestSheetRows(spreadsheetId: string, n: number) {
  const sheets = getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: 'Records!A2:O',
  });

  const values = res.data.values ?? [];
  const total = values.length;
  const start = Math.max(total - n, 0);
  const recent = values.slice(start);

  return { totalRows: total, recentRows: recent };
}

async function main() {
  const checkpoint = getLatestCheckpoint();
  if (!checkpoint) {
    console.log('No CA SOS checkpoint files found in data/checkpoints.');
    return;
  }

  const checkpointData = JSON.parse(fs.readFileSync(checkpoint.filePath, 'utf8')) as {
    next_index?: number;
    updated_at?: string;
  };

  console.log('Latest checkpoint:');
  console.log(`  File: ${checkpoint.filePath}`);
  console.log(`  Key: ${checkpoint.key}`);
  console.log(`  Next index: ${checkpointData.next_index ?? 'unknown'}`);
  console.log(`  Updated at: ${checkpointData.updated_at ?? 'unknown'}`);

  const sheetId = process.env.SHEET_ID;
  if (!sheetId) {
    console.log('SHEET_ID is not set; skipping Sheets verification.');
    return;
  }

  const sheetSummary = await fetchLatestSheetRows(sheetId, 5);
  console.log('\nGoogle Sheets summary (Records tab):');
  console.log(`  Total rows (excluding header): ${sheetSummary.totalRows}`);
  console.log('  Last up to 5 rows:');
  sheetSummary.recentRows.forEach((row, idx) => {
    console.log(`    [${sheetSummary.totalRows - sheetSummary.recentRows.length + idx + 1}] ${JSON.stringify(row)}`);
  });
}

main().catch(err => {
  console.error('Error checking latest run:', err);
  process.exit(1);
});

