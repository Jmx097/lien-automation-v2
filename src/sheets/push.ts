import { google } from "googleapis";

export async function pushToSheets(rows: any[]) {
  if (!process.env.SHEETS_KEY) {
    throw new Error("Missing SHEETS_KEY environment variable");
  }

  if (!process.env.SHEET_ID) {
    throw new Error("Missing SHEET_ID environment variable");
  }

  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.SHEETS_KEY),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  const sheets = google.sheets({ version: "v4", auth });

  const values = rows.map(r => [
    "CA",                         // state
    r.ucc_type,
    r.debtor_name,
    r.file_number,
    r.status,
    r.filing_date,
    r.lapse_date
  ]);

  await sheets.spreadsheets.values.append({
    spreadsheetId: process.env.SHEET_ID,
    range: "Sheet1!A1",
    valueInputOption: "USER_ENTERED",
    requestBody: { values }
  });

  return { uploaded: rows.length };
}