import { google } from "googleapis";
import { LienRecord } from "../types";

export async function pushToSheets(rows: LienRecord[]): Promise<{ uploaded: number }> {
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
    r.state,                   // CA
    r.ucc_type,                // Federal Tax Lien
    r.debtor_name,             // Full name or business
    r.debtor_address,          // Street, City, State ZIP
    r.file_number,             // e.g. U260005937931
    r.secured_party_name,      // INTERNAL REVENUE SERVICE
    r.secured_party_address,   // PO BOX 145595...
    r.status,                  // Active / Terminated
    r.filing_date,             // MM/DD/YYYY
    r.lapse_date,              // MM/DD/YYYY or 12/31/9999
    r.document_type,           // Lien Financing Stmt
    r.pdf_filename,            // U260005937931_01202026.pdf or ""
    r.processed ? "true" : "false",
    r.error ?? ""
  ]);

  await sheets.spreadsheets.values.append({
    spreadsheetId: process.env.SHEET_ID,
    range: "Sheet1!A1",
    valueInputOption: "USER_ENTERED",
    requestBody: { values }
  });

  return { uploaded: rows.length };
}
