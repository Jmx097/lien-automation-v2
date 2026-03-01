import { google } from "googleapis";
import { LienRecord } from "../types";

const SITE_ID_CA_SOS = 11;          // from your Lien Sites sheet for CA SOS
const LEAD_SOURCE = "777";
const LIABILITY_TYPE = "IRS";

type BusinessFlag = "Business" | "Personal";

function classifyBusinessPersonal(name: string): BusinessFlag {
  const upper = name.toUpperCase();
  const businessKeywords = [
    " INC", " LLC", " LLP", " LP", " LTD", " CO ", " CO.", " COMPANY",
    " CORPORATION", " CORP", " PLLC", " PC", " GROUP", " HOLDINGS"
  ];
  if (businessKeywords.some(k => upper.includes(k.trim()))) return "Business";
  return "Personal";
}

function splitPersonalName(name: string): { firstName: string; lastName: string } {
  // Handle "David and Jessica Donovan" or "David & Jessica Donovan"
  const andSplit = name.split(/\band\b|&/i);
  const segment = andSplit[andSplit.length - 1].trim();

  // Remove commas and common suffixes
  const cleaned = segment.replace(/[,]/g, "")
    .replace(/\b(JR|SR|II|III|IV|V)\b\.?/gi, "")
    .trim();

  const parts = cleaned.split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { firstName: "", lastName: "" };
  if (parts.length === 1) return { firstName: parts[0], lastName: "" };
  return {
    firstName: parts[0],
    lastName: parts[parts.length - 1],
  };
}

function parseAddress(raw: string, stateFallback: string): {
  street: string;
  city: string;
  state: string;
  zip: string;
} {
  let street = raw.trim();
  let city = "";
  let state = stateFallback;
  let zip = "";

  // Example expected formats:
  // "8220 E Indianola Ave, Scottsdale, AZ 85251"
  // "8220 E Indianola Ave Scottsdale AZ 85251"
  const zipMatch = raw.match(/(\d{5})(?:-\d{4})?$/);
  if (zipMatch) {
    zip = zipMatch[1]; // first 5 only
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
    street = raw.split(",")[0].trim();
  }

  return { street, city, state, zip };
}

export async function pushToSheets(rows: LienRecord[]): Promise<{ uploaded: number }> {
  if (!process.env.SHEETS_KEY) {
    throw new Error("Missing SHEETS_KEY environment variable");
  }
  if (!process.env.SHEET_ID) {
    throw new Error("Missing SHEET_ID environment variable");
  }

  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.SHEETS_KEY),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const sheets = google.sheets({ version: "v4", auth });

  const values = rows.map((r) => {
    const businessPersonal = classifyBusinessPersonal(r.debtor_name);
    const nameParts =
      businessPersonal === "Personal"
        ? splitPersonalName(r.debtor_name)
        : { firstName: "", lastName: "" };

    const addrParts = parseAddress(r.debtor_address, r.state);

    return [
      SITE_ID_CA_SOS,          // Site Id
      r.filing_date,           // LienOrReceiveDate (results-table / recorder date)
      "",                      // Amount (from PDF Total/Unpaid Balance - phase 2)
      "Lien",                  // LeadType (this scraper is liens only)
      LEAD_SOURCE,             // LeadSource (always 777)
      LIABILITY_TYPE,          // LiabilityType (IRS for this site)
      businessPersonal,        // BusinessPersonal
      businessPersonal === "Business" ? r.debtor_name : "", // Company
      nameParts.firstName,     // FirstName
      nameParts.lastName,      // LastName
      addrParts.street,        // Street
      addrParts.city,          // City
      addrParts.state,         // State
      addrParts.zip,           // Zip (first 5)
    ];
  });

  await sheets.spreadsheets.values.append({
    spreadsheetId: process.env.SHEET_ID,
    range: "Records!A2",
    valueInputOption: "USER_ENTERED",
    requestBody: { values },
  });

  return { uploaded: rows.length };
}
