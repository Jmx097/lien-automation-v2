export interface LienRecord {
  state: string;        // e.g. "CA" | "NY" | "IL"
  source: string;       // e.g. "ca_sos" | "nyc_acris" | "cook_county"
  county?: string;      // county-level sites e.g. "Cook"
  ucc_type: string;
  debtor_name: string;
  debtor_address: string;
  file_number: string;
  secured_party_name: string;
  secured_party_address: string;
  status: string;
  filing_date: string;        // MM/DD/YYYY
  lapse_date: string;         // MM/DD/YYYY or "12/31/9999"
  document_type: string;      // e.g. "Lien Financing Stmt"
  pdf_filename: string;       // e.g. "U260005937931_01202026.pdf" or ""
  processed: boolean;
  error?: string;             // "panel_failed" | "history_failed" | "no_download_available"
}
