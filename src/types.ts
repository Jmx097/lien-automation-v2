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
  amount?: string;            // Total from PDF, whole dollars only (cents parsed then truncated)
  amount_confidence?: number; // Extraction confidence [0,1]
  amount_reason?: string;     // Extraction reason code
  confidence_score?: number;  // Overall record confidence [0,1]
  lead_type?: string;         // "Lien" or "Release" from PDF header
}

export interface ScrapeRunQualitySummary {
  requested_date_start: string;
  requested_date_end: string;
  discovered_count?: number;
  returned_count: number;
  quarantined_count: number;
  partial_run: boolean;
  partial_reason?: string;
  filtered_out_count?: number;
  skipped_existing_count?: number;
  search_results_seen?: number;
  details_fetched?: number;
  enriched_records?: number;
  partial_records?: number;
  returned_min_filing_date?: string;
  returned_max_filing_date?: string;
}

export type ScrapeResult = LienRecord[] & {
  quality_summary?: ScrapeRunQualitySummary;
};

export function attachScrapeQualitySummary(
  records: LienRecord[],
  qualitySummary: ScrapeRunQualitySummary,
): ScrapeResult {
  const result = records as ScrapeResult;
  result.quality_summary = qualitySummary;
  return result;
}
