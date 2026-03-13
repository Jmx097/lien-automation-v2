import dotenv from 'dotenv';
import { fetchLatestMaricopaSearchableDate, scrapeMaricopaRecorder } from '../src/scraper/maricopa_recorder';
import {
  filterValidMaricopaArtifactCandidates,
  isFreshMaricopaSession,
  loadMaricopaArtifactCandidates,
  loadMaricopaSessionState,
} from '../src/scraper/maricopa_artifacts';

dotenv.config({ quiet: true });

async function main(): Promise<void> {
  const positiveDateStart = process.env.MARICOPA_POSITIVE_DATE_START ?? '01/01/2026';
  const positiveDateEnd = process.env.MARICOPA_POSITIVE_DATE_END ?? '02/13/2026';
  const zeroDateStart = process.env.MARICOPA_ZERO_DATE_START ?? '01/01/2030';
  const zeroDateEnd = process.env.MARICOPA_ZERO_DATE_END ?? '01/02/2030';
  const maxRecords = Number(process.env.MARICOPA_VALIDATION_MAX_RECORDS ?? '2');

  const session = await loadMaricopaSessionState();
  const candidates = await loadMaricopaArtifactCandidates();
  const validCandidates = filterValidMaricopaArtifactCandidates(candidates);
  const latestSearchableDate = await fetchLatestMaricopaSearchableDate();
  const positiveRows = await scrapeMaricopaRecorder({
    date_start: positiveDateStart,
    date_end: positiveDateEnd,
    max_records: maxRecords,
  });
  const zeroRows = await scrapeMaricopaRecorder({
    date_start: zeroDateStart,
    date_end: zeroDateEnd,
    max_records: maxRecords,
  });

  const completeRows = positiveRows.filter((row) => !row.error);
  const incompleteRows = positiveRows.filter((row) => Boolean(row.error));

  console.log(
    JSON.stringify(
      {
        latest_searchable_date: latestSearchableDate,
        session: session
          ? {
            captured_at: session.captured_at,
            fresh: isFreshMaricopaSession(session.captured_at),
            storage_state_path: session.storage_state_path,
          }
          : null,
        discovery_candidates: {
          count: candidates.length,
          valid_count: validCandidates.length,
          first: candidates[0] ?? null,
          first_valid: validCandidates[0] ?? null,
        },
        positive_range: {
          date_start: positiveDateStart,
          date_end: positiveDateEnd,
          record_count: positiveRows.length,
          complete_count: completeRows.length,
          incomplete_count: incompleteRows.length,
          first_file_number: positiveRows[0]?.file_number ?? null,
          first_complete_record: completeRows[0] ?? null,
          first_incomplete_record: incompleteRows[0] ?? null,
        },
        zero_range: {
          date_start: zeroDateStart,
          date_end: zeroDateEnd,
          record_count: zeroRows.length,
        },
      },
      null,
      2,
    ),
  );
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exitCode = 1;
});
