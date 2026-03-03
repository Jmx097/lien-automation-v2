import path from 'path';
import fs from 'fs';
import { log } from '../src/utils/logger';
import { LienRecord } from '../src/types';
import { scrapeCASOS_Enhanced } from '../src/scraper/ca_sos_enhanced';

async function main() {
  const [, , pdfFileName] = process.argv;
  const downloadsDir = path.join(process.cwd(), 'data/downloads');

  if (!pdfFileName) {
    console.error('Usage: npm run test:ca-sos-pdf-amount -- <fileName.pdf>');
    console.error(`Place the PDF under ${downloadsDir} and pass just the file name.`);
    process.exit(1);
  }

  const pdfPath = path.join(downloadsDir, pdfFileName);
  if (!fs.existsSync(pdfPath)) {
    console.error(`PDF not found at ${pdfPath}`);
    process.exit(1);
  }

  log({ stage: 'test_pdf_amount_start', pdf: pdfPath });

  // Run a tiny scrape window with max_records = 1 so that extractFromPDF is exercised.
  const records: LienRecord[] = await scrapeCASOS_Enhanced({
    date_start: '01/05/2026',
    date_end: '01/05/2026',
    max_records: 1,
  });

  for (const r of records) {
    console.log(JSON.stringify({
      file_number: r.file_number,
      amount: r.amount,
      lead_type: r.lead_type,
    }, null, 2));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

