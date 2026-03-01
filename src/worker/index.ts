import { SQLiteQueueStore } from '../queue/sqlite';
import { pushToSheets } from '../sheets/push';
import { log } from '../utils/logger';
import { scrapeCASOSDetail } from './detail-scraper';
import dotenv from 'dotenv';

dotenv.config();

const BATCH_SIZE = 1;
const MAX_ATTEMPTS = 3;
const BACKOFF_MS = 300000;

async function processJob(job: any): Promise<any> {
  log({ stage: 'worker_process_start', job_id: job.id, file_number: job.filingNumber });
  
  const detail = await scrapeCASOSDetail(job.filingNumber);
  
  // Map to LienRecord format for Sheets
  const record = {
    state: 'CA',
    source: job.site,
    county: '',
    ucc_type: 'Federal Tax Lien',  // From your search
    debtor_name: detail.debtor_name,
    debtor_address: detail.debtor_address,
    file_number: detail.file_number,
    secured_party_name: detail.secured_party_name,
    secured_party_address: detail.secured_party_address,
    status: 'Active',
    filing_date: job.filingDate,
    lapse_date: '12/31/9999',
    document_type: 'Notice of Federal Tax Lien',
    pdf_filename: detail.pdf_path ? detail.pdf_path.split('/').pop() : '',
    processed: true,
    error: ''
  };
  
  return record;
}

async function runWorker() {
  const queue = new SQLiteQueueStore();
  
  log({ stage: 'worker_start' });
  
  while (true) {
    const jobs = await queue.claimBatch(BATCH_SIZE);
    
    if (jobs.length === 0) {
      log({ stage: 'worker_complete', message: 'Queue empty' });
      break;
    }
    
    const job = jobs[0];
    log({ stage: 'worker_claimed', job_id: job.id, file_number: job.filingNumber });
    
    try {
      const record = await processJob(job);
      
      await pushToSheets([record]);
      
      await queue.markDone([job.id]);
      log({ stage: 'worker_job_done', job_id: job.id });
    } catch (err: any) {
      log({ stage: 'worker_job_failed', job_id: job.id, error: err.message });
      
      if (job.attempts >= MAX_ATTEMPTS) {
        await queue.markDone([job.id]);
        log({ stage: 'worker_job_exhausted', job_id: job.id });
      } else {
        await queue.markFailed([job.id], BACKOFF_MS);
        log({ stage: 'worker_retry_later', job_id: job.id, attempt: job.attempts });
      }
    }
    
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  
  log({ stage: 'worker_shutdown' });
}

runWorker()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Worker fatal error:', err);
    process.exit(1);
  });
