// MCP: chunk_scrape
// Inputs: {site: 'ucc-ca'|'nyacris'|'cook', date_start: '2026-02-01', date_end: '2026-03-01', chunk_id: 1, max_records: 10}
// Uses your existing src/scrapers/{site}.ts via exec
// Outputs BrightData session JSON + records array
import { execSync } from 'child_process';

export async function chunk_scrape(inputs: any) {
  try {
    // Validate inputs
    if (!inputs.site || !inputs.date_start || !inputs.date_end || !inputs.chunk_id) {
      throw new Error('Missing required inputs: site, date_start, date_end, chunk_id');
    }

    // Execute the scraper command
    const cmd = `node dist/scraper/${inputs.site}.js --start ${inputs.date_start} --end ${inputs.date_end} --chunk ${inputs.chunk_id} --max ${inputs.max_records || 10}`;
    
    console.log(`Executing command: ${cmd}`);
    
    const result = execSync(cmd, {
      encoding: 'utf8',
      cwd: '/app', // Assuming this will run in the docker container
      timeout: 300000 // 5 minute timeout
    });
    
    // Parse the result
    let records;
    try {
      records = JSON.parse(result);
    } catch (parseError) {
      console.error('Failed to parse scraper output:', result);
      throw new Error('Scraper returned invalid JSON');
    }
    
    return { 
      records: records, 
      proxy_session: process.env.BRIGHTDATA_SESSION,
      chunk_info: {
        site: inputs.site,
        date_start: inputs.date_start,
        date_end: inputs.date_end,
        chunk_id: inputs.chunk_id,
        max_records: inputs.max_records || 10
      }
    };
  } catch (error) {
    console.error('Error in chunk_scrape:', error);
    throw error;
  }
}