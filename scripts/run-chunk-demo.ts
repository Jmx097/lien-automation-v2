#!/usr/bin/env node
// scripts/run-chunk-demo.ts
import { processChunk } from '../src/scraper/chunk-processor';

async function main() {
  const args = process.argv.slice(2);
  
  // Parse command line arguments
  const chunkId = args[0] || `chunk-${Date.now()}`;
  const startDate = args[1] || '01/01/2024';
  const endDate = args[2] || '01/31/2024';
  const maxRecords = parseInt(args[3]) || 10;
  
  console.log('Running chunk processor demo with parameters:');
  console.log(`  Chunk ID: ${chunkId}`);
  console.log(`  Date Range: ${startDate} to ${endDate}`);
  console.log(`  Max Records: ${maxRecords}`);
  
  try {
    const result = await processChunk({
      chunkId,
      startDate,
      endDate,
      maxRecords
    });
    
    console.log('\nChunk Processing Result:');
    console.log(`  Success: ${result.success}`);
    console.log(`  Processed Count: ${result.processedCount}`);
    console.log(`  Failed Count: ${result.failedCount}`);
    console.log(`  Errors: ${result.errors.length > 0 ? result.errors.join(', ') : 'None'}`);
    
    if (result.records.length > 0) {
      console.log(`\nProcessed Records (${result.records.length}):`);
      result.records.forEach((record, index) => {
        console.log(`  ${index + 1}. ${record.file_number} - ${record.debtor_name}`);
      });
    }
  } catch (error) {
    console.error('Fatal error running chunk processor:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch(console.error);
}