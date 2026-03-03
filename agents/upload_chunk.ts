// MCP: upload_chunk
// Inputs: { records: [], chunk_info: {}, checksum: string }
// Uploads validated data to Google Sheets
import { execSync } from 'child_process';

export async function upload_chunk(inputs: any) {
  try {
    // Validate inputs
    if (!inputs.records || !Array.isArray(inputs.records)) {
      throw new Error('Invalid records input - must be an array');
    }

    if (!inputs.chunk_info) {
      throw new Error('Missing chunk_info input');
    }

    if (!inputs.checksum) {
      throw new Error('Missing checksum input');
    }

    // Prepare the upload command
    // This assumes you have an upload script in your project
    const cmd = `node dist/utils/upload-to-sheets.js`;
    
    // Create a temporary file with the data to upload
    const tempFilePath = `/tmp/chunk_${inputs.chunk_info.chunk_id}_data.json`;
    execSync(`echo '${JSON.stringify(inputs)}' > ${tempFilePath}`, { encoding: 'utf8' });
    
    console.log(`Uploading chunk ${inputs.chunk_info.chunk_id} with ${inputs.records.length} records`);
    
    // Execute the upload command with the data file as input
    const result = execSync(`${cmd} ${tempFilePath}`, {
      encoding: 'utf8',
      cwd: '/app', // Assuming this will run in the docker container
      timeout: 300000 // 5 minute timeout
    });
    
    // Clean up temporary file
    execSync(`rm ${tempFilePath}`, { encoding: 'utf8' });
    
    // Parse the result
    let uploadResult;
    try {
      uploadResult = JSON.parse(result);
    } catch (parseError) {
      uploadResult = { success: true, message: result.trim() };
    }
    
    return { 
      upload_result: uploadResult,
      chunk_info: inputs.chunk_info,
      record_count: inputs.records.length,
      checksum: inputs.checksum
    };
  } catch (error) {
    console.error('Error in upload_chunk:', error);
    throw error;
  }
}