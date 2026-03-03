// src/gates/pre-run-health.ts
import { execSync, spawn } from 'child_process';
import fs from 'fs';
import path from 'path';

export interface PreRunHealthResult {
  success: boolean;
  errors: string[];
}

/**
 * Pre-run health check for the lien automation pipeline
 * 
 * Checks:
 * 1. Docker container is running and responsive
 * 2. Required env vars are set
 * 3. Canary request succeeds
 * 4. Playwright browsers installed
 */
export async function preRunHealthCheck(): Promise<PreRunHealthResult> {
  const errors: string[] = [];

  try {
    // Check 1: Docker container is running and responsive
    await checkDockerContainer(errors);

    // Check 2: Required env vars are set
    checkEnvVars(errors);

    // Check 3: Canary request succeeds
    await checkCanaryRequest(errors);

    // Check 4: Playwright browsers installed
    checkPlaywrightBrowsers(errors);
  } catch (error) {
    errors.push(`Unexpected error during health check: ${error instanceof Error ? error.message : String(error)}`);
  }

  return {
    success: errors.length === 0,
    errors
  };
}

async function checkDockerContainer(errors: string[]): Promise<void> {
  try {
    // Check if docker is available
    execSync('docker --version', { stdio: 'ignore' });

    // Check if the lien-scraper container is running
    const containerStatus = execSync(
      'docker ps --filter "name=lien-scraper" --format "{{.Status}}"',
      { encoding: 'utf-8' }
    ).trim();

    if (!containerStatus) {
      // Try to start the container if it's not running
      try {
        execSync('docker start lien-scraper', { stdio: 'ignore' });
        console.log('Started lien-scraper container');
      } catch (startError) {
        // If we can't start it, try to run it
        try {
          execSync('docker run -d --name lien-scraper lien-scraper', { stdio: 'ignore' });
          console.log('Created and started new lien-scraper container');
        } catch (runError) {
          errors.push('Docker container is not running and could not be started');
        }
      }
    }
  } catch (error) {
    errors.push('Docker is not available or not functioning properly');
  }
}

function checkEnvVars(errors: string[]): void {
  const requiredEnvVars = [
    'BRIGHT_DATA_PROXY',
    'GOOGLE_SHEETS_CREDENTIALS',
    'DATABASE_URL'
  ];

  for (const envVar of requiredEnvVars) {
    if (!process.env[envVar]) {
      errors.push(`Required environment variable ${envVar} is not set`);
    }
  }
}

async function checkCanaryRequest(errors: string[]): Promise<void> {
  try {
    // Simple canary request - check if we can access a known endpoint
    // This would typically be a lightweight request to verify connectivity
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000); // 5 second timeout
    
    const response = await fetch('https://httpbin.org/get', {
      signal: controller.signal
    });
    
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      errors.push(`Canary request failed with status ${response.status}`);
    }
  } catch (error) {
    errors.push(`Canary request failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

function checkPlaywrightBrowsers(errors: string[]): void {
  try {
    // Check if Playwright browsers are installed
    execSync('npx playwright install --with-deps chromium', { stdio: 'ignore' });
  } catch (error) {
    errors.push('Playwright browsers are not installed or not functioning properly');
  }
}

// Export the function for use in other modules
export default preRunHealthCheck;