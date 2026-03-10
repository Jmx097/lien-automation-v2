// src/gates/pre-run-health.ts
import { execSync } from 'child_process';

export interface PreRunHealthResult {
  success: boolean;
  errors: string[];
}

type FetchLike = typeof fetch;

export interface PreRunHealthDependencies {
  execSyncImpl?: typeof execSync;
  fetchImpl?: FetchLike;
  env?: NodeJS.ProcessEnv;
  canaryUrl?: string;
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
export async function preRunHealthCheck(dependencies: PreRunHealthDependencies = {}): Promise<PreRunHealthResult> {
  const errors: string[] = [];
  const execSyncImpl = dependencies.execSyncImpl ?? execSync;
  const fetchImpl = dependencies.fetchImpl ?? fetch;
  const env = dependencies.env ?? process.env;
  const canaryUrl = dependencies.canaryUrl ?? 'https://httpbin.org/get';

  try {
    // Check 1: Docker container is running and responsive
    await checkDockerContainer(errors, execSyncImpl);

    // Check 2: Required env vars are set
    checkEnvVars(errors, env);

    // Check 3: Canary request succeeds
    await checkCanaryRequest(errors, fetchImpl, canaryUrl);

    // Check 4: Playwright browsers installed
    checkPlaywrightBrowsers(errors, execSyncImpl);
  } catch (error) {
    errors.push(`Unexpected error during health check: ${error instanceof Error ? error.message : String(error)}`);
  }

  return {
    success: errors.length === 0,
    errors
  };
}

async function checkDockerContainer(errors: string[], execSyncImpl: typeof execSync): Promise<void> {
  try {
    // Check if docker is available
    execSyncImpl('docker --version', { stdio: 'ignore' });

    // Check if the lien-scraper container is running
    const containerStatus = execSyncImpl(
      'docker ps --filter "name=lien-scraper" --format "{{.Status}}"',
      { encoding: 'utf-8' }
    ).trim();

    if (!containerStatus) {
      // Try to start the container if it's not running
      try {
        execSyncImpl('docker start lien-scraper', { stdio: 'ignore' });
        console.log('Started lien-scraper container');
      } catch {
        // If we can't start it, try to run it
        try {
          execSyncImpl('docker run -d --name lien-scraper lien-scraper', { stdio: 'ignore' });
          console.log('Created and started new lien-scraper container');
        } catch {
          errors.push('Docker container is not running and could not be started');
        }
      }
    }
  } catch {
    errors.push('Docker is not available or not functioning properly');
  }
}

function checkEnvVars(errors: string[], env: NodeJS.ProcessEnv): void {
  const requiredEnvVars = [
    'BRIGHT_DATA_PROXY',
    'GOOGLE_SHEETS_CREDENTIALS',
    'DATABASE_URL'
  ];

  for (const envVar of requiredEnvVars) {
    if (!env[envVar]) {
      errors.push(`Required environment variable ${envVar} is not set`);
    }
  }
}

async function checkCanaryRequest(errors: string[], fetchImpl: FetchLike, canaryUrl: string): Promise<void> {
  try {
    // Simple canary request - check if we can access a known endpoint
    // This would typically be a lightweight request to verify connectivity
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000); // 5 second timeout
    
    const response = await fetchImpl(canaryUrl, {
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

function checkPlaywrightBrowsers(errors: string[], execSyncImpl: typeof execSync): void {
  try {
    execSyncImpl('npx playwright --version', { stdio: 'ignore' });
  } catch {
    errors.push('Playwright browsers are not installed or not functioning properly');
  }
}

// Export the function for use in other modules
export default preRunHealthCheck;
