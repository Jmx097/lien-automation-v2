// src/utils/retry-policy.ts

export interface RetryPolicyConfig {
  perRecordTimeout: number;        // Timeout per record in milliseconds
  maxRetries: number;              // Maximum number of retries
  baseDelay: number;               // Base delay in milliseconds
  maxDelay: number;                // Maximum delay in milliseconds
  backoffMultiplier: number;       // Multiplier for exponential backoff
  jitter: boolean;                 // Add random jitter to prevent thundering herd
}

export interface RetryResult<T> {
  success: boolean;
  result?: T;
  error?: Error;
  attemptCount: number;
}

/**
 * Default retry policy configuration
 */
export const DEFAULT_RETRY_POLICY: RetryPolicyConfig = {
  perRecordTimeout: 60_000,        // 60 seconds
  maxRetries: 5,
  baseDelay: 1_000,                // 1 second
  maxDelay: 30_000,                // 30 seconds
  backoffMultiplier: 2,
  jitter: true
};

/**
 * Calculate delay with exponential backoff and optional jitter
 */
export function calculateDelay(attempt: number, config: RetryPolicyConfig): number {
  let delay = config.baseDelay * Math.pow(config.backoffMultiplier, attempt);
  
  // Cap at max delay
  delay = Math.min(delay, config.maxDelay);
  
  // Add jitter if enabled
  if (config.jitter) {
    const jitter = Math.random() * 0.5; // Up to 50% jitter
    delay = delay * (1 + jitter);
  }
  
  return Math.floor(delay);
}

/**
 * Execute a function with retry logic based on the provided policy
 */
export async function executeWithRetry<T>(
  fn: () => Promise<T>,
  config: RetryPolicyConfig = DEFAULT_RETRY_POLICY
): Promise<RetryResult<T>> {
  let lastError: Error | undefined;
  
  for (let attempt = 0; attempt <= config.maxRetries; attempt++) {
    try {
      // Create a timeout promise
      const timeoutPromise = new Promise<T>((_, reject) => {
        setTimeout(() => reject(new Error('Operation timed out')), config.perRecordTimeout);
      });
      
      // Race the function execution with the timeout
      const result = await Promise.race([fn(), timeoutPromise]);
      
      return {
        success: true,
        result,
        attemptCount: attempt + 1
      };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      
      // If this was the last attempt, don't delay
      if (attempt === config.maxRetries) {
        break;
      }
      
      // Calculate delay before next attempt
      const delay = calculateDelay(attempt, config);
      
      // Wait before retrying
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  return {
    success: false,
    error: lastError,
    attemptCount: config.maxRetries + 1
  };
}

/**
 * Process a record with retry logic
 */
export async function processRecordWithRetry<T>(
  recordId: string,
  processor: (id: string) => Promise<T>,
  config: RetryPolicyConfig = DEFAULT_RETRY_POLICY
): Promise<RetryResult<T>> {
  console.log(`Processing record ${recordId} (attempt 1)`);
  
  return executeWithRetry(() => processor(recordId), config);
}

export default {
  DEFAULT_RETRY_POLICY,
  calculateDelay,
  executeWithRetry,
  processRecordWithRetry
};