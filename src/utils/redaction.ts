export function redactSecret(value?: string | null): string {
  if (!value) return '';
  if (value.length <= 8) return '[REDACTED]';
  return `${value.slice(0, 4)}***${value.slice(-4)}`;
}

export function sanitizeErrorMessage(err: unknown): string {
  const text = err instanceof Error ? err.message : String(err);
  return text
    .replace(/(__?RequestVerificationToken=)[^&\s]+/gi, '$1[REDACTED]')
    .replace(/(authorization:\s*bearer\s+)[^\s]+/gi, '$1[REDACTED]')
    .replace(/(cookie:\s*)[^\r\n]+/gi, '$1[REDACTED]')
    .replace(/(https?:\/\/[^:\s]+:)[^@/\s]+@/gi, '$1[REDACTED]@');
}
