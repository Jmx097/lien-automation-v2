import { describe, expect, it } from 'vitest';
import { redactSecret, sanitizeErrorMessage } from '../../src/utils/redaction';

describe('redaction helpers', () => {
  it('redacts long secret values', () => {
    expect(redactSecret('abcdefghijklmno')).toBe('abcd***lmno');
  });

  it('sanitizes tokens, cookies, and embedded credentials', () => {
    const input = 'cookie: abc\r\n__RequestVerificationToken=secret123 https://user:pass@example.com/path authorization: Bearer token123';
    const sanitized = sanitizeErrorMessage(input);

    expect(sanitized).not.toContain('secret123');
    expect(sanitized).not.toContain('token123');
    expect(sanitized).not.toContain('pass@');
    expect(sanitized).toContain('[REDACTED]');
  });
});

