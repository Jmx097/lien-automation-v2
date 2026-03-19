import { describe, expect, it } from 'vitest';
import { getSchedulerSuppliedToken, isSchedulerRequestAuthorized } from '../../src/http/scheduler-auth';

describe('scheduler auth helper', () => {
  it('accepts a bearer token from the authorization header', () => {
    const req = {
      headers: {
        authorization: 'Bearer secret-token',
      },
    } as any;

    expect(getSchedulerSuppliedToken(req)).toBe('secret-token');
    expect(isSchedulerRequestAuthorized(req, 'secret-token')).toBe(true);
  });

  it('falls back to x-scheduler-token and rejects mismatches', () => {
    const req = {
      headers: {
        'x-scheduler-token': 'other-token',
      },
    } as any;

    expect(getSchedulerSuppliedToken(req)).toBe('other-token');
    expect(isSchedulerRequestAuthorized(req, 'secret-token')).toBe(false);
  });
});
