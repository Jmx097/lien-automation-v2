import { timingSafeEqual } from 'node:crypto';
import type { Request } from 'express';

function safelyMatchesToken(supplied: string, configured: string): boolean {
  const suppliedBytes = Buffer.from(supplied);
  const configuredBytes = Buffer.from(configured);
  if (suppliedBytes.length !== configuredBytes.length) return false;
  return timingSafeEqual(suppliedBytes, configuredBytes);
}

export function getCrmBearerToken(req: Pick<Request, 'headers'>): string | undefined {
  const authorization = req.headers.authorization;
  if (typeof authorization !== 'string') return undefined;
  const match = /^Bearer ([^\s]+)$/.exec(authorization);
  return match?.[1];
}

/** CRM routes are deliberately fail-closed when no service token is configured. */
export function isCrmRequestAuthorized(
  req: Pick<Request, 'headers'>,
  configuredToken: string | undefined,
): boolean {
  const suppliedToken = getCrmBearerToken(req);
  return Boolean(configuredToken && suppliedToken && safelyMatchesToken(suppliedToken, configuredToken));
}
