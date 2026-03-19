import type { Request } from 'express';

export function getSchedulerSuppliedToken(req: Pick<Request, 'headers'>): string | undefined {
  const authHeader = req.headers.authorization;
  const schedulerTokenHeader = req.headers['x-scheduler-token'];
  const bearerToken = typeof authHeader === 'string' && authHeader.startsWith('Bearer ')
    ? authHeader.slice(7)
    : undefined;
  return bearerToken ?? (Array.isArray(schedulerTokenHeader) ? schedulerTokenHeader[0] : schedulerTokenHeader);
}

export function isSchedulerRequestAuthorized(
  req: Pick<Request, 'headers'>,
  configuredToken: string | undefined,
): boolean {
  if (!configuredToken) return false;
  return getSchedulerSuppliedToken(req) === configuredToken;
}
