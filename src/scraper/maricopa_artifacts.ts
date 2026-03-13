import fs from 'fs/promises';
import path from 'path';
import type { BrowserContextOptions } from 'playwright';
import { createIsolatedBrowserContext, type BrowserTransportMode } from '../browser/transport';

export interface MaricopaSessionState {
  version: 1;
  captured_at: string;
  transport_mode: BrowserTransportMode;
  source_url: string;
  cookie_summary: string[];
  storage_state_path: string;
}

export interface MaricopaCapturedRequest {
  method: string;
  url: string;
  status?: number;
  resourceType?: string;
}

export interface MaricopaArtifactCandidate {
  urlTemplate: string;
  sampleUrl: string;
  kind: 'pdf' | 'image' | 'document' | 'unknown';
}

export interface MaricopaArtifactFetchResult {
  buffer: Buffer;
  contentType?: string;
  url: string;
}

const RESULTS_URL =
  'https://recorder.maricopa.gov/recording/document-search-results.html?lastNames=&firstNames=&middleNameIs=&documentTypeSelector=code&documentCode=FL&beginDate=2026-01-01&endDate=2026-02-13';

function maricopaRootDir(): string {
  return process.env.MARICOPA_OUT_DIR?.trim() || path.resolve(process.cwd(), 'out', 'maricopa');
}

export function getMaricopaSessionDir(): string {
  return path.join(maricopaRootDir(), 'session');
}

export function getMaricopaSessionMetaPath(): string {
  return path.join(getMaricopaSessionDir(), 'session-meta.json');
}

export function getMaricopaStorageStatePath(): string {
  return path.join(getMaricopaSessionDir(), 'storage-state.json');
}

export function getMaricopaDiscoveryPath(): string {
  return path.join(maricopaRootDir(), 'discovery-candidates.json');
}

export function getMaricopaArtifactDir(): string {
  return path.join(maricopaRootDir(), 'artifacts');
}

async function ensureDir(dir: string): Promise<void> {
  await fs.mkdir(dir, { recursive: true });
}

export function isFreshMaricopaSession(capturedAt: string): boolean {
  const maxAgeMinutes = Math.max(1, Number(process.env.MARICOPA_SESSION_MAX_AGE_MINUTES ?? '240'));
  const ageMs = Date.now() - new Date(capturedAt).getTime();
  return Number.isFinite(ageMs) && ageMs >= 0 && ageMs <= maxAgeMinutes * 60 * 1000;
}

export async function loadMaricopaSessionState(): Promise<MaricopaSessionState | null> {
  try {
    const raw = await fs.readFile(getMaricopaSessionMetaPath(), 'utf8');
    const parsed = JSON.parse(raw) as MaricopaSessionState;
    if (parsed.version !== 1) return null;
    await fs.access(parsed.storage_state_path);
    return parsed;
  } catch {
    return null;
  }
}

export async function saveMaricopaSessionState(
  storageState: BrowserContextOptions['storageState'],
  transportMode: BrowserTransportMode,
  sourceUrl = RESULTS_URL,
): Promise<MaricopaSessionState> {
  await ensureDir(getMaricopaSessionDir());
  const storageStatePath = getMaricopaStorageStatePath();
  await fs.writeFile(storageStatePath, JSON.stringify(storageState, null, 2), 'utf8');

  const cookies = (typeof storageState === 'object' && storageState && 'cookies' in storageState && Array.isArray(storageState.cookies))
    ? storageState.cookies
    : [];
  const meta: MaricopaSessionState = {
    version: 1,
    captured_at: new Date().toISOString(),
    transport_mode: transportMode,
    source_url: sourceUrl,
    cookie_summary: cookies
      .map((cookie) => `${cookie.domain}:${cookie.name}`)
      .filter(Boolean)
      .slice(0, 20),
    storage_state_path: storageStatePath,
  };

  await fs.writeFile(getMaricopaSessionMetaPath(), JSON.stringify(meta, null, 2), 'utf8');
  return meta;
}

export function buildMaricopaArtifactPath(recordingNumber: string, ext: string): string {
  const safeExt = ext.startsWith('.') ? ext : `.${ext}`;
  return path.join(getMaricopaArtifactDir(), `${recordingNumber}${safeExt}`);
}

function inferArtifactKind(url: string): MaricopaArtifactCandidate['kind'] {
  if (/\.pdf(?:$|[?#])/i.test(url)) return 'pdf';
  if (/\.(?:png|jpe?g|webp)(?:$|[?#])/i.test(url)) return 'image';
  if (/pdf|image|preview|viewer|document/i.test(url)) return 'document';
  return 'unknown';
}

export function discoverMaricopaArtifactCandidates(
  requests: MaricopaCapturedRequest[],
  recordingNumber?: string,
): MaricopaArtifactCandidate[] {
  const seen = new Set<string>();
  const candidates: MaricopaArtifactCandidate[] = [];

  for (const request of requests) {
    const url = request.url;
    if (!/maricopa\.gov/i.test(url)) continue;
    if (/documents\/search|documents\/index$/i.test(url)) continue;
    if (recordingNumber && !url.includes(recordingNumber)) continue;
    if (!/pdf|image|preview|viewer|document|download|page/i.test(url) && !/\.(?:pdf|png|jpe?g|webp)(?:$|[?#])/i.test(url)) {
      continue;
    }

    const urlTemplate = recordingNumber ? url.split(recordingNumber).join('{recordingNumber}') : url;
    if (seen.has(urlTemplate)) continue;
    seen.add(urlTemplate);
    candidates.push({
      urlTemplate,
      sampleUrl: url,
      kind: inferArtifactKind(url),
    });
  }

  return candidates;
}

export async function saveMaricopaArtifactCandidates(candidates: MaricopaArtifactCandidate[]): Promise<void> {
  await ensureDir(maricopaRootDir());
  await fs.writeFile(getMaricopaDiscoveryPath(), JSON.stringify(candidates, null, 2), 'utf8');
}

export async function loadMaricopaArtifactCandidates(): Promise<MaricopaArtifactCandidate[]> {
  try {
    const raw = await fs.readFile(getMaricopaDiscoveryPath(), 'utf8');
    const parsed = JSON.parse(raw) as MaricopaArtifactCandidate[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export async function resolveMaricopaArtifactUrl(recordingNumber: string): Promise<string | null> {
  const envTemplate = process.env.MARICOPA_ARTIFACT_URL_TEMPLATE?.trim();
  if (envTemplate) {
    return envTemplate.split('{recordingNumber}').join(recordingNumber);
  }

  const candidates = await loadMaricopaArtifactCandidates();
  const preferred = candidates.find((candidate) => candidate.kind === 'pdf')
    ?? candidates.find((candidate) => candidate.kind === 'image')
    ?? candidates[0];

  if (!preferred) return null;
  return preferred.urlTemplate.split('{recordingNumber}').join(recordingNumber);
}

export async function fetchMaricopaArtifactWithSession(url: string): Promise<MaricopaArtifactFetchResult | null> {
  const session = await loadMaricopaSessionState();
  const handle = await createIsolatedBrowserContext({
    contextOptions: session?.storage_state_path ? { storageState: session.storage_state_path } : undefined,
  });

  try {
    const page = await handle.context.newPage();
    await page.goto(RESULTS_URL, { waitUntil: 'domcontentloaded' }).catch(() => null);
    const payload = await page.evaluate(async (artifactUrl: string) => {
      try {
        const response = await fetch(artifactUrl, { credentials: 'include' });
        if (!response.ok) {
          return { ok: false, status: response.status, contentType: response.headers.get('content-type') ?? undefined, body: '' };
        }
        const contentType = response.headers.get('content-type') ?? undefined;
        const bytes = new Uint8Array(await response.arrayBuffer());
        let binary = '';
        for (let index = 0; index < bytes.length; index += 1) {
          binary += String.fromCharCode(bytes[index]);
        }
        return { ok: true, contentType, body: btoa(binary) };
      } catch (error) {
        return { ok: false, error: String(error) };
      }
    }, url);

    if (!payload.ok || !payload.body) return null;
    return {
      buffer: Buffer.from(payload.body, 'base64'),
      contentType: payload.contentType,
      url,
    };
  } finally {
    await handle.close();
  }
}
