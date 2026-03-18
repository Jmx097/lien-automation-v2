import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import type { BrowserContextOptions } from 'playwright';
import { createIsolatedBrowserContext, type BrowserTransportMode } from '../browser/transport';
import { ScheduledRunStore } from '../scheduler/store';

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

export interface MaricopaArtifactCandidateValidation {
  valid: boolean;
  reason?: string;
}

export interface MaricopaArtifactFetchResult {
  buffer: Buffer;
  contentType?: string;
  url: string;
}

export interface MaricopaPersistedStateReadiness {
  artifactRetrievalEnabled: boolean;
  sessionPresent: boolean;
  sessionFresh: boolean;
  sessionCapturedAt?: string;
  artifactCandidatesPresent: boolean;
  artifactCandidateCount: number;
  refreshRequired: boolean;
  refreshReason?: 'session_missing_or_stale' | 'artifact_candidates_missing' | 'artifact_retrieval_disabled';
  detail: string;
}

const RESULTS_URL =
  'https://recorder.maricopa.gov/recording/document-search-results.html?lastNames=&firstNames=&middleNameIs=&documentTypeSelector=code&documentCode=FL&beginDate=2026-01-01&endDate=2026-02-13';
const MARICOPA_SITE = 'maricopa_recorder';
const SESSION_ARTIFACT_KEY = 'session_state';
const DISCOVERY_ARTIFACT_KEY = 'artifact_candidates';

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

function getRuntimeStorageStatePath(): string {
  const configured = process.env.MARICOPA_OUT_DIR?.trim();
  if (configured) return path.join(configured, 'session', 'storage-state.json');
  return path.join(os.tmpdir(), 'maricopa', 'session', 'storage-state.json');
}

async function withStore<T>(fn: (store: ScheduledRunStore) => Promise<T>): Promise<T> {
  const store = new ScheduledRunStore();
  try {
    return await fn(store);
  } finally {
    await store.close().catch(() => null);
  }
}

async function saveArtifactPayload(key: string, payload: unknown): Promise<void> {
  await withStore((store) => store.upsertSiteStateArtifact({
    site: MARICOPA_SITE,
    artifact_key: key,
    payload_json: JSON.stringify(payload),
    updated_at: new Date().toISOString(),
  }));
}

async function loadArtifactPayload<T>(key: string): Promise<T | null> {
  try {
    const record = await withStore((store) => store.getSiteStateArtifact(MARICOPA_SITE, key));
    if (!record?.payload_json) return null;
    return JSON.parse(record.payload_json) as T;
  } catch {
    return null;
  }
}

interface PersistedMaricopaSessionPayload {
  meta: Omit<MaricopaSessionState, 'storage_state_path'>;
  storage_state: BrowserContextOptions['storageState'];
}

const FORBIDDEN_MARICOPA_COOKIE_NAMES = new Set([
  'csrf',
  'arraffinity',
  'arraffinitysamesite',
  '_ga_kdhkt2nc21',
  '_ga',
  'cf_clearance',
  '__cf_bm',
]);

function sanitizeMaricopaStorageState(
  storageState: BrowserContextOptions['storageState'],
): BrowserContextOptions['storageState'] {
  if (!storageState || typeof storageState !== 'object') return storageState;

  const cookies = Array.isArray(storageState.cookies)
    ? storageState.cookies.filter((cookie) => !FORBIDDEN_MARICOPA_COOKIE_NAMES.has(cookie.name.toLowerCase()))
    : storageState.cookies;

  return {
    ...storageState,
    cookies,
  };
}

function hasPdfSignature(buffer: Buffer): boolean {
  const prefix = buffer.subarray(0, 16).toString('latin1').trimStart();
  return prefix.startsWith('%PDF-');
}

function hasPngSignature(buffer: Buffer): boolean {
  return buffer.length >= 8 && buffer.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]));
}

function hasJpegSignature(buffer: Buffer): boolean {
  return buffer.length >= 3 && buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff;
}

export function isUsableMaricopaArtifactPayload(
  buffer: Buffer,
  contentType?: string,
  url = '',
): boolean {
  if (!buffer || buffer.length < 32) return false;

  const normalizedContentType = contentType?.toLowerCase() ?? '';
  const normalizedUrl = url.toLowerCase();
  const looksLikePdfUrl = /\.pdf(?:$|[?#])/i.test(normalizedUrl) || /\/preview\/pdf\?/i.test(normalizedUrl);
  const looksLikeImageUrl = /\.(?:png|jpe?g)(?:$|[?#])/i.test(normalizedUrl) || /\/document-preview\.html\?/i.test(normalizedUrl);

  if (normalizedContentType.includes('text/html')) return false;

  if (normalizedContentType.includes('pdf') || looksLikePdfUrl) {
    return hasPdfSignature(buffer);
  }

  if (normalizedContentType.includes('png') || normalizedUrl.endsWith('.png') || normalizedUrl.includes('image')) {
    return hasPngSignature(buffer);
  }

  if (normalizedContentType.includes('jpeg') || normalizedContentType.includes('jpg') || normalizedUrl.endsWith('.jpg') || normalizedUrl.endsWith('.jpeg')) {
    return hasJpegSignature(buffer);
  }

  if (looksLikeImageUrl) {
    return hasPngSignature(buffer) || hasJpegSignature(buffer);
  }

  return hasPdfSignature(buffer) || hasPngSignature(buffer) || hasJpegSignature(buffer);
}

export function isFreshMaricopaSession(capturedAt: string): boolean {
  const maxAgeMinutes = Math.max(1, Number(process.env.MARICOPA_SESSION_MAX_AGE_MINUTES ?? '240'));
  const ageMs = Date.now() - new Date(capturedAt).getTime();
  return Number.isFinite(ageMs) && ageMs >= 0 && ageMs <= maxAgeMinutes * 60 * 1000;
}

export function isMaricopaArtifactRetrievalEnabled(): boolean {
  return process.env.MARICOPA_ENABLE_ARTIFACT_RETRIEVAL !== '0';
}

export async function loadMaricopaSessionState(): Promise<MaricopaSessionState | null> {
  const dbPayload = await loadArtifactPayload<PersistedMaricopaSessionPayload>(SESSION_ARTIFACT_KEY);
  if (dbPayload?.meta?.version === 1 && dbPayload.storage_state) {
    const storageStatePath = getRuntimeStorageStatePath();
    await ensureDir(path.dirname(storageStatePath));
    await fs.writeFile(
      storageStatePath,
      JSON.stringify(sanitizeMaricopaStorageState(dbPayload.storage_state), null, 2),
      'utf8',
    );
    return {
      ...dbPayload.meta,
      storage_state_path: storageStatePath,
    };
  }

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
  const sanitizedStorageState = sanitizeMaricopaStorageState(storageState);
  await fs.writeFile(storageStatePath, JSON.stringify(sanitizedStorageState, null, 2), 'utf8');

  const cookies = (typeof sanitizedStorageState === 'object'
    && sanitizedStorageState
    && 'cookies' in sanitizedStorageState
    && Array.isArray(sanitizedStorageState.cookies))
    ? sanitizedStorageState.cookies
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
  await saveArtifactPayload(SESSION_ARTIFACT_KEY, {
    meta: {
      version: meta.version,
      captured_at: meta.captured_at,
      transport_mode: meta.transport_mode,
      source_url: meta.source_url,
      cookie_summary: meta.cookie_summary,
    },
    storage_state: sanitizedStorageState,
  } satisfies PersistedMaricopaSessionPayload);
  return meta;
}

export function buildMaricopaArtifactPath(recordingNumber: string, ext: string): string {
  const safeExt = ext.startsWith('.') ? ext : `.${ext}`;
  return path.join(getMaricopaArtifactDir(), `${recordingNumber}${safeExt}`);
}

function inferArtifactKind(url: string): MaricopaArtifactCandidate['kind'] {
  if (/\.pdf(?:$|[?#])/i.test(url)) return 'pdf';
  if (/\.(?:png|jpe?g|webp)(?:$|[?#])/i.test(url)) return 'image';
  if (/\/preview\/pdf\?/i.test(url)) return 'pdf';
  if (/\/document-preview\.html\?/i.test(url)) return 'image';
  if (/pdf|image|preview|viewer|document/i.test(url)) return 'document';
  return 'unknown';
}

export function validateMaricopaArtifactCandidate(
  candidate: MaricopaArtifactCandidate,
  recordingNumber?: string,
): MaricopaArtifactCandidateValidation {
  const url = candidate.sampleUrl || candidate.urlTemplate;

  if (!/^https:\/\/(?:publicapi\.recorder\.maricopa\.gov|recorder\.maricopa\.gov)/i.test(url)) {
    return { valid: false, reason: 'non_maricopa_host' };
  }
  if (/analytics\.google|google-analytics|privacy-sandbox/i.test(url)) {
    return { valid: false, reason: 'analytics_url' };
  }
  if (/cdn-cgi|challenge-platform/i.test(url)) {
    return { valid: false, reason: 'challenge_asset' };
  }
  if (/\/asset\/jcr:|logo-|seal\.png|iStock/i.test(url)) {
    return { valid: false, reason: 'static_asset' };
  }
  if (/document-search-results\.html|\/documents\/search|\/documents\/index/i.test(url)) {
    return { valid: false, reason: 'search_or_results_page' };
  }
  if (recordingNumber && !url.includes(recordingNumber) && !candidate.urlTemplate.includes('{recordingNumber}')) {
    return { valid: false, reason: 'recording_number_mismatch' };
  }

  const isPdfPreview = /\/preview\/pdf\?(?:.*&)??recordingNumber=(?:\{recordingNumber\}|\d{11})/i.test(url);
  const isPngPreviewPage =
    /\/document-preview\.html\?(?:.*&)??recordingNumber=(?:\{recordingNumber\}|\d{11})/i.test(url)
    || /\/recording\/api\/document\/(?:\{recordingNumber\}|\d{11})\/page\/\d+\.(?:png|jpe?g|webp)/i.test(url);

  if (isPdfPreview || isPngPreviewPage) {
    return { valid: true };
  }

  return { valid: false, reason: 'not_artifact_preview' };
}

export function filterValidMaricopaArtifactCandidates(
  candidates: MaricopaArtifactCandidate[],
  recordingNumber?: string,
): MaricopaArtifactCandidate[] {
  return candidates.filter((candidate) => validateMaricopaArtifactCandidate(candidate, recordingNumber).valid);
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

  return filterValidMaricopaArtifactCandidates(candidates, recordingNumber);
}

export async function saveMaricopaArtifactCandidates(candidates: MaricopaArtifactCandidate[]): Promise<void> {
  await ensureDir(maricopaRootDir());
  await fs.writeFile(getMaricopaDiscoveryPath(), JSON.stringify(candidates, null, 2), 'utf8');
  await saveArtifactPayload(DISCOVERY_ARTIFACT_KEY, candidates);
}

export async function loadMaricopaArtifactCandidates(): Promise<MaricopaArtifactCandidate[]> {
  const dbPayload = await loadArtifactPayload<MaricopaArtifactCandidate[]>(DISCOVERY_ARTIFACT_KEY);
  if (Array.isArray(dbPayload)) return dbPayload;

  try {
    const raw = await fs.readFile(getMaricopaDiscoveryPath(), 'utf8');
    const parsed = JSON.parse(raw) as MaricopaArtifactCandidate[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export async function getMaricopaPersistedStateReadiness(): Promise<MaricopaPersistedStateReadiness> {
  const artifactRetrievalEnabled = isMaricopaArtifactRetrievalEnabled();
  const session = await loadMaricopaSessionState();
  const candidates = await loadMaricopaArtifactCandidates();
  const validCandidates = filterValidMaricopaArtifactCandidates(candidates);
  const sessionFresh = Boolean(session?.captured_at && isFreshMaricopaSession(session.captured_at));

  if (!artifactRetrievalEnabled) {
    return {
      artifactRetrievalEnabled,
      sessionPresent: Boolean(session),
      sessionFresh,
      sessionCapturedAt: session?.captured_at,
      artifactCandidatesPresent: validCandidates.length > 0,
      artifactCandidateCount: validCandidates.length,
      refreshRequired: false,
      refreshReason: 'artifact_retrieval_disabled',
      detail: 'Maricopa artifact retrieval is disabled by configuration.',
    };
  }

  if (!session || !sessionFresh) {
    return {
      artifactRetrievalEnabled,
      sessionPresent: Boolean(session),
      sessionFresh,
      sessionCapturedAt: session?.captured_at,
      artifactCandidatesPresent: validCandidates.length > 0,
      artifactCandidateCount: validCandidates.length,
      refreshRequired: true,
      refreshReason: 'session_missing_or_stale',
      detail: session
        ? `Maricopa session is stale (captured_at=${session.captured_at}). Run refresh:maricopa-session on the droplet.`
        : 'Maricopa session is missing. Run refresh:maricopa-session on the droplet.',
    };
  }

  if (validCandidates.length === 0) {
    return {
      artifactRetrievalEnabled,
      sessionPresent: true,
      sessionFresh: true,
      sessionCapturedAt: session.captured_at,
      artifactCandidatesPresent: false,
      artifactCandidateCount: 0,
      refreshRequired: true,
      refreshReason: 'artifact_candidates_missing',
      detail: candidates.length === 0
        ? 'Maricopa artifact candidates are missing. Run discover:maricopa-live on the droplet.'
        : 'Maricopa artifact candidates are present but invalid. Rerun discover:maricopa-live to capture preview/document endpoints.',
    };
  }

  return {
    artifactRetrievalEnabled,
    sessionPresent: true,
    sessionFresh: true,
    sessionCapturedAt: session.captured_at,
    artifactCandidatesPresent: true,
    artifactCandidateCount: validCandidates.length,
    refreshRequired: false,
    detail: 'Maricopa persisted session and artifact candidates are available.',
  };
}

export async function resolveMaricopaArtifactUrl(recordingNumber: string): Promise<string | null> {
  const envTemplate = process.env.MARICOPA_ARTIFACT_URL_TEMPLATE?.trim();
  if (envTemplate) {
    return envTemplate.split('{recordingNumber}').join(recordingNumber);
  }

  const candidates = filterValidMaricopaArtifactCandidates(await loadMaricopaArtifactCandidates(), recordingNumber);
  const preferred = candidates.find((candidate) => candidate.kind === 'pdf')
    ?? candidates.find((candidate) => candidate.kind === 'image')
    ?? candidates[0];

  if (!preferred) return null;
  return preferred.urlTemplate.split('{recordingNumber}').join(recordingNumber);
}

export async function fetchMaricopaArtifactWithSession(url: string): Promise<MaricopaArtifactFetchResult | null> {
  const session = await loadMaricopaSessionState();
  const attempts = Math.max(1, Number(process.env.MARICOPA_ARTIFACT_FETCH_ATTEMPTS ?? '2'));

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const handle = await createIsolatedBrowserContext({
      contextOptions: session?.storage_state_path ? { storageState: session.storage_state_path } : undefined,
    });

    try {
      const requestTimeout = Number(process.env.MARICOPA_ARTIFACT_FETCH_TIMEOUT_MS ?? '60000');
      const apiResponse = await handle.context.request.get(url, {
        timeout: requestTimeout,
        failOnStatusCode: false,
      }).catch(() => null);

      if (apiResponse?.ok()) {
        const contentType = apiResponse.headers()['content-type'] ?? undefined;
        const buffer = Buffer.from(await apiResponse.body());
        if (isUsableMaricopaArtifactPayload(buffer, contentType, apiResponse.url())) {
          return {
            buffer,
            contentType,
            url: apiResponse.url(),
          };
        }
      }

      const page = await handle.context.newPage();
      const response = await page.goto(url, {
        waitUntil: 'domcontentloaded',
        timeout: requestTimeout,
      }).catch(() => null);

      if (!response || !response.ok()) return null;

      const contentType = response.headers()['content-type'] ?? undefined;

      if (/text\/html/i.test(contentType ?? '') && /document-preview\.html/i.test(url)) {
        const imageUrl = await page.locator('img').first().getAttribute('src').catch(() => null);
        if (!imageUrl) return null;
        const absoluteImageUrl = new URL(imageUrl, page.url()).toString();
        const imageResponse = await handle.context.request.get(absoluteImageUrl, {
          timeout: requestTimeout,
          failOnStatusCode: false,
        }).catch(() => null);
        if (!imageResponse || !imageResponse.ok()) return null;
        const imageBuffer = Buffer.from(await imageResponse.body());
        if (!isUsableMaricopaArtifactPayload(imageBuffer, imageResponse.headers()['content-type'] ?? undefined, absoluteImageUrl)) {
          return null;
        }
        return {
          buffer: imageBuffer,
          contentType: imageResponse.headers()['content-type'] ?? undefined,
          url: absoluteImageUrl,
        };
      }

      const responseBuffer = Buffer.from(await response.body());
      if (!isUsableMaricopaArtifactPayload(responseBuffer, contentType, response.url())) {
        return null;
      }
      return {
        buffer: responseBuffer,
        contentType,
        url: response.url(),
      };
    } catch (error) {
      if (attempt >= attempts) throw error;
    } finally {
      await handle.close().catch(() => null);
    }
  }

  return null;
}
