import fs from 'fs/promises';
import path from 'path';
import { execFileSync } from 'child_process';
import { extractAmountFromText, type AmountReason } from './amount-extraction';
import { checkOCRRuntime, getOCRBinaryCommands } from './ocr-runtime';

export interface MaricopaOcrExtraction {
  amount?: string;
  amountConfidence?: number;
  amountReason: AmountReason;
  leadType?: 'Lien' | 'Release';
  debtorName?: string;
  debtorAddress?: string;
}

export interface ArtifactOcrOptions {
  artifactPath: string;
  artifactContentType?: string;
  maxPages?: number;
}

function normalizeText(value: string | undefined): string {
  return (value ?? '').replace(/\s+/g, ' ').trim();
}

const STREET_SUFFIX_PATTERN =
  /\b(?:ALY|AVE|AVENUE|BLVD|BOULEVARD|CIR|CIRCLE|CT|COURT|DR|DRIVE|HWY|LANE|LN|LOOP|PKWY|PARKWAY|PL|PLACE|RD|ROAD|ST|STREET|TER|TERRACE|TRL|TRAIL|WAY)\b/i;
const CITY_STATE_ZIP_PATTERN = /\b([A-Z][A-Z .'-]+,\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?)\b/i;
const CITY_STATE_ZIP_NO_COMMA_PATTERN = /\b([A-Z][A-Z .'-]+\s+[A-Z]{2}\s+\d{5}(?:-\d{4})?)\b/i;
const EXPLICIT_UNIT_PATTERN =
  /^\s*(?:APT|UNIT|STE|SUITE|LOT|BLDG|BUILDING|FL|FLOOR|RM|ROOM|TRLR|TRAILER|SPACE|SPC|#)\s*[A-Z0-9-]{1,12}/i;
const PO_BOX_PATTERN = /\bP\.?\s*O\.?\s+BOX\b/i;

function trimTrailingAddressNoiseAfterZip(value: string): string {
  const zipPattern = /\b\d{5}(?:-\d{4})?\b/g;
  let match: RegExpExecArray | null;
  let trimmed = value;

  while ((match = zipPattern.exec(value)) !== null) {
    const zipEnd = match.index + match[0].length;
    const suffix = value.slice(zipEnd).trim();
    if (!suffix || /^[._,;:|/\\()\-[\]\s]*\d{0,2}[A-Za-z]?$/.test(suffix)) {
      trimmed = value.slice(0, zipEnd);
    }
  }

  return trimmed;
}

function trimStreetLineNoise(value: string): string {
  const normalized = value
    .replace(/[|]+/g, ' ')
    .replace(/[^A-Za-z0-9#&/.,'\-\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  const suffixMatches = [...normalized.matchAll(new RegExp(STREET_SUFFIX_PATTERN.source, 'ig'))];
  const lastSuffix = suffixMatches[suffixMatches.length - 1];
  if (!lastSuffix || typeof lastSuffix.index !== 'number') {
    return normalized;
  }

  const suffixEnd = lastSuffix.index + lastSuffix[0].length;
  const suffixTail = normalized.slice(suffixEnd).trim();
  const optionalUnit = suffixTail.match(EXPLICIT_UNIT_PATTERN)?.[0] ?? '';

  return `${normalized.slice(0, suffixEnd)} ${optionalUnit}`.trim();
}

function isSkippableOcrNoiseLine(value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed) return true;

  const compact = trimmed.replace(/[^A-Za-z0-9]/g, '');
  if (!compact) return true;
  if (compact.length <= 2) return true;
  if (/\d{5}(?:-\d{4})?$/.test(compact)) return false;
  if (/\b(?:APT|UNIT|STE|SUITE|LOT|BLDG|BUILDING|FL|FLOOR|RM|ROOM|POBOX)\b/i.test(compact)) return false;

  const alphaCount = (compact.match(/[A-Za-z]/g)?.length ?? 0);
  const digitCount = (compact.match(/\d/g)?.length ?? 0);
  if (digitCount === 0 && alphaCount <= 8) return true;
  if (digitCount > 0 && compact.length <= 6 && alphaCount <= 2) return true;

  return false;
}

function extractStreetSegment(value: string): string {
  const normalized = normalizeText(value);
  const cityStateZipMatch = normalized.match(CITY_STATE_ZIP_PATTERN) ?? normalized.match(CITY_STATE_ZIP_NO_COMMA_PATTERN);
  if (cityStateZipMatch && typeof cityStateZipMatch.index === 'number') {
    return normalized.slice(0, cityStateZipMatch.index).replace(/[,\s]+$/g, '').trim();
  }

  return normalized.split(/\s*,\s*/)[0]?.trim() ?? '';
}

export function isSuspiciousMaricopaAddress(value: string | undefined): boolean {
  const normalized = normalizeText(value);
  if (!normalized) return true;
  if (!/\d/.test(normalized) || !/[A-Za-z]/.test(normalized)) return true;
  if (!/\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b/i.test(normalized)) return true;

  const streetSegment = extractStreetSegment(normalized);
  if (!streetSegment) return true;
  if (PO_BOX_PATTERN.test(streetSegment)) return false;

  const suffixMatches = [...streetSegment.matchAll(new RegExp(STREET_SUFFIX_PATTERN.source, 'ig'))];
  const lastSuffix = suffixMatches[suffixMatches.length - 1];
  if (!lastSuffix || typeof lastSuffix.index !== 'number') {
    return true;
  }

  const tail = streetSegment.slice(lastSuffix.index + lastSuffix[0].length).trim();
  if (!tail) return false;

  return !EXPLICIT_UNIT_PATTERN.test(` ${tail}`);
}

export function normalizeMaricopaOcrAddress(value: string | undefined): string | undefined {
  if (!value) return undefined;

  const normalized = value
    .replace(/\r/g, '\n')
    .replace(/\b(?:total\s+amount\s+due|tax\s+period|important|kind\s+of\s+tax|serial\s+number|unpaid\s+balance)\b[\s\S]*$/i, ' ')
    .replace(/\bResidence\b[:\-\s]*/gi, ' ')
    .replace(/\b(Address|Taxpayer Address)\b[:\-\s]*/gi, ' ')
    .replace(/_+/g, ' ')
    .replace(/[|]+/g, ' ')
    .replace(/\n+/g, ' ')
    .replace(/\s+/g, ' ')
    .replace(/\s+([,.;:])/g, '$1')
    .trim()
    .replace(/([,.;:])(?=[A-Za-z])/g, '$1 ')
    .replace(/\s+,/g, ',')
    .replace(/,\s*,+/g, ',')
    .trim();

  const cityStateZipMatch = normalized.match(/(.*?)([A-Z][A-Z .'-]+,\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?)(?:\s+.*)?$/i);
  if (cityStateZipMatch && /\d/.test(cityStateZipMatch[1])) {
    const rawStreetPart = cityStateZipMatch[1].trim();
    if (!STREET_SUFFIX_PATTERN.test(rawStreetPart)) {
      // Avoid splitting single-line addresses where the city token is fused into the same segment.
      const cleanedFallback = trimTrailingAddressNoiseAfterZip(normalized)
        .replace(/\s+([,.;:])/g, '$1')
        .replace(/,\s*,+/g, ',')
        .replace(/[.;:,]+$/g, '')
        .trim();
      if (cleanedFallback && /\d/.test(cleanedFallback) && cleanedFallback.length >= 8) {
        return cleanedFallback;
      }
    }

    const streetPart = trimStreetLineNoise(rawStreetPart);
    const cityStateZip = trimTrailingAddressNoiseAfterZip(cityStateZipMatch[2].trim());
    const rebuilt = [streetPart, cityStateZip].filter(Boolean).join(', ').replace(/[.;:,]+$/g, '').trim();
    if (rebuilt && /\d/.test(rebuilt) && rebuilt.length >= 8) {
      return rebuilt;
    }
  }

  const parts = trimTrailingAddressNoiseAfterZip(normalized)
    .split(/\s*,\s*/)
    .filter(Boolean);
  const hasSeparatedCityStateZip =
    parts.length >= 3 && /^[A-Z]{2}\s+\d{5}(?:-\d{4})?$/i.test(parts[parts.length - 1]);
  const cleanedStreet = parts[0]
    ? hasSeparatedCityStateZip
      ? trimStreetLineNoise(parts[0])
      : parts[0]
    : '';
  const cleaned = [cleanedStreet, ...parts.slice(1)].filter(Boolean).join(', ')
    .replace(/\s+([,.;:])/g, '$1')
    .replace(/,\s*,+/g, ',')
    .replace(/[.;:,]+$/g, '')
    .trim();

  if (!cleaned) return undefined;
  if (!/\d/.test(cleaned)) return undefined;
  if (cleaned.length < 8) return undefined;
  return cleaned;
}

function extractResidenceBlock(text: string): string | undefined {
  const inlineBlockMatch = text.match(
    /Residence\b[:\-\s]*([^\n\r]+)[\r\n\s]+([A-Z][A-Z .'-]+,\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?)/i,
  );
  if (inlineBlockMatch) {
    const normalized = normalizeMaricopaOcrAddress(`${inlineBlockMatch[1]}, ${inlineBlockMatch[2]}`);
    if (normalized) return normalized;
  }

  const lines = text
    .replace(/\r/g, '\n')
    .split('\n')
    .map((line) => line.replace(/[|]+/g, ' ').trim())
    .filter(Boolean);

  const stopPattern = /^(important|tax period|kind of tax|serial number|unpaid balance|place of filing|recording and endorsement cover page|document id:|fees and taxes|cross reference data)\b/i;
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const residenceMatch = line.match(/\b(?:Residence|Address|Taxpayer Address)\b[:\-\s]*(.*)$/i);
    if (!residenceMatch) continue;

    const collected: string[] = [];
    const remainder = residenceMatch[1]?.trim();
    if (remainder) {
      collected.push(remainder);
    }

    for (let nextIndex = index + 1; nextIndex < lines.length && collected.length < 3; nextIndex += 1) {
      const nextLine = lines[nextIndex];
      if (stopPattern.test(nextLine)) break;

      const cityStateZipMatch = nextLine.match(CITY_STATE_ZIP_PATTERN) ?? nextLine.match(CITY_STATE_ZIP_NO_COMMA_PATTERN);
      if (cityStateZipMatch?.[1]) {
        collected.push(cityStateZipMatch[1]);
        break;
      }

      if (isSkippableOcrNoiseLine(nextLine)) continue;

      if (/\d/.test(nextLine) || /\b(?:APT|UNIT|FL|FLOOR|SUITE|STE|PO BOX)\b/i.test(nextLine)) {
        collected.push(nextLine);
        continue;
      }

      break;
    }

    if (collected.length === 0) continue;

    const rawAddress =
      collected.length >= 2 && (CITY_STATE_ZIP_PATTERN.test(collected[1]) || CITY_STATE_ZIP_NO_COMMA_PATTERN.test(collected[1]))
        ? `${collected[0]}, ${collected[1]}`
        : collected.join(' ');

    const normalized = normalizeMaricopaOcrAddress(rawAddress);
    if (normalized) return normalized;
  }

  return undefined;
}

function sanitizeDebtorName(value: string | undefined): string | undefined {
  const normalized = normalizeText(value)
    .replace(/\b(?:name of taxpayer|taxpayer name|recorded|filed|doc(?:ument)?\s+date|party\s*[12]|remarks)\b[:\-\s]*/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!normalized) return undefined;
  if (!/[A-Za-z]/.test(normalized)) return undefined;
  if (/^(?:internal revenue service|irs)$/i.test(normalized)) return undefined;
  if (/^\d{1,2}\/\d{1,2}\/\d{4}/.test(normalized)) return undefined;
  if (/[0-9\[\]{}|]/.test(normalized)) return undefined;
  if ((normalized.match(/[^A-Za-z\s,'.&-]/g)?.length ?? 0) > 2) return undefined;
  if (normalized.length > 120) return undefined;
  return normalized;
}

export function extractMaricopaFieldsFromText(text: string): Pick<MaricopaOcrExtraction, 'leadType' | 'debtorName' | 'debtorAddress'> {
  let leadType: 'Lien' | 'Release' | undefined;
  if (/certificate\s+of\s+release\s+of\s+federal/i.test(text) || /form\s+668\s*\(?\s*z\s*\)?/i.test(text)) {
    leadType = 'Release';
  } else if (/notice\s+of\s+federal\s+tax\s+li/i.test(text) || /form\s+668\s*\(?\s*y\s*\)?/i.test(text)) {
    leadType = 'Lien';
  }

  const debtorMatch =
    text.match(/name\s+of\s+taxpayer\s+(.+?)(?:\n|residence|address)/is) ??
    text.match(/taxpayer\s+name\s*[:\-]?\s*(.+?)(?:\n|residence|address)/is);
  const debtorName = sanitizeDebtorName(debtorMatch?.[1]);
  const debtorAddress = extractResidenceBlock(text);

  return {
    leadType,
    debtorName,
    debtorAddress,
  };
}

async function ensureDir(dir: string): Promise<void> {
  await fs.mkdir(dir, { recursive: true });
}

async function ocrImageFile(imagePath: string): Promise<string> {
  const outputBase = imagePath.replace(/\.[^.]+$/, '_ocr');
  const { tesseract } = getOCRBinaryCommands();
  try {
    execFileSync(tesseract, [imagePath, outputBase, '--psm', '6'], { stdio: 'ignore', timeout: 30000 });
    const textPath = `${outputBase}.txt`;
    const text = await fs.readFile(textPath, 'utf8').catch(() => '');
    await fs.rm(textPath, { force: true }).catch(() => null);
    return text;
  } catch {
    await fs.rm(`${outputBase}.txt`, { force: true }).catch(() => null);
    return '';
  }
}

async function ocrPdf(pdfPath: string, maxPages: number): Promise<string> {
  const dir = path.dirname(pdfPath);
  const base = path.basename(pdfPath, path.extname(pdfPath));
  const imgPrefix = path.join(dir, `${base}_page`);
  const { pdftoppm } = getOCRBinaryCommands();

  execFileSync(pdftoppm, ['-png', '-r', '300', pdfPath, imgPrefix], { stdio: 'ignore', timeout: 20000 });

  const imgFiles = (await fs.readdir(dir))
    .filter((file) => file.startsWith(`${base}_page`) && file.endsWith('.png'))
    .sort()
    .slice(0, maxPages)
    .map((file) => path.join(dir, file));

  let fullText = '';
  for (const imgFile of imgFiles) {
    const text = await ocrImageFile(imgFile);
    await fs.rm(imgFile, { force: true }).catch(() => null);
    if (text.trim()) fullText += `${text}\n`;
  }

  return fullText;
}

export async function extractMaricopaFieldsFromArtifact(options: ArtifactOcrOptions): Promise<MaricopaOcrExtraction> {
  const runtime = checkOCRRuntime();
  if (!runtime.ok) return { amountReason: 'ocr_missing' };

  const maxPages = Math.max(1, options.maxPages ?? Number(process.env.MARICOPA_OCR_MAX_PAGES ?? '2'));
  const ext = path.extname(options.artifactPath).toLowerCase();
  const isPdf = options.artifactContentType?.includes('pdf') || ext === '.pdf';
  const isImage = options.artifactContentType?.startsWith('image/') || ['.png', '.jpg', '.jpeg', '.webp'].includes(ext);

  if (!isPdf && !isImage) {
    return { amountReason: 'ocr_error' };
  }

  const workDir = path.join(path.dirname(options.artifactPath), 'ocr-work');
  await ensureDir(workDir);

  let fullText = '';
  try {
    if (isPdf) {
      fullText = await ocrPdf(options.artifactPath, maxPages);
    } else {
      fullText = await ocrImageFile(options.artifactPath);
    }
  } catch {
    return { amountReason: 'ocr_error' };
  } finally {
    await fs.rm(workDir, { recursive: true, force: true }).catch(() => null);
  }

  if (!fullText.trim()) {
    return { amountReason: 'ocr_no_text' };
  }

  const amountResult = extractAmountFromText(fullText, Number(process.env.AMOUNT_MIN_CONFIDENCE ?? '0.75'));
  const fields = extractMaricopaFieldsFromText(fullText);

  return {
    amount: amountResult.amount,
    amountConfidence: amountResult.confidence,
    amountReason: amountResult.reason,
    leadType: fields.leadType,
    debtorName: fields.debtorName,
    debtorAddress: fields.debtorAddress,
  };
}
