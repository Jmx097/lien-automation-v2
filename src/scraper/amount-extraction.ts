export type AmountReason =
  | 'ok'
  | 'ocr_missing'
  | 'ocr_no_text'
  | 'amount_not_found'
  | 'amount_low_confidence'
  | 'ocr_error';

export interface AmountExtractionResult {
  amount?: string;
  confidence: number;
  reason: AmountReason;
}

const KEYWORD_REGEX =
  /\b(total|amount\s+due|total\s+amount|amount|balance|liability|tax\s+due)\b/i;

function normalizeOcrText(text: string): string {
  const cleaned = text
    .replace(/[Ã¢â‚¬ËœÃ¢â‚¬â„¢]/g, "'")
    .replace(/[Ã¢â‚¬Å“Ã¢â‚¬Â]/g, '"')
    .replace(/[Ã¢â‚¬â€œÃ¢â‚¬â€]/g, '-')
    .replace(/[|]/g, 'I')
    .replace(/\$\s+/g, '$')
    .replace(/\s{2,}/g, ' ');

  return cleaned
    .replace(/(?<=\d)[Oo](?=\d)/g, '0')
    .replace(/(?<=\$)\s*[Oo](?=[\d,])/g, '0')
    .replace(/(?<=\d)[lI](?=\d)/g, '1');
}

function toWholeDollar(raw: string): number | null {
  const cleaned = raw
    .replace(/[O]/g, '0')
    .replace(/[lI]/g, '1')
    .replace(/\s+/g, '')
    .replace(/,/g, '')
    .replace(/\$/g, '');

  const parsed = Number.parseFloat(cleaned);
  if (!Number.isFinite(parsed)) return null;
  if (parsed <= 0 || parsed >= 1_000_000_000) return null;
  return Math.trunc(parsed);
}

function parseCurrencyCandidates(line: string): number[] {
  const matches =
    line.match(/\$?\s*(?:[\dOolI]{1,3}(?:[\s,][\dOolI]{3})+|[\dOolI]{4,}|[\dOolI]{1,3})(?:\.\d{1,2})?/g) ?? [];
  return matches
    .map(toWholeDollar)
    .filter((value): value is number => value !== null);
}

export function extractAmountFromText(text: string, minConfidence = 0.75): AmountExtractionResult {
  const normalized = normalizeOcrText(text);
  const lines = normalized
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (lines.length === 0) {
    return { confidence: 0, reason: 'ocr_no_text' };
  }

  const keywordCandidates: Array<{ value: number; confidence: number }> = [];
  const fallbackCandidates: Array<{ value: number; confidence: number }> = [];

  for (const line of lines) {
    const values = parseCurrencyCandidates(line);
    if (values.length === 0) continue;

    const hasKeyword = KEYWORD_REGEX.test(line);
    if (hasKeyword) {
      for (const value of values) {
        const hasDollarSign = line.includes('$');
        keywordCandidates.push({
          value,
          confidence: hasDollarSign ? 0.98 : 0.9,
        });
      }
      continue;
    }

    if (/federal|lien|tax|irs/i.test(line)) {
      for (const value of values) {
        fallbackCandidates.push({ value, confidence: 0.7 });
      }
    }
  }

  if (keywordCandidates.length > 0) {
    const best = keywordCandidates.sort((a, b) => b.value - a.value)[0];
    if (best.confidence < minConfidence) {
      return { confidence: best.confidence, reason: 'amount_low_confidence' };
    }
    return { amount: String(best.value), confidence: best.confidence, reason: 'ok' };
  }

  if (fallbackCandidates.length > 0) {
    const best = fallbackCandidates.sort((a, b) => b.value - a.value)[0];
    if (best.confidence < minConfidence) {
      return { confidence: best.confidence, reason: 'amount_low_confidence' };
    }
    return { amount: String(best.value), confidence: best.confidence, reason: 'ok' };
  }

  return { confidence: 0, reason: 'amount_not_found' };
}
