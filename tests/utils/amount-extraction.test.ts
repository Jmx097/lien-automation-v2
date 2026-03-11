import { describe, expect, it } from 'vitest';
import { extractAmountFromText } from '../../src/scraper/amount-extraction';

describe('extractAmountFromText', () => {
  it('prefers keyword-adjacent amount with high confidence', () => {
    const text = 'NOTICE OF FEDERAL TAX LIEN\nTotal Amount: $123,456.78\nOther number 100';
    const result = extractAmountFromText(text, 0.75);

    expect(result.amount).toBe('123456');
    expect(result.reason).toBe('ok');
    expect(result.confidence).toBeGreaterThan(0.9);
  });

  it('keeps all digits when OCR returns an ungrouped amount', () => {
    const text = 'NOTICE OF FEDERAL TAX LIEN\nTotal Amount Due 12345678.90';
    const result = extractAmountFromText(text, 0.75);

    expect(result.amount).toBe('12345678');
    expect(result.reason).toBe('ok');
  });

  it('prefers a nearby larger dollar amount over a tiny stray keyword-line number', () => {
    const text = 'NOTICE OF FEDERAL TAX LIEN\nTOTAL AMOUNT 11\n$8,882.07';
    const result = extractAmountFromText(text, 0.75);

    expect(result.amount).toBe('8882');
    expect(result.reason).toBe('ok');
    expect(result.confidence).toBeGreaterThanOrEqual(0.8);
  });

  it('returns low-confidence when only fallback number exists', () => {
    const text = 'federal tax lien\nvalue 987654';
    const result = extractAmountFromText(text, 0.75);

    expect(result.amount).toBeUndefined();
    expect(result.reason).toBe('amount_low_confidence');
  });

  it('returns amount_not_found when no numeric amount exists', () => {
    const text = 'notice only no amount here';
    const result = extractAmountFromText(text, 0.75);

    expect(result.amount).toBeUndefined();
    expect(result.reason).toBe('amount_not_found');
  });
});
