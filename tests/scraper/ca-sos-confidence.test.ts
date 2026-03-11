import { describe, expect, it } from 'vitest';
import { resolveCARecordConfidenceScore } from '../../src/scraper/ca_sos_enhanced';

describe('resolveCARecordConfidenceScore', () => {
  it('uses OCR agreement and structured residence to produce a high-confidence score', () => {
    const score = resolveCARecordConfidenceScore(
      'Acme Holdings LLC',
      '123 Main St, Los Angeles, CA 90012',
      {
        amountConfidence: 0.82,
        amountReason: 'ok',
        taxpayerName: 'ACME HOLDINGS LLC',
        residence: '123 Main St, Los Angeles, CA 90012',
      }
    );

    expect(score).toBe(0.92);
  });

  it('falls back to debtor fields when OCR debtor details are unavailable', () => {
    const score = resolveCARecordConfidenceScore(
      'Jane Smith',
      '500 Market St, San Francisco, CA 94105',
      {
        amountReason: 'ocr_missing',
      }
    );

    expect(score).toBe(0.85);
  });

  it('preserves low-confidence amount matches when that is the strongest signal', () => {
    const score = resolveCARecordConfidenceScore(
      '',
      '',
      {
        amountConfidence: 0.5,
        amountReason: 'amount_low_confidence',
      }
    );

    expect(score).toBe(0.5);
  });
});
