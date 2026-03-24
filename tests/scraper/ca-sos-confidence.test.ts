import { describe, expect, it } from 'vitest';
import { interpretCASOSResultsState, resolveCARecordConfidenceScore } from '../../src/scraper/ca_sos_enhanced';

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

  it('accepts visible CA result rows even when the results banner is absent', () => {
    const result = interpretCASOSResultsState({
      finalUrl: 'https://bizfileonline.sos.ca.gov/search/ucc',
      title: 'Search Results',
      readyState: 'complete',
      resultsContainerVisible: true,
      rowCount: 0,
      drawerButtonCount: 3,
      resultCount: null,
      noResultsVisible: false,
    });

    expect(result).toEqual(
      expect.objectContaining({
        hasNoResults: false,
        rowCount: 3,
        resultCount: 3,
      }),
    );
  });

  it('treats an explicit no-results state as zero results', () => {
    const result = interpretCASOSResultsState({
      finalUrl: 'https://bizfileonline.sos.ca.gov/search/ucc',
      title: 'Search Results',
      readyState: 'complete',
      resultsContainerVisible: false,
      rowCount: 0,
      drawerButtonCount: 0,
      resultCount: null,
      noResultsVisible: true,
    });

    expect(result).toEqual(
      expect.objectContaining({
        hasNoResults: true,
        rowCount: 0,
        resultCount: 0,
      }),
    );
  });

  it('accepts results count text while rows are still settling into the container', () => {
    const result = interpretCASOSResultsState(
      {
        finalUrl: 'https://bizfileonline.sos.ca.gov/search/ucc',
        title: 'Search Results',
        readyState: 'complete',
        resultsContainerVisible: true,
        rowCount: 0,
        drawerButtonCount: 0,
        resultCountText: 'Results: 18',
        resultCount: 18,
        noResultsVisible: false,
      },
      { allowContainerOnlyCount: true },
    );

    expect(result).toEqual(
      expect.objectContaining({
        hasNoResults: false,
        rowCount: 0,
        resultCount: 18,
      }),
    );
  });
});
