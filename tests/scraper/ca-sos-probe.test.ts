import { describe, expect, it } from 'vitest';
import { parseCASOSResultsCount } from '../../src/scraper/ca_sos_enhanced';

describe('parseCASOSResultsCount', () => {
  it('parses a visible results count', () => {
    expect(parseCASOSResultsCount('Results: 31')).toBe(31);
  });

  it('parses a zero-result header', () => {
    expect(parseCASOSResultsCount('Results: 0')).toBe(0);
  });

  it('returns null when the results header is missing', () => {
    expect(parseCASOSResultsCount('Loading search results...')).toBeNull();
  });
});
