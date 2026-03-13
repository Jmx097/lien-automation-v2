import fs from 'fs';
import path from 'path';
import { describe, expect, it } from 'vitest';
import { extractMaricopaFieldsFromText, normalizeMaricopaOcrAddress } from '../../src/scraper/maricopa_ocr';

const fixtureDir = path.join(process.cwd(), 'tests', 'fixtures', 'maricopa');

describe('maricopa OCR parsing', () => {
  it('extracts lead type debtor name and address from OCR text', () => {
    const text = fs.readFileSync(path.join(fixtureDir, 'ocr-text.txt'), 'utf8');
    expect(extractMaricopaFieldsFromText(text)).toEqual({
      leadType: 'Lien',
      debtorName: 'BASCH DAVID',
      debtorAddress: '123 MAIN ST, PHOENIX, AZ 85003',
    });
  });

  it('normalizes OCR address noise after zip codes', () => {
    expect(normalizeMaricopaOcrAddress('123 MAIN ST PHOENIX, AZ 85003 _ 1')).toBe('123 MAIN ST PHOENIX, AZ 85003');
  });
});
