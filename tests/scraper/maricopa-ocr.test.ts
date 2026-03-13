import fs from 'fs';
import path from 'path';
import { describe, expect, it } from 'vitest';
import {
  extractMaricopaFieldsFromText,
  isSuspiciousMaricopaAddress,
  normalizeMaricopaOcrAddress,
} from '../../src/scraper/maricopa_ocr';

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

  it('keeps a clean residence block and rejects noisy debtor-name tails', () => {
    const text = [
      'Name of Taxpayer DAVID BASCH J 4 Zi Oo m& 7 re]',
      'Residence 3436 E MARLENE DR bs Gneg 4 4',
      'GILBERT, AZ 85296',
      'Total Amount Due 96408.57',
    ].join('\n');

    expect(extractMaricopaFieldsFromText(text)).toEqual({
      leadType: undefined,
      debtorName: undefined,
      debtorAddress: '3436 E MARLENE DR, GILBERT, AZ 85296',
    });
  });

  it('trims OCR junk after a recognized street suffix', () => {
    expect(normalizeMaricopaOcrAddress('106 S OREGON ST bO ° mA led, CHANDLER, AZ 85225-0000')).toBe(
      '106 S OREGON ST, CHANDLER, AZ 85225-0000',
    );
    expect(normalizeMaricopaOcrAddress('39506 N DAISY MOUNTAIN DR 122 471 agooos 72, ANTHEM, AZ 85086-1665')).toBe(
      '39506 N DAISY MOUNTAIN DR, ANTHEM, AZ 85086-1665',
    );
    expect(normalizeMaricopaOcrAddress('3436 E MARLENE DR bs Gneg 4 4 GILBERT, AZ 85296 O oS OF')).toBe(
      '3436 E MARLENE DR, GILBERT, AZ 85296',
    );
  });
  it('preserves explicit unit designators while trimming OCR junk', () => {
    expect(normalizeMaricopaOcrAddress('123 MAIN ST APT 5 xx xX, PHOENIX, AZ 85003')).toBe(
      '123 MAIN ST APT 5, PHOENIX, AZ 85003',
    );
    expect(normalizeMaricopaOcrAddress('456 W CENTER RD # 12 ae oe, MESA, AZ 85201')).toBe(
      '456 W CENTER RD # 12, MESA, AZ 85201',
    );
  });

  it('skips short OCR garbage lines between residence and city lines', () => {
    const text = [
      'Residence 106 S OREGON ST',
      'ae oS oe',
      'CHANDLER, AZ 85225-0000',
      'Total Amount Due 15328.12',
    ].join('\n');

    expect(extractMaricopaFieldsFromText(text).debtorAddress).toBe('106 S OREGON ST, CHANDLER, AZ 85225-0000');
  });

  it('flags leftover street-tail junk as suspicious after normalization', () => {
    expect(isSuspiciousMaricopaAddress('123 MAIN ST bs Gneg 4 4, PHOENIX, AZ 85003')).toBe(true);
    expect(isSuspiciousMaricopaAddress('123 MAIN ST APT 5, PHOENIX, AZ 85003')).toBe(false);
    expect(isSuspiciousMaricopaAddress('PO BOX 123, PHOENIX, AZ 85003')).toBe(false);
  });
});
