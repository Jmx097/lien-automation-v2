import { describe, expect, it } from 'vitest';
import { buildRowValues, FROZEN_SHEET_HEADERS } from '../../src/sheets/push';

describe('site-specific sheet export mapping', () => {
  it('matches the frozen top row exactly', () => {
    expect(FROZEN_SHEET_HEADERS).toEqual([
      'Site Id',
      'LienOrReceiveDate',
      'Amount',
      'LeadType',
      'LeadSource',
      'LiabilityType',
      'BusinessPersonal',
      'Company',
      'FirstName',
      'LastName',
      'Street',
      'City',
      'State',
      'Zip',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
    ]);
  });

  it('uses the nyc site config when exporting nyc acris rows', () => {
    const rows = buildRowValues([
      {
        state: 'NY',
        source: 'nyc_acris',
        county: 'New York City',
        ucc_type: 'Federal Tax Lien',
        debtor_name: 'ACME LLC',
        debtor_address: '123 Main St, New York, NY 10001',
        file_number: '2026022700399005',
        secured_party_name: 'Internal Revenue Service',
        secured_party_address: '',
        status: 'Active',
        filing_date: '02/27/2026',
        lapse_date: '12/31/9999',
        document_type: 'FEDERAL LIEN-IRS',
        pdf_filename: '',
        processed: true,
        confidence_score: 0.88,
      },
    ]);

    expect(rows[0][0]).toBe(12);
    expect(rows[0]).toHaveLength(FROZEN_SHEET_HEADERS.length);
    expect(rows[0][3]).toBe('Lien');
    expect(rows[0][6]).toBe('Business');
    expect(rows[0][7]).toBe('ACME LLC');
    expect(rows[0][10]).toBe('123 Main St');
    expect(rows[0][11]).toBe('New York');
    expect(rows[0][13]).toBe('10001');
  });

  it('parses OCR-style addresses without commas into street city and zip', () => {
    const rows = buildRowValues([
      {
        state: 'NY',
        source: 'nyc_acris',
        county: 'New York City',
        ucc_type: 'Federal Tax Lien',
        debtor_name: 'DOMINIQUE PIERRE LOUIS',
        debtor_address: '123 MAIN ST BROOKLYN NY 11201',
        file_number: '2026030600410003',
        secured_party_name: 'Internal Revenue Service',
        secured_party_address: '',
        status: 'Active',
        filing_date: '03/06/2026',
        lapse_date: '12/31/9999',
        document_type: 'FEDERAL LIEN-IRS',
        pdf_filename: '',
        processed: true,
        confidence_score: 0.98,
      },
    ]);

    expect(rows[0][10]).toBe('123 MAIN ST');
    expect(rows[0][11]).toBe('BROOKLYN');
    expect(rows[0][12]).toBe('NY');
    expect(rows[0][13]).toBe('11201');
  });

  it('trims OCR zip suffix noise before splitting structured address columns', () => {
    const rows = buildRowValues([
      {
        state: 'NY',
        source: 'nyc_acris',
        county: 'New York City',
        ucc_type: 'Federal Tax Lien',
        debtor_name: 'DOMINIQUE PIERRE LOUIS',
        debtor_address: '160 COLUMBIA HTS APT 10C BROOKLYN, NY 11201-2189 . 1',
        file_number: '2026030600410002',
        secured_party_name: 'Internal Revenue Service',
        secured_party_address: '',
        status: 'Active',
        filing_date: '03/06/2026',
        lapse_date: '12/31/9999',
        document_type: 'FEDERAL LIEN-IRS',
        pdf_filename: '',
        processed: true,
        confidence_score: 0.92,
      },
    ]);

    expect(rows[0][10]).toBe('160 COLUMBIA HTS APT 10C');
    expect(rows[0][11]).toBe('BROOKLYN');
    expect(rows[0][12]).toBe('NY');
    expect(rows[0][13]).toBe('11201');
  });

  it('trims underscore OCR zip suffix noise and ignores punctuation-only name fragments', () => {
    const rows = buildRowValues([
      {
        state: 'NY',
        source: 'nyc_acris',
        county: 'New York City',
        ucc_type: 'Federal Tax Lien',
        debtor_name: 'ALBERTO :',
        debtor_address: '160 COLUMBIA HTS APT 10C BROOKLYN, NY 11201-2189 _',
        file_number: '2026030600410002',
        secured_party_name: 'Internal Revenue Service',
        secured_party_address: '',
        status: 'Active',
        filing_date: '03/06/2026',
        lapse_date: '12/31/9999',
        document_type: 'FEDERAL LIEN-IRS',
        pdf_filename: '',
        processed: true,
        confidence_score: 0.92,
      },
    ]);

    expect(rows[0][8]).toBe('ALBERTO');
    expect(rows[0][9]).toBe('');
    expect(rows[0][10]).toBe('160 COLUMBIA HTS APT 10C');
    expect(rows[0][11]).toBe('BROOKLYN');
    expect(rows[0][12]).toBe('NY');
    expect(rows[0][13]).toBe('11201');
  });

  it('leaves structured address fields blank when OCR address text is unusable', () => {
    const rows = buildRowValues([
      {
        state: 'NY',
        source: 'nyc_acris',
        county: 'New York City',
        ucc_type: 'Federal Tax Lien',
        debtor_name: 'ACME HOLDINGS LLC',
        debtor_address: '',
        file_number: '2026030600410004',
        secured_party_name: 'Internal Revenue Service',
        secured_party_address: '',
        status: 'Active',
        filing_date: '03/06/2026',
        lapse_date: '12/31/9999',
        document_type: 'FEDERAL LIEN-IRS',
        pdf_filename: '',
        processed: true,
      },
    ]);

    expect(rows[0][10]).toBe('');
    expect(rows[0][11]).toBe('');
    expect(rows[0][13]).toBe('');
  });
});
