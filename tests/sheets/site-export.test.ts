import { describe, expect, it } from 'vitest';
import { buildRowValues } from '../../src/sheets/push';

describe('site-specific sheet export mapping', () => {
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
      },
    ]);

    expect(rows[0][0]).toBe(12);
    expect(rows[0][6]).toBe('Business');
    expect(rows[0][7]).toBe('ACME LLC');
  });
});

