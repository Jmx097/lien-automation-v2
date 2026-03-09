export const supportedSites = ['ca_sos', 'nyc_acris'] as const;

export type SupportedSite = (typeof supportedSites)[number];

export interface SiteExportConfig {
  siteId: number;
  leadSource: string;
  liabilityType: string;
  state: string;
}

export const siteExportConfig: Record<SupportedSite, SiteExportConfig> = {
  ca_sos: {
    siteId: 11,
    leadSource: '777',
    liabilityType: 'IRS',
    state: 'CA',
  },
  nyc_acris: {
    siteId: Number(process.env.SITE_ID_NYC_ACRIS ?? '12'),
    leadSource: process.env.LEAD_SOURCE_NYC_ACRIS ?? '777',
    liabilityType: process.env.LIABILITY_TYPE_NYC_ACRIS ?? 'IRS',
    state: 'NY',
  },
};

