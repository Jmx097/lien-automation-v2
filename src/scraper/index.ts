import { scrapeCASOS_Enhanced } from './ca_sos_enhanced';
import { scrapeNYCAcris } from './nyc_acris';

export const scrapers = {
  ca_sos: scrapeCASOS_Enhanced,
  nyc_acris: scrapeNYCAcris,
};
