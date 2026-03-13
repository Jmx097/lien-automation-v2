import { scrapeCASOS_Enhanced } from './ca_sos_enhanced';
import { scrapeMaricopaRecorder } from './maricopa_recorder';
import { scrapeNYCAcris } from './nyc_acris';

export const scrapers = {
  ca_sos: scrapeCASOS_Enhanced,
  maricopa_recorder: scrapeMaricopaRecorder,
  nyc_acris: scrapeNYCAcris,
};
