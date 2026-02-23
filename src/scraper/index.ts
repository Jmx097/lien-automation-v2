import { scrapeCASOS } from "./ca_sos";
// TODO: import { scrapeNYCACRIS } from "./nyc_acris";
// TODO: import { scrapeCookCounty } from "./cook_county";

/**
 * Scraper registry — maps site keys to their scraper functions.
 * Add new scrapers here as they are implemented.
 * Used by the server's /scrape and /scrape-all endpoints for dynamic routing.
 */
export const scrapers: Record<string, Function> = {
  ca_sos: scrapeCASOS,
  // nyc_acris: scrapeNYCACRIS,
  // cook_county: scrapeCookCounty,
};
