import { chromium, Browser } from "playwright";

/**
 * Creates a browser instance for scraping.
 *
 * - Production: connects to Bright Data's remote Scraping Browser via CDP.
 *   Handles residential IP rotation, CAPTCHA solving, and fingerprinting automatically.
 *   Requires BRIGHTDATA_AUTH env var in format "username:password".
 *
 * - Development: launches a local headless Chromium instance (no env var needed).
 */
export async function createBrowser(): Promise<Browser> {
  if (process.env.BRIGHTDATA_AUTH) {
    return chromium.connectOverCDP(
      `wss://${process.env.BRIGHTDATA_AUTH}@brd.superproxy.io:9222`
    );
  }
  return chromium.launch({ headless: true });
}
