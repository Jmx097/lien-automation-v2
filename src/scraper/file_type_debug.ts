import fs from 'fs';
import path from 'path';
import { Page } from 'playwright';
import { log } from '../utils/logger';


function sanitizeForLog(filePath: string): string {
  return filePath.replace(/[^a-zA-Z0-9_./-]/g, '_');
}

export async function captureFileTypeSelectionFailureDebug(page: Page): Promise<void> {
  const frameCount = page.frames().length;
  const currentUrl = page.url();

  const { advancedVisible, selectMetadata } = await page.evaluate(() => {
    const normalize = (value: string | null | undefined) => (value ?? '').trim().toLowerCase();

    const advancedButton = Array.from(document.querySelectorAll('button')).find((button) =>
      normalize(button.textContent).includes('advanced')
    );

    const buttonExpanded = advancedButton?.getAttribute('aria-expanded') === 'true';
    const panelVisible = Array.from(document.querySelectorAll('input, select')).some((el) => {
      const ariaLabel = normalize(el.getAttribute('aria-label'));
      const label = normalize((el as HTMLInputElement).labels?.[0]?.textContent);
      const name = normalize((el as HTMLInputElement).name);
      return (
        ariaLabel.includes('file date') ||
        label.includes('file date') ||
        name.includes('filedate') ||
        name.includes('file_date')
      );
    });

    const allSelects = Array.from(document.querySelectorAll('select')) as HTMLSelectElement[];
    const metadata = allSelects.map((sel) => ({
      id: sel.id || '',
      name: sel.name || '',
      ariaLabel: sel.getAttribute('aria-label') || '',
      optionLabelCount: Array.from(sel.options).filter((opt) => (opt.textContent ?? '').trim().length > 0).length,
    }));

    return {
      advancedVisible: buttonExpanded || panelVisible,
      selectMetadata: metadata,
    };
  });

  log({
    stage: 'file_type_selection_failure_context',
    url: currentUrl,
    frame_count: frameCount,
    advanced_panel_visible: advancedVisible,
    select_metadata: selectMetadata,
  });

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const debugDir = path.join(process.cwd(), 'debug');
  fs.mkdirSync(debugDir, { recursive: true });

  const screenshotPath = path.join(debugDir, `file-type-failure-${timestamp}.png`);
  const htmlPath = path.join(debugDir, `file-type-failure-${timestamp}.html`);

  await page.screenshot({ path: screenshotPath, fullPage: true });
  const html = await page.content();
  fs.writeFileSync(htmlPath, html, 'utf-8');

  const safeScreenshotPath = sanitizeForLog(path.relative(process.cwd(), screenshotPath));
  const safeHtmlPath = sanitizeForLog(path.relative(process.cwd(), htmlPath));

  log({
    stage: 'file_type_selection_failure_artifacts',
    screenshot_path: safeScreenshotPath,
    html_path: safeHtmlPath,
  });
}
