import { Page } from 'playwright';
import { log } from '../../utils/logger';

interface SelectFileTypeOptions {
  onFailure?: () => Promise<void>;
}

type StrategyName = 'locator_label' | 'locator_combobox' | 'locator_select' | 'dom_fallback';

const STRATEGY_TIMEOUT_MS = 2500;
const TARGET_LABEL = 'Federal Tax Lien';

async function tryLocatorStrategy(page: Page, strategy: Exclude<StrategyName, 'dom_fallback'>): Promise<boolean> {
  const locator =
    strategy === 'locator_label'
      ? page.getByLabel(/file type/i)
      : strategy === 'locator_combobox'
        ? page.getByRole('combobox', { name: /file type/i })
        : page.locator('select[aria-label*="File Type" i], select[name*="fileType" i], select[id*="fileType" i]');

  log({ stage: 'file_type_strategy_attempt', strategy, timeout_ms: STRATEGY_TIMEOUT_MS });

  try {
    const control = locator.first();
    await control.waitFor({ state: 'visible', timeout: STRATEGY_TIMEOUT_MS });
    await control.selectOption({ label: TARGET_LABEL }, { timeout: STRATEGY_TIMEOUT_MS });
    log({ stage: 'file_type_selected', method: strategy });
    return true;
  } catch (error) {
    log({
      stage: 'file_type_strategy_failed',
      strategy,
      error: error instanceof Error ? error.message : String(error),
    });
    return false;
  }
}

async function tryDomFallback(page: Page): Promise<boolean> {
  const startedAt = Date.now();
  log({ stage: 'file_type_strategy_attempt', strategy: 'dom_fallback', timeout_ms: STRATEGY_TIMEOUT_MS });

  while (Date.now() - startedAt < STRATEGY_TIMEOUT_MS) {
    try {
      const selected = await page.evaluate(({ targetLabel }) => {
        const normalized = (value: string | null | undefined) => (value ?? '').trim().toLowerCase();
        const selects = Array.from(document.querySelectorAll('select')) as HTMLSelectElement[];

        const byNameOrId = selects.find((sel) => {
          const id = normalized(sel.id);
          const name = normalized(sel.name);
          return id.includes('filetype') || id.includes('file_type') || name.includes('filetype') || name.includes('file_type');
        });

        const withLabel = selects.find((sel) => {
          const id = sel.id;
          if (!id) return false;
          const label = document.querySelector(`label[for="${id}"]`);
          return normalized(label?.textContent).includes('file type');
        });

        const withAria = selects.find((sel) => normalized(sel.getAttribute('aria-label')).includes('file type'));
        const match = byNameOrId ?? withLabel ?? withAria;

        if (!match) return false;

        const option = Array.from(match.options).find((opt) => normalized(opt.text).includes(normalized(targetLabel)));
        if (!option) return false;

        match.value = option.value;
        match.dispatchEvent(new Event('input', { bubbles: true }));
        match.dispatchEvent(new Event('change', { bubbles: true }));
        return true;
      }, { targetLabel: TARGET_LABEL });

      if (selected) {
        log({ stage: 'file_type_selected', method: 'dom_fallback' });
        return true;
      }
    } catch (error) {
      log({
        stage: 'file_type_strategy_failed',
        strategy: 'dom_fallback',
        error: error instanceof Error ? error.message : String(error),
      });
      return false;
    }

    await page.waitForTimeout(250);
  }

  log({ stage: 'file_type_strategy_failed', strategy: 'dom_fallback', error: 'No matching select/option found' });
  return false;
}

export async function selectFileType(page: Page, options: SelectFileTypeOptions = {}): Promise<void> {
  const strategies: Array<Exclude<StrategyName, 'dom_fallback'>> = ['locator_label', 'locator_combobox', 'locator_select'];

  for (const strategy of strategies) {
    if (await tryLocatorStrategy(page, strategy)) return;
  }

  if (await tryDomFallback(page)) return;

  if (options.onFailure) {
    await options.onFailure();
  }

  throw new Error(
    'Could not find/select File Type control after trying strategies: locator_label, locator_combobox, locator_select, dom_fallback.'
  );
}
