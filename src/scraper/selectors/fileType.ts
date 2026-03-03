import { Page } from "playwright";

// Playwright runtime strategy helper for production scrapers.
// For DOM-only smoke/unit tests, use src/scraper/file_type_selector.ts instead.
type Logger = (payload: Record<string, unknown>) => void;

interface SelectFileTypeOptions {
  log?: Logger;
  onFailure?: () => Promise<void> | void;
}

export async function selectFederalTaxLienFileType(page: Page, log?: Logger): Promise<boolean> {
  const fileTypeSelectCandidates = [
    { method: "label", locator: page.getByLabel(/file type/i) },
    { method: "combobox_role", locator: page.getByRole("combobox", { name: /file type/i }) },
    { method: "aria_label_select", locator: page.locator('select[aria-label*="File Type" i]') },
    { method: "name_or_id_select", locator: page.locator('select[name*="fileType" i], select[id*="fileType" i]') },
  ];

  for (const candidate of fileTypeSelectCandidates) {
    if ((await candidate.locator.count()) === 0) continue;

    const control = candidate.locator.first();
    try {
      await control.waitFor({ state: "visible", timeout: 2000 });
      await control.selectOption({ label: "Federal Tax Lien" }, { timeout: 2000 });
      log?.({ stage: "file_type_selected", method: candidate.method });
      return true;
    } catch (err: unknown) {
      log?.({
        stage: "file_type_candidate_failed",
        method: candidate.method,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  const fileTypeSelected = await page.evaluate((value) => {
    // TODO: follow-up refactor: deduplicate matching logic shared with src/scraper/file_type_selector.ts.
    const normalizeInPage = (candidate: string | null | undefined) => (candidate ?? "").trim().toLowerCase();
    const selects = Array.from(document.querySelectorAll("select")) as HTMLSelectElement[];

    const byNameOrId = selects.find((sel) => {
      const id = normalizeInPage(sel.id);
      const name = normalizeInPage(sel.name);
      return id.includes("filetype") || id.includes("file_type") || name.includes("filetype") || name.includes("file_type");
    });

    const withLabel = selects.find((sel) => {
      const id = sel.id;
      if (!id) return false;
      const label = document.querySelector(`label[for="${id}"]`);
      return normalizeInPage(label?.textContent).includes("file type");
    });

    const byAria = selects.find((sel) => normalizeInPage(sel.getAttribute("aria-label")).includes("file type"));

    const fallback = byNameOrId ?? withLabel ?? byAria;
    if (!fallback) return false;

    const option = Array.from(fallback.options).find((opt) => normalizeInPage(opt.text).includes(normalizeInPage(value)));
    if (!option) return false;

    fallback.value = option.value;
    fallback.dispatchEvent(new Event("input", { bubbles: true }));
    fallback.dispatchEvent(new Event("change", { bubbles: true }));
    return true;
  }, "Federal Tax Lien");

  if (fileTypeSelected) {
    log?.({ stage: "file_type_selected", method: "dom_fallback" });
  }

  return fileTypeSelected;
}

export async function selectFileType(
  page: Page,
  { log, onFailure }: SelectFileTypeOptions = {},
): Promise<boolean> {
  const selected = await selectFederalTaxLienFileType(page, log);

  if (!selected) {
    await onFailure?.();
  }

  return selected;
}
