import { Page } from "playwright";

type Logger = (payload: Record<string, unknown>) => void;

const normalize = (value: string | null | undefined) => (value ?? "").trim().toLowerCase();

export function selectFederalTaxLienFromDocument(doc: Document, valueToSelect = "Federal Tax Lien"): boolean {
  const selects = Array.from(doc.querySelectorAll("select")) as HTMLSelectElement[];

  const byNameOrId = selects.find((sel) => {
    const id = normalize(sel.id);
    const name = normalize(sel.name);
    return id.includes("filetype") || id.includes("file_type") || name.includes("filetype") || name.includes("file_type");
  });

  const withLabel = selects.find((sel) => {
    const id = sel.id;
    if (!id) return false;
    const label = doc.querySelector(`label[for="${id}"]`);
    return normalize(label?.textContent).includes("file type");
  });

  const byAria = selects.find((sel) => normalize(sel.getAttribute("aria-label")).includes("file type"));

  const fallback = byNameOrId ?? withLabel ?? byAria;
  if (!fallback) return false;

  const option = Array.from(fallback.options).find((opt) => normalize(opt.text).includes(normalize(valueToSelect)));
  if (!option) return false;

  const EventCtor = doc.defaultView?.Event ?? Event;
  fallback.value = option.value;
  fallback.dispatchEvent(new EventCtor("input", { bubbles: true }));
  fallback.dispatchEvent(new EventCtor("change", { bubbles: true }));
  return true;
}

export async function selectFederalTaxLienFileType(page: Page, log?: Logger): Promise<boolean> {
  const fileTypeSelectCandidates = [
    page.getByLabel(/file type/i),
    page.getByRole("combobox", { name: /file type/i }),
    page.locator('select[aria-label*="File Type" i]'),
    page.locator('select[name*="fileType" i], select[id*="fileType" i]'),
  ];

  for (const candidate of fileTypeSelectCandidates) {
    if ((await candidate.count()) === 0) continue;

    const control = candidate.first();
    try {
      await control.waitFor({ state: "visible", timeout: 3000 });
      await control.selectOption({ label: "Federal Tax Lien" });
      log?.({ stage: "file_type_selected", method: "locator" });
      return true;
    } catch {
      // try next selector variant
    }
  }

  const fileTypeSelected = await page.evaluate((value) => {
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
