// DOM-only helper for smoke/unit tests (e.g. src/tests/selector-smoke.ts).
// Do not import this module in production Playwright scrapers.
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
