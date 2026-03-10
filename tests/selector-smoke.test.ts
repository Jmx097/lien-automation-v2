import { describe, expect, it } from 'vitest';
import { selectFederalTaxLienFromDocument } from '../src/scraper/file_type_selector';

type MockOption = {
  value: string;
  text: string;
};

type MockSelect = {
  id: string;
  name: string;
  options: MockOption[];
  value: string;
  selectedOptions: Array<{ textContent: string }>;
  getAttribute(name: string): string | null;
  dispatchEvent(_event: Event): boolean;
};

type MockLabel = {
  textContent: string;
};

function createMockDocument(config: {
  selectId?: string;
  selectName?: string;
  ariaLabel?: string;
  labelFor?: string;
  labelText?: string;
}): Document {
  const options: MockOption[] = [
    { value: 'u', text: 'UCC Financing Statement' },
    { value: 'f', text: 'Federal Tax Lien' },
  ];

  const select: MockSelect = {
    id: config.selectId ?? '',
    name: config.selectName ?? '',
    options,
    value: options[0].value,
    selectedOptions: [{ textContent: options[0].text }],
    getAttribute(name: string) {
      if (name === 'aria-label') return config.ariaLabel ?? null;
      return null;
    },
    dispatchEvent() {
      const selected = options.find((option) => option.value === select.value) ?? options[0];
      select.selectedOptions = [{ textContent: selected.text }];
      return true;
    },
  };

  const label: MockLabel | null = config.labelFor
    ? { textContent: config.labelText ?? '' }
    : null;

  const documentLike = {
    defaultView: { Event },
    querySelectorAll(selector: string) {
      if (selector === 'select') return [select];
      return [];
    },
    querySelector(selector: string) {
      if (!label || !config.labelFor) return null;
      const match = selector.match(/^label\[for="(.+)"\]$/);
      if (match && match[1] === config.labelFor) return label;
      return null;
    },
  };

  return documentLike as unknown as Document;
}

describe('selector smoke', () => {
  it('selects Federal Tax Lien using a file type id', () => {
    const doc = createMockDocument({ selectId: 'fileType', ariaLabel: 'File Type' });
    expect(selectFederalTaxLienFromDocument(doc)).toBe(true);
  });

  it('selects Federal Tax Lien using an associated label', () => {
    const doc = createMockDocument({ selectId: 'search-file', labelFor: 'search-file', labelText: 'File Type' });
    expect(selectFederalTaxLienFromDocument(doc)).toBe(true);
  });

  it('selects Federal Tax Lien using aria-label fallback', () => {
    const doc = createMockDocument({ ariaLabel: 'File Type Filter' });
    expect(selectFederalTaxLienFromDocument(doc)).toBe(true);
  });
});
