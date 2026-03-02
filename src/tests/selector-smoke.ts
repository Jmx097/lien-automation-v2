import { JSDOM } from "jsdom";
import { selectFederalTaxLienFromDocument } from "../scraper/file_type_selector";

type Fixture = {
  name: string;
  html: string;
};

const fixtures: Fixture[] = [
  {
    name: "proper combobox",
    html: `
      <label for="fileType">File Type</label>
      <select id="fileType" aria-label="File Type" role="combobox">
        <option value="a">Amendment</option>
        <option value="f">Federal Tax Lien</option>
      </select>
    `,
  },
  {
    name: "plain select with label",
    html: `
      <label for="search-file">File Type</label>
      <select id="search-file">
        <option value="u">UCC Financing Statement</option>
        <option value="f">Federal Tax Lien</option>
      </select>
    `,
  },
  {
    name: "select requiring DOM fallback",
    html: `
      <select aria-label="File Type Filter" style="display:none">
        <option value="u">UCC Financing Statement</option>
        <option value="f">Federal Tax Lien</option>
      </select>
    `,
  },
];

for (const fixture of fixtures) {
  const dom = new JSDOM(fixture.html);
  const selected = selectFederalTaxLienFromDocument(dom.window.document);
  if (!selected) {
    throw new Error(`[${fixture.name}] helper returned false`);
  }

  const select = dom.window.document.querySelector("select") as HTMLSelectElement | null;
  const option = select?.selectedOptions[0]?.textContent?.trim();
  if (option !== "Federal Tax Lien") {
    throw new Error(`[${fixture.name}] expected selected option 'Federal Tax Lien', got '${option}'`);
  }

  console.log(`PASS: ${fixture.name}`);
}

console.log("Selector smoke test passed for all fixtures.");
