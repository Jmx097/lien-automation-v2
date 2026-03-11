import { describe, expect, it } from 'vitest';
import { checkOCRRuntime, getOCRBinaryCommands } from '../../src/scraper/ocr-runtime';

describe('ocr runtime', () => {
  it('uses explicit binary path overrides when provided', () => {
    const result = checkOCRRuntime({
      env: {
        ...process.env,
        TESSERACT_PATH: 'C:\\OCR\\tesseract.exe',
        PDFTOPPM_PATH: 'C:\\Poppler\\pdftoppm.exe',
      },
      commandExists: (cmd) => /ocr|poppler/i.test(cmd),
      smokeTest: (cmd, args) =>
        (cmd === 'C:\\OCR\\tesseract.exe' && args.join(' ') === '--version') ||
        (cmd === 'C:\\Poppler\\pdftoppm.exe' && args.join(' ') === '-v'),
    });

    expect(result).toMatchObject({
      ok: true,
      missing: [],
      commands: {
        tesseract: 'C:\\OCR\\tesseract.exe',
        pdftoppm: 'C:\\Poppler\\pdftoppm.exe',
      },
    });
  });

  it('falls back to PATH commands when overrides are absent', () => {
    const result = checkOCRRuntime({
      env: {},
      commandExists: (cmd) => cmd === 'tesseract' || cmd === 'pdftoppm',
      smokeTest: () => true,
    });

    expect(result).toMatchObject({
      ok: true,
      missing: [],
      commands: {
        tesseract: 'tesseract',
        pdftoppm: 'pdftoppm',
      },
    });
  });

  it('reports missing binaries when an override path is invalid', () => {
    const result = checkOCRRuntime({
      env: {
        TESSERACT_PATH: 'C:\\missing\\tesseract.exe',
      },
      commandExists: (cmd) => cmd === 'pdftoppm',
      smokeTest: () => true,
    });

    expect(result.ok).toBe(false);
    expect(result.missing).toContain('tesseract');
    expect(result.commands?.tesseract).toBe('C:\\missing\\tesseract.exe');
  });

  it('exposes the resolved OCR binary commands', () => {
    expect(
      getOCRBinaryCommands({
        TESSERACT_PATH: 'C:\\OCR\\tesseract.exe',
        PDFTOPPM_PATH: '',
      })
    ).toEqual({
      tesseract: 'C:\\OCR\\tesseract.exe',
      pdftoppm: 'pdftoppm',
    });
  });
});
