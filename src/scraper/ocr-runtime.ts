import { spawnSync } from 'child_process';

export interface OCRRuntimeCheckResult {
  ok: boolean;
  missing: string[];
  detail?: string;
}

function commandExists(cmd: string): boolean {
  const checker = process.platform === 'win32' ? 'where' : 'command';
  const args = process.platform === 'win32' ? [cmd] : ['-v', cmd];
  const result = spawnSync(checker, args, { stdio: 'ignore', shell: process.platform !== 'win32' });
  return result.status === 0;
}

function smokeTestBinary(cmd: string, args: string[]): boolean {
  const result = spawnSync(cmd, args, { stdio: 'ignore' });
  return result.status === 0;
}

export function checkOCRRuntime(): OCRRuntimeCheckResult {
  const missing: string[] = [];

  if (!commandExists('tesseract')) {
    missing.push('tesseract');
  }

  if (!commandExists('pdftoppm')) {
    missing.push('pdftoppm');
  }

  if (missing.length > 0) {
    return {
      ok: false,
      missing,
      detail: `Missing OCR binaries: ${missing.join(', ')}`,
    };
  }

  const tesseractOk = smokeTestBinary('tesseract', ['--version']);
  const pdftoppmOk = smokeTestBinary('pdftoppm', ['-v']);

  if (!tesseractOk || !pdftoppmOk) {
    const broken: string[] = [];
    if (!tesseractOk) broken.push('tesseract');
    if (!pdftoppmOk) broken.push('pdftoppm');

    return {
      ok: false,
      missing: broken,
      detail: `OCR binaries installed but not functional: ${broken.join(', ')}`,
    };
  }

  return { ok: true, missing: [] };
}
