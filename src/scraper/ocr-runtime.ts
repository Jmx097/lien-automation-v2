import fs from 'fs';
import { spawnSync } from 'child_process';

export interface OCRRuntimeCheckResult {
  ok: boolean;
  missing: string[];
  detail?: string;
  commands?: OCRRuntimeBinaryCommands;
}

export interface OCRRuntimeBinaryCommands {
  tesseract: string;
  pdftoppm: string;
}

type OCRBinaryName = keyof OCRRuntimeBinaryCommands;

interface OCRRuntimeCheckOptions {
  env?: NodeJS.ProcessEnv;
  commandExists?: (cmd: string) => boolean;
  smokeTest?: (cmd: string, args: string[]) => boolean;
}

const OCR_BINARY_ENV_VARS: Record<OCRBinaryName, string> = {
  tesseract: 'TESSERACT_PATH',
  pdftoppm: 'PDFTOPPM_PATH',
};

function commandExists(cmd: string): boolean {
  if (!cmd) return false;
  if (/[\\/]/.test(cmd) || /^[A-Za-z]:/.test(cmd)) {
    return fs.existsSync(cmd);
  }
  const checker = process.platform === 'win32' ? 'where' : 'command';
  const args = process.platform === 'win32' ? [cmd] : ['-v', cmd];
  const result = spawnSync(checker, args, { stdio: 'ignore', shell: process.platform !== 'win32' });
  return result.status === 0;
}

function smokeTestBinary(cmd: string, args: string[]): boolean {
  const result = spawnSync(cmd, args, { stdio: 'ignore' });
  return result.status === 0;
}

export function getOCRBinaryCommands(env: NodeJS.ProcessEnv = process.env): OCRRuntimeBinaryCommands {
  return {
    tesseract: env.TESSERACT_PATH?.trim() || 'tesseract',
    pdftoppm: env.PDFTOPPM_PATH?.trim() || 'pdftoppm',
  };
}

export function checkOCRRuntime(options: OCRRuntimeCheckOptions = {}): OCRRuntimeCheckResult {
  const env = options.env ?? process.env;
  const exists = options.commandExists ?? commandExists;
  const smokeTest = options.smokeTest ?? smokeTestBinary;
  const commands = getOCRBinaryCommands(env);
  const missing: string[] = [];

  if (!exists(commands.tesseract)) {
    missing.push('tesseract');
  }

  if (!exists(commands.pdftoppm)) {
    missing.push('pdftoppm');
  }

  if (missing.length > 0) {
    return {
      ok: false,
      missing,
      detail: `Missing OCR binaries: ${missing.join(', ')}`,
      commands,
    };
  }

  const tesseractOk = smokeTest(commands.tesseract, ['--version']);
  const pdftoppmOk = smokeTest(commands.pdftoppm, ['-v']);

  if (!tesseractOk || !pdftoppmOk) {
    const broken: string[] = [];
    if (!tesseractOk) broken.push('tesseract');
    if (!pdftoppmOk) broken.push('pdftoppm');

    return {
      ok: false,
      missing: broken,
      detail: `OCR binaries installed but not functional: ${broken.join(', ')}`,
      commands,
    };
  }

  return { ok: true, missing: [], commands };
}
