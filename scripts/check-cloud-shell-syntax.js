#!/usr/bin/env node

const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const repoRoot = path.resolve(__dirname, '..');
const scriptsDir = path.join(repoRoot, 'scripts', 'cloud');

const shellScripts = fs
  .readdirSync(scriptsDir)
  .filter((entry) => entry.endsWith('.sh'))
  .map((entry) => path.join(scriptsDir, entry));

const bashCandidates = process.platform === 'win32'
  ? [
      process.env.BASH,
      'C:\\Program Files\\Git\\bin\\bash.exe',
      'C:\\Program Files\\Git\\usr\\bin\\bash.exe',
      'bash',
    ].filter(Boolean)
  : [process.env.BASH, 'bash'].filter(Boolean);

let lastFailure = null;

for (const candidate of bashCandidates) {
  const result = spawnSync(candidate, ['-n', ...shellScripts], {
    stdio: 'inherit',
    shell: false,
  });

  if (!result.error && result.status === 0) {
    process.exit(0);
  }

  lastFailure = result.error ?? new Error(`bash exited with status ${result.status ?? 'unknown'}`);
}

throw new Error(`Unable to validate cloud shell scripts with bash. Last failure: ${lastFailure?.message ?? 'unknown error'}`);
