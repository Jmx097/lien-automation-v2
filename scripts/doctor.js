const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

function runNode(args) {
  return execFileSync(process.execPath, args, { encoding: 'utf8' }).trim();
}

function runNpmVersion() {
  if (process.platform === 'win32') {
    const npmCli = path.join(path.dirname(process.execPath), 'node_modules', 'npm', 'bin', 'npm-cli.js');
    return runNode([npmCli, '-v']);
  }

  return execFileSync('npm', ['-v'], { encoding: 'utf8' }).trim();
}

function fail(message, hints = []) {
  console.error(message);
  for (const hint of hints) {
    console.error(hint);
  }
  process.exit(1);
}

function main() {
  const repoRoot = process.cwd();
  const pkgPath = path.join(repoRoot, 'package.json');

  console.log(`[doctor] repo: ${repoRoot}`);
  console.log(`[doctor] node: ${process.version}`);
  console.log(`[doctor] npm:  ${runNpmVersion()}`);

  const [major, minor] = process.versions.node.split('.').map((part) => Number.parseInt(part, 10));
  if (major < 20 || (major === 20 && minor < 16)) {
    fail('[doctor][error] Node >=20.16.0 required (recommended: 22 LTS).', [
      '[doctor][hint] Install Node 22 LTS or upgrade this machine to at least 20.16.0.',
    ]);
  }

  if (!fs.existsSync(pkgPath)) {
    fail('[doctor][error] package.json not found. Are you in repo root?');
  }

  const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
  const scripts = pkg.scripts ?? {};
  const requiredScripts = ['doctor', 'test:types', 'test:smoke'];
  const missingScripts = requiredScripts.filter((name) => !scripts[name]);
  if (missingScripts.length > 0) {
    fail(`[doctor][error] package.json scripts are outdated (missing ${missingScripts.join(', ')}).`, [
      '[doctor][hint] verify scripts with: npm run',
    ]);
  }

  const tscBin = path.join(repoRoot, 'node_modules', '.bin', process.platform === 'win32' ? 'tsc.cmd' : 'tsc');
  if (!fs.existsSync(tscBin)) {
    fail('[doctor][error] typescript binary missing (node_modules/.bin/tsc not found).', [
      '[doctor][hint] run: npm install',
    ]);
  }

  console.log('[doctor] OK: runtime, scripts, and local toolchain look good.');
}

main();
