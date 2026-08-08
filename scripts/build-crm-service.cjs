#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const ts = require('typescript');

const root = path.resolve(__dirname, '..');
const outputRoot = path.join(root, 'dist-crm');
const files = [
  'src/crm-server.ts',
  'src/crm/auth.ts',
  'src/crm/index.ts',
  'src/crm/repository.ts',
  'src/crm/router.ts',
  'src/crm/service.ts',
  'src/crm/types.ts',
];

fs.rmSync(outputRoot, { recursive: true, force: true });
for (const relativePath of files) {
  const sourcePath = path.join(root, relativePath);
  const outputPath = path.join(outputRoot, relativePath.replace(/\.ts$/, '.js'));
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  const result = ts.transpileModule(fs.readFileSync(sourcePath, 'utf8'), {
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.CommonJS,
      moduleResolution: ts.ModuleResolutionKind.NodeJs,
      esModuleInterop: true,
    },
    fileName: sourcePath,
    reportDiagnostics: true,
  });
  const diagnostics = result.diagnostics?.filter((diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error) ?? [];
  if (diagnostics.length) {
    throw new Error(ts.formatDiagnosticsWithColorAndContext(diagnostics, {
      getCanonicalFileName: (name) => name,
      getCurrentDirectory: () => root,
      getNewLine: () => '\n',
    }));
  }
  fs.writeFileSync(outputPath, result.outputText, { mode: 0o644 });
}

const migrationSource = path.join(root, 'src/crm/migrations/001_initial_schema.sql');
const migrationDestination = path.join(outputRoot, 'src/crm/migrations/001_initial_schema.sql');
fs.mkdirSync(path.dirname(migrationDestination), { recursive: true });
fs.copyFileSync(migrationSource, migrationDestination);
fs.writeFileSync(path.join(outputRoot, 'package.json'), JSON.stringify({
  name: 'plinko-crm-service',
  private: true,
  version: '1.0.0',
  scripts: { start: 'node src/crm-server.js' },
  dependencies: { express: '5.2.1', pg: '8.20.0' },
}, null, 2) + '\n', { mode: 0o644 });
console.log(`Built isolated CRM service bundle at ${outputRoot}`);
