#!/usr/bin/env node

const { execFileSync } = require('node:child_process');

function stripOuterQuotes(value) {
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1).trim();
  }
  return value;
}

function normalizeSecretRef(rawSecretRef) {
  return stripOuterQuotes(String(rawSecretRef ?? '').replace(/[\r\n]/g, '').trim())
    .replace(/^https:\/\/secretmanager\.googleapis\.com\//, '')
    .replace(/^\/\/secretmanager\.googleapis\.com\//, '')
    .replace(/^secretmanager\.googleapis\.com\//, '')
    .replace(/^\/+/, '');
}

function resolveSecretRef(rawSecretRef, defaultProject) {
  const normalizedRef = normalizeSecretRef(rawSecretRef);

  if (!normalizedRef) {
    throw new Error('SCHEDULE_RUN_TOKEN_SECRET_REF is empty');
  }

  const versionRefMatch = normalizedRef.match(
    /^projects\/([^/]+)\/secrets\/([^/]+)\/versions\/([^/]+)$/
  );
  const secretRefMatch = normalizedRef.match(/^projects\/([^/]+)\/secrets\/([^/]+)$/);

  let secretProject;
  let secretName;
  let secretVersion;
  let sourceKind;

  if (versionRefMatch) {
    [, secretProject, secretName, secretVersion] = versionRefMatch;
    sourceKind = 'version-ref';
  } else if (secretRefMatch) {
    [, secretProject, secretName] = secretRefMatch;
    secretVersion = 'latest';
    sourceKind = 'secret-ref';
  } else {
    secretProject = String(defaultProject ?? '').trim();
    secretName = normalizedRef;
    secretVersion = 'latest';
    sourceKind = 'bare-secret';
  }

  return {
    normalizedRef,
    secretProject,
    secretName,
    secretVersion,
    sourceKind,
    segmentCount: normalizedRef.split('/').filter(Boolean).length,
    positionalVersionRef: Boolean(versionRefMatch),
  };
}

function buildAccessCommandArgs(resolved) {
  if (resolved.positionalVersionRef) {
    return ['secrets', 'versions', 'access', resolved.normalizedRef];
  }

  return [
    'secrets',
    'versions',
    'access',
    resolved.secretVersion,
    '--secret',
    resolved.secretName,
    '--project',
    resolved.secretProject,
  ];
}

function resolveToken(rawSecretRef = process.env.SCHEDULE_RUN_TOKEN_SECRET_REF, defaultProject = process.env.GCP_PROJECT_ID) {
  const resolved = resolveSecretRef(rawSecretRef, defaultProject);
  process.stderr.write(
    `Resolved scheduler token source=${resolved.sourceKind}; normalized_ref_segments=${resolved.segmentCount}\n`
  );

  return execFileSync('gcloud', buildAccessCommandArgs(resolved), { encoding: 'utf8' }).trimEnd();
}

if (require.main === module) {
  process.stdout.write(resolveToken());
}

module.exports = {
  buildAccessCommandArgs,
  normalizeSecretRef,
  resolveSecretRef,
  resolveToken,
};
