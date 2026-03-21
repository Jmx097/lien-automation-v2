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

function extractVersionResource(normalizedRef) {
  const match = normalizedRef.match(/(projects\/[^/\s]+\/secrets\/[^/\s]+\/versions\/[^/\s"'`]+)/);
  return match ? match[1] : null;
}

function extractSecretResource(normalizedRef) {
  const match = normalizedRef.match(/(projects\/[^/\s]+\/secrets\/[^/\s"'`]+)/);
  return match ? match[1] : null;
}

function resolveSecretRef(rawSecretRef, defaultProject) {
  const normalizedRef = normalizeSecretRef(rawSecretRef);

  if (!normalizedRef) {
    throw new Error('SCHEDULE_RUN_TOKEN_SECRET_REF is empty');
  }

  const extractedVersionRef = extractVersionResource(normalizedRef);
  const extractedSecretRef = extractedVersionRef || extractSecretResource(normalizedRef);
  const versionRefMatch = extractedVersionRef?.match(
    /^projects\/([^/]+)\/secrets\/([^/]+)\/versions\/([^/]+)$/
  );
  const secretRefMatch = extractedSecretRef?.match(/^projects\/([^/]+)\/secrets\/([^/]+)$/);

  let secretProject;
  let secretName;
  let secretVersion;
  let sourceKind;
  let accessRef = normalizedRef;

  if (versionRefMatch) {
    [, secretProject, secretName, secretVersion] = versionRefMatch;
    sourceKind = 'version-ref';
    accessRef = extractedVersionRef;
  } else if (secretRefMatch) {
    [, secretProject, secretName] = secretRefMatch;
    secretVersion = 'latest';
    sourceKind = 'secret-ref';
    accessRef = extractedSecretRef;
  } else {
    secretProject = String(defaultProject ?? '').trim();
    secretName = normalizedRef;
    secretVersion = 'latest';
    sourceKind = 'bare-secret';
  }

  return {
    accessRef,
    normalizedRef,
    secretProject,
    secretName,
    secretVersion,
    sourceKind,
    segmentCount: accessRef.split('/').filter(Boolean).length,
    positionalVersionRef: Boolean(versionRefMatch),
  };
}

function buildAccessCommandArgs(resolved) {
  if (resolved.positionalVersionRef) {
    return ['secrets', 'versions', 'access', resolved.accessRef];
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
  extractSecretResource,
  extractVersionResource,
  normalizeSecretRef,
  resolveSecretRef,
  resolveToken,
};
