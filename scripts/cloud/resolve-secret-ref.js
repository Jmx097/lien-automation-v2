#!/usr/bin/env node

const { execFileSync } = require('node:child_process');

const SECRET_RESOURCE_RE = /^projects\/([^/]+)\/secrets\/([^/]+)$/;
const VERSION_RESOURCE_RE = /^projects\/([^/]+)\/secrets\/([^/]+)\/versions\/([^/]+)$/;
const VERSION_COLLECTION_RESOURCE_RE = /^projects\/([^/]+)\/secrets\/([^/]+)\/versions$/;
const SIMPLE_SECRET_NAME_RE = /^[A-Za-z0-9_-]+$/;
const RESOURCE_MARKER_RE = /(projects|secrets|versions|secretmanager\.googleapis\.com)/i;
const ACCEPTED_FORMATS = [
  '<secret-name>',
  'projects/<project>/secrets/<name>',
  'projects/<project>/secrets/<name>/versions/<version>',
];

function stripOuterQuotes(value) {
  let next = String(value ?? '');

  while (
    (next.startsWith('"') && next.endsWith('"')) ||
    (next.startsWith("'") && next.endsWith("'"))
  ) {
    next = next.slice(1, -1).trim();
  }

  return next;
}

function tryExtractStringFromJson(value) {
  const trimmed = String(value ?? '').trim();
  if (!trimmed) {
    return trimmed;
  }

  try {
    const parsed = JSON.parse(trimmed);
    if (typeof parsed === 'string') {
      return parsed;
    }

    if (parsed && typeof parsed === 'object') {
      for (const key of ['secretRef', 'secret_ref', 'resource', 'name', 'ref', 'value']) {
        if (typeof parsed[key] === 'string') {
          return parsed[key];
        }
      }
    }
  } catch {
    return trimmed;
  }

  return trimmed;
}

function normalizeSecretRef(rawSecretRef) {
  return stripOuterQuotes(tryExtractStringFromJson(rawSecretRef))
    .replace(/[\r\n\t ]+/g, '')
    .replace(/\\\//g, '/')
    .replace(/\\/g, '/')
    .replace(/%2F/gi, '/')
    .replace(/^https:\/\/secretmanager\.googleapis\.com\//, '')
    .replace(/^\/\/secretmanager\.googleapis\.com\//, '')
    .replace(/^secretmanager\.googleapis\.com\//, '')
    .replace(/^\/+/, '')
    .replace(/\/+$/, '');
}

function containsJsonPunctuation(value) {
  return /[{}[\]":]/.test(value);
}

function buildSafeDiagnostics(rawSecretRef, normalizedRef, sourceKind = 'unknown') {
  const rawValue = String(rawSecretRef ?? '');
  const normalizedValue = String(normalizedRef ?? '');

  return {
    source_kind: sourceKind,
    normalized_ref_segments: normalizedValue ? normalizedValue.split('/').filter(Boolean).length : 0,
    contains_slash: normalizedValue.includes('/'),
    contains_backslash: rawValue.includes('\\'),
    contains_colon: normalizedValue.includes(':') || rawValue.includes(':'),
    contains_percent2f: /%2f/i.test(rawValue),
    contains_json_punctuation: containsJsonPunctuation(rawValue),
    contains_resource_markers: RESOURCE_MARKER_RE.test(rawValue) || RESOURCE_MARKER_RE.test(normalizedValue),
    accepted_formats: [
      ...ACCEPTED_FORMATS,
    ],
  };
}

function createMalformedRefError(rawSecretRef, normalizedRef) {
  const diagnostics = buildSafeDiagnostics(rawSecretRef, normalizedRef, 'malformed');
  return new Error(
    `Malformed secret ref. Supported formats are bare secret name, secret resource, or version resource. diagnostics=${JSON.stringify(
      diagnostics
    )}`
  );
}

function resolveSecretRef(rawSecretRef, defaultProject) {
  const normalizedRef = normalizeSecretRef(rawSecretRef);

  if (!normalizedRef) {
    throw new Error('SCHEDULE_RUN_TOKEN_SECRET_REF is empty');
  }

  const versionRefMatch = normalizedRef.match(VERSION_RESOURCE_RE);
  const versionCollectionMatch = normalizedRef.match(VERSION_COLLECTION_RESOURCE_RE);
  const secretRefMatch = normalizedRef.match(SECRET_RESOURCE_RE);

  let secretProject;
  let secretName;
  let secretVersion;
  let sourceKind;
  let accessRef = normalizedRef;

  if (versionRefMatch) {
    [, secretProject, secretName, secretVersion] = versionRefMatch;
    sourceKind = 'version-ref';
  } else if (versionCollectionMatch) {
    [, secretProject, secretName] = versionCollectionMatch;
    secretVersion = 'latest';
    sourceKind = 'version-collection-ref';
  } else if (secretRefMatch) {
    [, secretProject, secretName] = secretRefMatch;
    secretVersion = 'latest';
    sourceKind = 'secret-ref';
  } else if (SIMPLE_SECRET_NAME_RE.test(normalizedRef)) {
    secretProject = String(defaultProject ?? '').trim();
    secretName = normalizedRef;
    secretVersion = 'latest';
    sourceKind = 'bare-secret';
  } else if (RESOURCE_MARKER_RE.test(normalizedRef) || /[/:{}[\]"%]/.test(normalizedRef)) {
    throw createMalformedRefError(rawSecretRef, normalizedRef);
  } else {
    throw createMalformedRefError(rawSecretRef, normalizedRef);
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

function validateSecretRef(rawSecretRef = process.env.SCHEDULE_RUN_TOKEN_SECRET_REF, defaultProject = process.env.GCP_PROJECT_ID) {
  const resolved = resolveSecretRef(rawSecretRef, defaultProject);
  return {
    source_kind: resolved.sourceKind,
    normalized_ref_segments: resolved.segmentCount,
    positional_version_ref: resolved.positionalVersionRef,
    uses_default_project: resolved.sourceKind === 'bare-secret',
    accepted_formats: [
      ...ACCEPTED_FORMATS,
    ],
  };
}

function explainSecretRef(rawSecretRef = process.env.SCHEDULE_RUN_TOKEN_SECRET_REF, defaultProject = process.env.GCP_PROJECT_ID) {
  return validateSecretRef(rawSecretRef, defaultProject);
}

function resolveToken(
  rawSecretRef = process.env.SCHEDULE_RUN_TOKEN_SECRET_REF,
  defaultProject = process.env.GCP_PROJECT_ID
) {
  const resolved = resolveSecretRef(rawSecretRef, defaultProject);
  process.stderr.write(
    `Resolved scheduler token source=${resolved.sourceKind}; normalized_ref_segments=${resolved.segmentCount}\n`
  );

  return execFileSync('gcloud', buildAccessCommandArgs(resolved), { encoding: 'utf8' }).trimEnd();
}

if (require.main === module) {
  const mode = process.argv[2];

  if (mode === '--validate-only' || mode === '--explain') {
    process.stdout.write(`${JSON.stringify(explainSecretRef())}\n`);
  } else {
    process.stdout.write(resolveToken());
  }
}

module.exports = {
  buildAccessCommandArgs,
  buildSafeDiagnostics,
  createMalformedRefError,
  explainSecretRef,
  normalizeSecretRef,
  resolveSecretRef,
  resolveToken,
  validateSecretRef,
};
