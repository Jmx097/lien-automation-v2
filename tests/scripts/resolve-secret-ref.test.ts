import { describe, expect, it } from 'vitest';
import { execFileSync } from 'node:child_process';
import path from 'node:path';

const scriptPath = path.resolve(__dirname, '../../scripts/cloud/resolve-secret-ref.js');
const {
  buildAccessCommandArgs,
  normalizeSecretRef,
  resolveSecretRef,
  validateSecretRef,
} = require('../../scripts/cloud/resolve-secret-ref.js');

describe('resolve-secret-ref', () => {
  it('normalizes a bare secret name wrapped in quotes', () => {
    expect(normalizeSecretRef("'scheduler-token'\n")).toBe('scheduler-token');
  });

  it('resolves a bare secret name against the default project', () => {
    const resolved = resolveSecretRef('scheduler-token', 'project-123');

    expect(resolved).toMatchObject({
      normalizedRef: 'scheduler-token',
      secretProject: 'project-123',
      secretName: 'scheduler-token',
      secretVersion: 'latest',
      sourceKind: 'bare-secret',
      segmentCount: 1,
    });
    expect(buildAccessCommandArgs(resolved)).toEqual([
      'secrets',
      'versions',
      'access',
      'latest',
      '--secret',
      'scheduler-token',
      '--project',
      'project-123',
    ]);
  });

  it('resolves a secret resource ref', () => {
    const resolved = resolveSecretRef('projects/demo-project/secrets/scheduler-token', 'ignored-project');

    expect(resolved).toMatchObject({
      normalizedRef: 'projects/demo-project/secrets/scheduler-token',
      secretProject: 'demo-project',
      secretName: 'scheduler-token',
      secretVersion: 'latest',
      sourceKind: 'secret-ref',
      segmentCount: 4,
    });
  });

  it('resolves a version resource ref', () => {
    const resolved = resolveSecretRef(
      'projects/demo-project/secrets/scheduler-token/versions/5',
      'ignored-project'
    );

    expect(resolved).toMatchObject({
      accessRef: 'projects/demo-project/secrets/scheduler-token/versions/5',
      normalizedRef: 'projects/demo-project/secrets/scheduler-token/versions/5',
      secretProject: 'demo-project',
      secretName: 'scheduler-token',
      secretVersion: '5',
      sourceKind: 'version-ref',
      segmentCount: 6,
      positionalVersionRef: true,
    });
    expect(buildAccessCommandArgs(resolved)).toEqual([
      'secrets',
      'versions',
      'access',
      'projects/demo-project/secrets/scheduler-token/versions/5',
    ]);
  });


  it('resolves a version collection resource ref to latest', () => {
    const resolved = resolveSecretRef(
      'projects/demo-project/secrets/scheduler-token/versions',
      'ignored-project'
    );

    expect(resolved).toMatchObject({
      normalizedRef: 'projects/demo-project/secrets/scheduler-token/versions',
      secretProject: 'demo-project',
      secretName: 'scheduler-token',
      secretVersion: 'latest',
      sourceKind: 'version-collection-ref',
      segmentCount: 5,
      positionalVersionRef: false,
    });
    expect(buildAccessCommandArgs(resolved)).toEqual([
      'secrets',
      'versions',
      'access',
      'latest',
      '--secret',
      'scheduler-token',
      '--project',
      'demo-project',
    ]);
  });

  it('normalizes a secretmanager.googleapis.com ref', () => {
    const resolved = resolveSecretRef(
      'https://secretmanager.googleapis.com/projects/demo-project/secrets/scheduler-token/versions/latest',
      'ignored-project'
    );

    expect(resolved).toMatchObject({
      normalizedRef: 'projects/demo-project/secrets/scheduler-token/versions/latest',
      secretProject: 'demo-project',
      secretName: 'scheduler-token',
      secretVersion: 'latest',
      sourceKind: 'version-ref',
      segmentCount: 6,
      positionalVersionRef: true,
    });
  });


  it('normalizes trailing slash refs', () => {
    const resolved = resolveSecretRef(
      'projects/demo-project/secrets/scheduler-token/versions/latest/',
      'ignored-project'
    );

    expect(resolved).toMatchObject({
      normalizedRef: 'projects/demo-project/secrets/scheduler-token/versions/latest',
      sourceKind: 'version-ref',
      secretVersion: 'latest',
    });
  });

  it('normalizes escaped slash refs', () => {
    const resolved = resolveSecretRef(
      'projects\\/demo-project\\/secrets\\/scheduler-token\\/versions\\/latest',
      'ignored-project'
    );

    expect(resolved).toMatchObject({
      normalizedRef: 'projects/demo-project/secrets/scheduler-token/versions/latest',
      sourceKind: 'version-ref',
      secretProject: 'demo-project',
      secretName: 'scheduler-token',
      secretVersion: 'latest',
    });
  });

  it('normalizes percent-encoded slash refs', () => {
    const resolved = resolveSecretRef(
      'projects%2Fdemo-project%2Fsecrets%2Fscheduler-token%2Fversions%2Flatest',
      'ignored-project'
    );

    expect(resolved).toMatchObject({
      normalizedRef: 'projects/demo-project/secrets/scheduler-token/versions/latest',
      sourceKind: 'version-ref',
    });
  });

  it('extracts refs from JSON-wrapped strings', () => {
    const resolved = resolveSecretRef(
      '{"secretRef":"projects/demo-project/secrets/scheduler-token/versions/latest"}',
      'ignored-project'
    );

    expect(resolved).toMatchObject({
      normalizedRef: 'projects/demo-project/secrets/scheduler-token/versions/latest',
      sourceKind: 'version-ref',
      secretProject: 'demo-project',
    });
  });

  it('extracts refs from alternate JSON key names', () => {
    const resolved = resolveSecretRef(
      '{"secret_resource":"projects/demo-project/secrets/scheduler-token/versions/latest"}',
      'ignored-project'
    );

    expect(resolved).toMatchObject({
      normalizedRef: 'projects/demo-project/secrets/scheduler-token/versions/latest',
      sourceKind: 'version-ref',
      secretProject: 'demo-project',
    });
  });

  it('extracts a bare secret name from a single-key JSON wrapper', () => {
    const resolved = resolveSecretRef('{"scheduler":"scheduler-token"}', 'project-123');

    expect(resolved).toMatchObject({
      normalizedRef: 'scheduler-token',
      sourceKind: 'bare-secret',
      secretProject: 'project-123',
      secretName: 'scheduler-token',
    });
  });

  it('prefers a recognized JSON secret key when other string metadata is present', () => {
    const resolved = resolveSecretRef(
      '{"kind":"secret-manager","secretName":"scheduler-token"}',
      'project-123'
    );

    expect(resolved).toMatchObject({
      normalizedRef: 'scheduler-token',
      sourceKind: 'bare-secret',
      secretProject: 'project-123',
      secretName: 'scheduler-token',
    });
  });

  it('extracts a nested recognized JSON secret ref', () => {
    const resolved = resolveSecretRef(
      '{"secret":{"name":"scheduler-token"},"kind":"secret-manager"}',
      'project-123'
    );

    expect(resolved).toMatchObject({
      normalizedRef: 'scheduler-token',
      sourceKind: 'bare-secret',
      secretProject: 'project-123',
      secretName: 'scheduler-token',
    });
  });

  it('extracts a nested recognized JSON secret resource', () => {
    const resolved = resolveSecretRef(
      '{"config":{"secret_resource":"projects/demo-project/secrets/scheduler-token/versions/latest"}}',
      'ignored-project'
    );

    expect(resolved).toMatchObject({
      normalizedRef: 'projects/demo-project/secrets/scheduler-token/versions/latest',
      sourceKind: 'version-ref',
      secretProject: 'demo-project',
    });
  });

  it('fails ambiguous JSON-wrapped refs with multiple candidate strings', () => {
    expect(() =>
      resolveSecretRef(
        '{"primary":"scheduler-token","secondary":"projects/demo-project/secrets/other-token"}',
        'project-123'
      )
    ).toThrow(/Ambiguous JSON-wrapped secret ref/);
  });

  it('fails malformed resource-like refs instead of falling back to bare-secret', () => {
    expect(() =>
      resolveSecretRef('resource=projects_demo_project_secrets_scheduler-token_versions_latest', 'project-123')
    ).toThrow(/Malformed secret ref/);
  });

  it('fails malformed project resource refs with safe diagnostics', () => {
    expect(() =>
      resolveSecretRef('projects/demo-project/secrets/scheduler-token/versions/not/a/version', 'project-123')
    ).toThrow(/accepted_formats/);
  });

  it('returns validation metadata without exposing the secret ref', () => {
    expect(validateSecretRef('scheduler-token', 'project-123')).toEqual({
      source_kind: 'bare-secret',
      normalized_ref_segments: 1,
      positional_version_ref: false,
      uses_default_project: true,
      accepted_formats: [
        '<secret-name>',
        'projects/<project>/secrets/<name>',
        'projects/<project>/secrets/<name>/versions/<version>',
      ],
    });
  });

  it('supports validate-only mode for valid refs', () => {
    const output = execFileSync(
      'node',
      [scriptPath, '--validate-only'],
      {
        encoding: 'utf8',
        env: {
          ...process.env,
          SCHEDULE_RUN_TOKEN_SECRET_REF: 'scheduler-token',
          GCP_PROJECT_ID: 'project-123',
        },
      }
    ).trim();

    expect(JSON.parse(output)).toEqual({
      source_kind: 'bare-secret',
      normalized_ref_segments: 1,
      positional_version_ref: false,
      uses_default_project: true,
      accepted_formats: [
        '<secret-name>',
        'projects/<project>/secrets/<name>',
        'projects/<project>/secrets/<name>/versions/<version>',
      ],
    });
    expect(output).not.toContain('scheduler-token');
  });

  it('fails validate-only mode for malformed refs', () => {
    expect(() =>
      execFileSync('node', [scriptPath, '--validate-only'], {
        encoding: 'utf8',
        env: {
          ...process.env,
          SCHEDULE_RUN_TOKEN_SECRET_REF: 'projects/demo-project/secrets/scheduler-token/versions/not/a/version',
          GCP_PROJECT_ID: 'project-123',
        },
      })
    ).toThrow(/Malformed secret ref/);
  });

  it('fails validate-only mode for ambiguous JSON refs', () => {
    expect(() =>
      execFileSync('node', [scriptPath, '--validate-only'], {
        encoding: 'utf8',
        env: {
          ...process.env,
          SCHEDULE_RUN_TOKEN_SECRET_REF: '{"primary":"scheduler-token","secondary":"other-token"}',
          GCP_PROJECT_ID: 'project-123',
        },
      })
    ).toThrow(/Ambiguous JSON-wrapped secret ref/);
  });
});
