import { describe, expect, it } from 'vitest';

const {
  buildAccessCommandArgs,
  normalizeSecretRef,
  resolveSecretRef,
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
      normalizedRef: 'projects/demo-project/secrets/scheduler-token/versions/5',
      secretProject: 'demo-project',
      secretName: 'scheduler-token',
      secretVersion: '5',
      sourceKind: 'version-ref',
      segmentCount: 6,
    });
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
    });
  });
});
