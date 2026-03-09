import { describe, expect, it, vi } from 'vitest';
import {
  createDefaultConnectivityState,
  recordConnectivityFailure,
  recordConnectivitySuccess,
} from '../../src/scheduler/connectivity';

describe('scheduler connectivity state', () => {
  it('opens the circuit after two policy blocks within the failure window', () => {
    const start = new Date('2026-03-09T10:00:00.000Z');
    const first = recordConnectivityFailure(
      createDefaultConnectivityState('nyc_acris'),
      'robots.txt brob',
      'policy_block',
      start,
    ).state;
    const second = recordConnectivityFailure(
      first,
      'robots.txt brob',
      'policy_block',
      new Date('2026-03-09T10:05:00.000Z'),
    );

    expect(second.state.status).toBe('blocked');
    expect(second.becameBlocked).toBe(true);
  });

  it('opens the circuit after three transient failures within the failure window', () => {
    let state = createDefaultConnectivityState('nyc_acris');
    state = recordConnectivityFailure(state, 'timeout one', 'timeout_or_navigation', new Date('2026-03-09T10:00:00.000Z')).state;
    state = recordConnectivityFailure(state, 'timeout two', 'timeout_or_navigation', new Date('2026-03-09T10:03:00.000Z')).state;
    const third = recordConnectivityFailure(state, 'timeout three', 'timeout_or_navigation', new Date('2026-03-09T10:06:00.000Z'));

    expect(third.state.status).toBe('blocked');
    expect(third.becameBlocked).toBe(true);
  });

  it('requires two successful probes to recover from a blocked state', () => {
    const blocked = {
      ...createDefaultConnectivityState('nyc_acris'),
      status: 'blocked' as const,
      opened_at: '2026-03-09T10:00:00.000Z',
      next_probe_at: '2026-03-09T10:15:00.000Z',
      policy_block_count: 2,
    };

    const first = recordConnectivitySuccess(blocked, 'probe', new Date('2026-03-09T10:15:00.000Z'));
    const second = recordConnectivitySuccess(first.state, 'probe', new Date('2026-03-09T10:30:00.000Z'));

    expect(first.state.status).toBe('probing');
    expect(second.state.status).toBe('healthy');
    expect(second.recovered).toBe(true);
  });

  it('degrades after repeated empty result failures without blocking immediately', () => {
    const first = recordConnectivityFailure(
      createDefaultConnectivityState('nyc_acris'),
      'No ACRIS rows found during live selector validation',
      'selector_or_empty_results',
      new Date('2026-03-09T10:00:00.000Z'),
    ).state;
    const second = recordConnectivityFailure(
      first,
      'No rows found on result page 1',
      'selector_or_empty_results',
      new Date('2026-03-09T10:05:00.000Z'),
    );

    expect(second.state.status).toBe('degraded');
    expect(second.becameBlocked).toBe(false);
  });
});
