import { describe, expect, it, vi } from 'vitest';
import {
  classifyNYCAcrisFailure,
  classifyMaricopaFailure,
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

  it('classifies stale Maricopa session failures explicitly', () => {
    expect(classifyMaricopaFailure('Maricopa session is stale (captured_at=2026-03-10). Run refresh:maricopa-session on the droplet.'))
      .toBe('session_missing_or_stale');
  });

  it('classifies Maricopa Sheets quota failures as sheet_export', () => {
    expect(
      classifyMaricopaFailure("Error: Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute per user' of service 'sheets.googleapis.com'")
    ).toBe('sheet_export');
  });

  it('classifies NYC out-of-range result windows explicitly', () => {
    expect(
      classifyNYCAcrisFailure('ACRIS returned 10 rows outside requested range 03/08/2026-03/15/2026 upstream_range=03/04/2026-03/06/2026')
    ).toBe('range_result_integrity');
  });

  it('classifies blank bootstrap failures separately from selector issues', () => {
    expect(
      classifyNYCAcrisFailure(
        'NYC probe_index_page page not ready: {"step":"probe_index_page","attempt":2,"kind":"index","expectedPath":"/DS/DocumentSearch/Index","finalUrl":"about:blank","title":"","readyState":"unavailable","htmlLength":0,"bodyTextLength":0,"hasToken":false,"hasShellMarker":false,"hasResultMarker":false,"hasViewerIframe":false,"ok":false,"reason":"unexpected_url"}'
      )
    ).toBe('transport_or_bootstrap');
  });

  it('blocks Maricopa immediately when persisted session state is missing', () => {
    const result = recordConnectivityFailure(
      createDefaultConnectivityState('maricopa_recorder'),
      'Maricopa session is missing. Run refresh:maricopa-session on the droplet.',
      'session_missing_or_stale',
      new Date('2026-03-09T10:00:00.000Z'),
    );

    expect(result.state.status).toBe('blocked');
    expect(result.becameBlocked).toBe(true);
  });

  it('recovers Maricopa after probe success from a blocked state', () => {
    vi.stubEnv('MARICOPA_RECORDER_PROBE_SUCCESSES_REQUIRED', '1');
    const blocked = {
      ...createDefaultConnectivityState('maricopa_recorder'),
      status: 'blocked' as const,
      opened_at: '2026-03-09T10:00:00.000Z',
      next_probe_at: '2026-03-09T10:15:00.000Z',
      policy_block_count: 2,
    };

    const outcome = recordConnectivitySuccess(blocked, 'probe', new Date('2026-03-09T10:15:00.000Z'));

    expect(outcome.state.status).toBe('healthy');
    expect(outcome.recovered).toBe(true);
    vi.unstubAllEnvs();
  });
});
