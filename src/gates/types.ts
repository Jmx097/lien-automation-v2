// src/gates/types.ts
export interface GateResult {
  success: boolean;
  errors?: string[];
}

export interface ChunkIntegrityResult extends GateResult {
  partial?: boolean;
  missing_ids?: string[];
}

export interface PostRunVerifyResult extends GateResult {
  summary_path?: string;
  checkpoint_updated?: boolean;
}

export interface RunSummary {
  chunk_id: string;
  start_time: string;
  end_time: string;
  elapsed_seconds: number;
  expected_count: number;
  processed_count: number;
  failed_count: number;
  timeout_count: number;
  error_breakdown: {
    timeout: number;
    '403_forbidden': number;
    '429_rate_limit': number;
    selector_fail: number;
    network_error: number;
  };
  checkpoint_updated: boolean;
  gate_results: {
    pre_run_health: GateResult;
    chunk_integrity: ChunkIntegrityResult;
    post_run_verify: PostRunVerifyResult;
  };
}