# Phase 2 Implementation Plan

Based on the MCP_GATE_PIPELINE_PLAN.md, here's what we need to implement for Phase 2:

## Goals
1. Update queue schema (status, chunk_id, etc.)
2. Implement chunk processor
3. Add checkpoint tracking
4. Update scraper to use chunks

## Key Files to Create/Modify

### 1. Database Schema Updates
- Update `src/queue/sqlite.ts` to add new columns:
  - `chunk_id TEXT`
  - `status TEXT DEFAULT 'pending'` (pending | in_progress | done | failed)
  - `error_code TEXT`
  - `retry_count INTEGER DEFAULT 0`
  - `last_attempt_at TIMESTAMP`
  - `completed_at TIMESTAMP`

### 2. Checkpoint Table
- Add new table for tracking checkpoints:
  ```sql
  CREATE TABLE checkpoints (
    id INTEGER PRIMARY KEY,
    last_processed_id INTEGER,
    last_processed_date TEXT,
    chunk_size INTEGER DEFAULT 25,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```

### 3. Run Summaries Table
- Add new table for run summaries:
  ```sql
  CREATE TABLE run_summaries (
    id INTEGER PRIMARY KEY,
    chunk_id TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    expected_count INTEGER,
    processed_count INTEGER,
    failed_count INTEGER,
    timeout_count INTEGER,
    summary_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
  ```

### 4. Chunk Processor Logic
- Enhance `src/scraper/chunk-processor.ts` with:
  - Chunk definition logic (count-based and date-window)
  - Integration with Gate 2 (chunk integrity)
  - Integration with Gate 3 (post-run verification)

### 5. Enhanced Scraper
- Update scraping logic to work with chunks instead of monolithic runs

## Implementation Steps

1. Create branch: `feature/phase2-chunking`
2. Update database schema in `src/queue/sqlite.ts`
3. Implement checkpoint tracking
4. Enhance chunk processor with integrity and verification gates
5. Update main scraper to use chunking
6. Add tests for new functionality
7. Update documentation
8. Create PR

## Commands to Run

```bash
# Create new branch
git checkout -b feature/phase2-chunking

# After implementation, commit changes
git add .
git commit -m "feat: Implement Phase 2 - Chunking Logic and Schema Updates"

# Push and create PR
git push -u origin feature/phase2-chunking
```