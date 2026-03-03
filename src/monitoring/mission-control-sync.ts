type StageStatus =
  | 'planning'
  | 'inbox'
  | 'assigned'
  | 'in_progress'
  | 'testing'
  | 'review'
  | 'done';

type LogEvent = Record<string, unknown> & {
  timestamp?: string;
  stage?: string;
  site?: string;
  date_start?: string;
  date_end?: string;
  error?: string;
  duration_seconds?: number;
  records?: number;
  records_enqueued?: number;
  total_pending?: number;
  job_id?: number;
  file_number?: string;
  attempt?: number;
  token_input?: number;
  token_output?: number;
  token_total?: number;
  estimated_cost_usd?: number;
};

type WorkspaceResponse = {
  id: string;
  slug: string;
  name: string;
};

type TaskResponse = {
  id: string;
  title: string;
};

const SYNC_ENABLED =
  (process.env.MISSION_CONTROL_SYNC_ENABLED || 'true').toLowerCase() === 'true';
const MC_BASE_URL = (process.env.MISSION_CONTROL_URL || 'http://127.0.0.1:4000').replace(/\/+$/, '');
const RUN_WORKSPACE_SLUG = process.env.MISSION_CONTROL_RUN_WORKSPACE_SLUG || 'run-tracking-operations';
const INTERNAL_WORKSPACE_SLUG =
  process.env.MISSION_CONTROL_INTERNAL_WORKSPACE_SLUG || 'internal-dev-hardening';
const REQUEST_TIMEOUT_MS = Number(process.env.MISSION_CONTROL_SYNC_TIMEOUT_MS || 5000);
const WORKER_HEARTBEAT_MIN_INTERVAL_MS = Number(
  process.env.MISSION_CONTROL_WORKER_HEARTBEAT_INTERVAL_MS || 5000
);
const MC_API_TOKEN = process.env.MC_API_TOKEN || '';
const STALE_WATCHDOG_ENABLED =
  (process.env.MISSION_CONTROL_STALE_WATCHDOG_ENABLED || 'true').toLowerCase() === 'true';
const STALE_WATCHDOG_INTERVAL_MS = Number(
  process.env.MISSION_CONTROL_STALE_WATCHDOG_INTERVAL_MS || 60000
);
const STALE_THRESHOLD_MS = Number(process.env.MISSION_CONTROL_STALE_THRESHOLD_MS || 20 * 60 * 1000);
const MAX_RETRY_ATTEMPT = Number(process.env.MISSION_CONTROL_MAX_RETRY_ATTEMPT || 4);

class MissionControlSync {
  private readonly enabled: boolean;
  private readonly baseUrl: string;
  private readonly runWorkspaceSlug: string;
  private readonly runTaskIdsByKey = new Map<string, string>();
  private readonly namedTaskIds = new Map<string, string>();
  private workspaceId: string | null = null;
  private internalWorkspaceId: string | null = null;
  private sequence = Promise.resolve();
  private warnedUnavailable = false;
  private lastWorkerHeartbeatPush = 0;
  private staleTimer: NodeJS.Timeout | null = null;
  private lastDailySummaryDate = '';

  constructor(enabled: boolean, baseUrl: string, runWorkspaceSlug: string) {
    this.enabled = enabled;
    this.baseUrl = baseUrl;
    this.runWorkspaceSlug = runWorkspaceSlug;
    this.startStaleWatchdog();
  }

  public enqueue(event: LogEvent): void {
    if (!this.enabled) return;
    this.sequence = this.sequence
      .then(async () => this.sync(event))
      .catch(() => {
        // Keep failures isolated from the scraping pipeline.
      });
  }

  private async sync(event: LogEvent): Promise<void> {
    const stage = this.asString(event.stage);
    if (!stage) return;

    const workspaceId = await this.getWorkspaceId();
    if (!workspaceId) return;

    if (this.isRunStage(stage)) {
      await this.syncRunEvent(workspaceId, event);
      return;
    }

    if (this.isWorkerStage(stage)) {
      await this.syncWorkerEvent(workspaceId, event);
    }

    await this.maybeUpdateDailySummary(workspaceId);
  }

  private async syncRunEvent(workspaceId: string, event: LogEvent): Promise<void> {
    const stage = this.asString(event.stage) || 'unknown';
    const runKey = this.buildRunKey(event);
    const title = this.buildRunTitle(event);
    const status = this.stageToStatus(stage);
    const priority = this.stageToPriority(stage);
    const description = this.buildRunDescription(event);

    const taskId = await this.getOrCreateTask(workspaceId, runKey, title, description, status, priority);
    await this.patchTask(taskId, {
      status,
      priority,
      description,
      title,
      status_reason:
        status === 'review' ? this.asString(event.error) || `stage=${stage}` : null,
    });

    await this.syncCostSummary(workspaceId, event);
  }

  private async syncWorkerEvent(workspaceId: string, event: LogEvent): Promise<void> {
    const stage = this.asString(event.stage) || 'worker_update';
    const now = Date.now();

    // Throttle noisy heartbeat updates to keep overhead low.
    if (
      (stage === 'worker_claimed' || stage === 'worker_process_start' || stage === 'worker_job_done') &&
      now - this.lastWorkerHeartbeatPush < WORKER_HEARTBEAT_MIN_INTERVAL_MS
    ) {
      return;
    }

    if (stage === 'worker_claimed' || stage === 'worker_process_start' || stage === 'worker_job_done') {
      this.lastWorkerHeartbeatPush = now;
    }

    if (stage === 'worker_retry_later') {
      const attempt = this.asNumber(event.attempt);
      const retryBudgetExceeded = attempt !== null && attempt >= MAX_RETRY_ATTEMPT;
      const retryTaskId = await this.getOrCreateNamedTask(
        workspaceId,
        'worker-retries',
        'Worker Retry Monitor',
        this.buildWorkerDescription(event),
        retryBudgetExceeded ? 'review' : 'testing',
        retryBudgetExceeded ? 'urgent' : 'high'
      );
      await this.patchTask(retryTaskId, {
        status: retryBudgetExceeded ? 'review' : 'testing',
        priority: retryBudgetExceeded ? 'urgent' : 'high',
        description: this.buildWorkerDescription(event),
        status_reason: retryBudgetExceeded
          ? `retry budget exceeded at attempt ${attempt}`
          : this.asString(event.error) || null,
      });
      return;
    }

    if (stage === 'worker_job_failed' || stage === 'worker_job_exhausted') {
      const failureTaskId = await this.getOrCreateNamedTask(
        workspaceId,
        'worker-failures',
        'Worker Failure Monitor',
        this.buildWorkerDescription(event),
        'review',
        'urgent'
      );
      await this.patchTask(failureTaskId, {
        status: 'review',
        priority: 'urgent',
        description: this.buildWorkerDescription(event),
        status_reason: this.asString(event.error) || 'worker failure',
      });
      return;
    }

    const heartbeatTaskId = await this.getOrCreateNamedTask(
      workspaceId,
      'worker-heartbeat',
      'Worker Queue Heartbeat',
      this.buildWorkerDescription(event),
      stage === 'worker_complete' || stage === 'worker_shutdown' ? 'done' : 'in_progress',
      'normal'
    );

    await this.patchTask(heartbeatTaskId, {
      status: stage === 'worker_complete' || stage === 'worker_shutdown' ? 'review' : 'in_progress',
      description: this.buildWorkerDescription(event),
      status_reason:
        stage === 'worker_complete' || stage === 'worker_shutdown'
          ? 'Awaiting evidence review before done'
          : null,
    });
  }

  private startStaleWatchdog(): void {
    if (!this.enabled || !STALE_WATCHDOG_ENABLED || this.staleTimer) return;
    this.staleTimer = setInterval(() => {
      this.sequence = this.sequence
        .then(async () => this.runStaleWatchdog())
        .catch(() => {
          // Keep watchdog failures isolated.
        });
    }, STALE_WATCHDOG_INTERVAL_MS);
  }

  private async runStaleWatchdog(): Promise<void> {
    const workspaceId = await this.getWorkspaceId();
    if (!workspaceId) return;

    type TaskLite = {
      id: string;
      title: string;
      status: StageStatus;
      updated_at?: string;
    };

    const tasks = await this.requestJson<TaskLite[]>(
      `/api/tasks?workspace_id=${encodeURIComponent(workspaceId)}`
    );
    const now = Date.now();

    for (const task of tasks) {
      if (!['assigned', 'in_progress', 'testing'].includes(task.status)) continue;
      const updatedAtMs = task.updated_at ? Date.parse(task.updated_at) : NaN;
      if (!Number.isFinite(updatedAtMs)) continue;
      if (now - updatedAtMs < STALE_THRESHOLD_MS) continue;

      await this.patchTask(task.id, {
        status: 'review',
        priority: 'urgent',
        status_reason: `stale watchdog: no update for ${Math.round((now - updatedAtMs) / 60000)}m`,
      });
    }
  }

  private async getWorkspaceId(): Promise<string | null> {
    if (this.workspaceId) return this.workspaceId;

    try {
      const workspace = await this.requestJson<WorkspaceResponse>(
        `/api/workspaces/${encodeURIComponent(this.runWorkspaceSlug)}`
      );
      this.workspaceId = workspace.id;
      return this.workspaceId;
    } catch {
      if (!this.warnedUnavailable) {
        this.warnedUnavailable = true;
      }
      return null;
    }
  }

  private async getInternalWorkspaceId(): Promise<string | null> {
    if (this.internalWorkspaceId) return this.internalWorkspaceId;

    try {
      const workspace = await this.requestJson<WorkspaceResponse>(
        `/api/workspaces/${encodeURIComponent(INTERNAL_WORKSPACE_SLUG)}`
      );
      this.internalWorkspaceId = workspace.id;
      return this.internalWorkspaceId;
    } catch {
      return null;
    }
  }

  private async getOrCreateTask(
    workspaceId: string,
    cacheKey: string,
    title: string,
    description: string,
    status: StageStatus,
    priority: 'low' | 'normal' | 'high' | 'urgent'
  ): Promise<string> {
    const cached = this.runTaskIdsByKey.get(cacheKey);
    if (cached) return cached;

    const existing = await this.findTaskByTitle(workspaceId, title);
    if (existing) {
      this.runTaskIdsByKey.set(cacheKey, existing.id);
      return existing.id;
    }

    const created = await this.requestJson<TaskResponse>('/api/tasks', {
      method: 'POST',
      body: JSON.stringify({
        workspace_id: workspaceId,
        title,
        description,
        status,
        priority,
      }),
      headers: { 'Content-Type': 'application/json' },
    });

    this.runTaskIdsByKey.set(cacheKey, created.id);
    return created.id;
  }

  private async getOrCreateNamedTask(
    workspaceId: string,
    nameKey: string,
    title: string,
    description: string,
    status: StageStatus,
    priority: 'low' | 'normal' | 'high' | 'urgent'
  ): Promise<string> {
    const cached = this.namedTaskIds.get(nameKey);
    if (cached) return cached;

    const existing = await this.findTaskByTitle(workspaceId, title);
    if (existing) {
      this.namedTaskIds.set(nameKey, existing.id);
      return existing.id;
    }

    const created = await this.requestJson<TaskResponse>('/api/tasks', {
      method: 'POST',
      body: JSON.stringify({
        workspace_id: workspaceId,
        title,
        description,
        status,
        priority,
      }),
      headers: { 'Content-Type': 'application/json' },
    });

    this.namedTaskIds.set(nameKey, created.id);
    return created.id;
  }

  private async findTaskByTitle(workspaceId: string, title: string): Promise<TaskResponse | null> {
    const tasks = await this.requestJson<TaskResponse[]>(
      `/api/tasks?workspace_id=${encodeURIComponent(workspaceId)}`
    );
    return tasks.find((task) => task.title === title) || null;
  }

  private async patchTask(
    taskId: string,
    patch: Partial<{
      title: string;
      description: string;
      status: StageStatus;
      priority: 'low' | 'normal' | 'high' | 'urgent';
      status_reason: string | null;
    }>
  ): Promise<void> {
    await this.requestJson(`/api/tasks/${encodeURIComponent(taskId)}`, {
      method: 'PATCH',
      body: JSON.stringify(patch),
      headers: { 'Content-Type': 'application/json' },
    });
  }

  private isRunStage(stage: string): boolean {
    return [
      'scrape_start',
      'scrape_complete',
      'fatal_error',
      'enqueue_start',
      'enqueue_complete',
      'enqueue_error',
      'scrape_enhanced_start',
      'scrape_enhanced_complete',
      'scrape_enhanced_error',
    ].includes(stage);
  }

  private isWorkerStage(stage: string): boolean {
    return [
      'worker_start',
      'worker_claimed',
      'worker_process_start',
      'worker_job_done',
      'worker_job_failed',
      'worker_retry_later',
      'worker_job_exhausted',
      'worker_complete',
      'worker_shutdown',
    ].includes(stage);
  }

  private buildRunKey(event: LogEvent): string {
    const site = this.asString(event.site) || 'unknown-site';
    const start = this.asString(event.date_start) || 'na';
    const end = this.asString(event.date_end) || 'na';
    return `${site}|${start}|${end}`;
  }

  private buildRunTitle(event: LogEvent): string {
    const site = this.asString(event.site) || 'unknown-site';
    const start = this.asString(event.date_start) || 'na';
    const end = this.asString(event.date_end) || 'na';
    return `Run ${site}: ${start} -> ${end}`;
  }

  private buildRunDescription(event: LogEvent): string {
    const lines = [
      'Automated projection from lien pipeline runtime logs.',
      `last_stage: ${this.asString(event.stage) || 'unknown'}`,
      `last_update: ${this.asString(event.timestamp) || new Date().toISOString()}`,
      `site: ${this.asString(event.site) || 'unknown'}`,
      `date_start: ${this.asString(event.date_start) || 'n/a'}`,
      `date_end: ${this.asString(event.date_end) || 'n/a'}`,
    ];

    const records = this.asNumber(event.records);
    const enqueued = this.asNumber(event.records_enqueued);
    const pending = this.asNumber(event.total_pending);
    const duration = this.asNumber(event.duration_seconds);
    const err = this.asString(event.error);
    const tokenInput = this.asNumber(event.token_input);
    const tokenOutput = this.asNumber(event.token_output);
    const tokenTotal = this.asNumber(event.token_total);
    const estimatedCostUsd = this.asNumber(event.estimated_cost_usd);

    if (records !== null) lines.push(`records: ${records}`);
    if (enqueued !== null) lines.push(`records_enqueued: ${enqueued}`);
    if (pending !== null) lines.push(`total_pending: ${pending}`);
    if (duration !== null) lines.push(`duration_seconds: ${duration}`);
    if (tokenInput !== null) lines.push(`token_input: ${tokenInput}`);
    if (tokenOutput !== null) lines.push(`token_output: ${tokenOutput}`);
    if (tokenTotal !== null) lines.push(`token_total: ${tokenTotal}`);
    if (estimatedCostUsd !== null) lines.push(`estimated_cost_usd: ${estimatedCostUsd}`);
    if (err) lines.push(`error: ${err}`);

    return lines.join('\n');
  }

  private buildWorkerDescription(event: LogEvent): string {
    const lines = [
      'Live worker monitor for queue throughput and timeout risk.',
      `last_stage: ${this.asString(event.stage) || 'worker_update'}`,
      `last_update: ${this.asString(event.timestamp) || new Date().toISOString()}`,
    ];

    const jobId = this.asNumber(event.job_id);
    const attempt = this.asNumber(event.attempt);
    const fileNumber = this.asString(event.file_number);
    const err = this.asString(event.error);

    if (jobId !== null) lines.push(`job_id: ${jobId}`);
    if (attempt !== null) lines.push(`attempt: ${attempt}`);
    if (fileNumber) lines.push(`file_number: ${fileNumber}`);
    if (err) lines.push(`error: ${err}`);

    return lines.join('\n');
  }

  private stageToStatus(stage: string): StageStatus {
    if (stage.endsWith('_start')) return 'in_progress';
    // Keep completion in testing/review until evidence is attached.
    if (stage.endsWith('_complete')) return 'testing';
    if (stage.endsWith('_error') || stage === 'fatal_error') return 'review';
    return 'in_progress';
  }

  private stageToPriority(stage: string): 'low' | 'normal' | 'high' | 'urgent' {
    if (stage.endsWith('_error') || stage === 'fatal_error') return 'urgent';
    if (stage === 'enqueue_complete') return 'high';
    return 'normal';
  }

  private async syncCostSummary(workspaceId: string, event: LogEvent): Promise<void> {
    const stage = this.asString(event.stage) || '';
    if (!stage.endsWith('_complete') && !stage.endsWith('_error') && stage !== 'fatal_error') {
      return;
    }

    const taskId = await this.getOrCreateNamedTask(
      workspaceId,
      'run-cost-summary',
      'Run Cost and Token Summary',
      'Automated cost visibility per run event.',
      'in_progress',
      'normal'
    );

    const lines = [
      'Automated run-level token/cost visibility.',
      `last_stage: ${stage || 'unknown'}`,
      `last_update: ${this.asString(event.timestamp) || new Date().toISOString()}`,
      `site: ${this.asString(event.site) || 'unknown'}`,
      `date_start: ${this.asString(event.date_start) || 'n/a'}`,
      `date_end: ${this.asString(event.date_end) || 'n/a'}`,
      `token_input: ${this.asNumber(event.token_input) ?? 'n/a'}`,
      `token_output: ${this.asNumber(event.token_output) ?? 'n/a'}`,
      `token_total: ${this.asNumber(event.token_total) ?? 'n/a'}`,
      `estimated_cost_usd: ${this.asNumber(event.estimated_cost_usd) ?? 'n/a'}`,
    ];

    await this.patchTask(taskId, {
      status: stage.endsWith('_error') || stage === 'fatal_error' ? 'review' : 'in_progress',
      priority: stage.endsWith('_error') || stage === 'fatal_error' ? 'high' : 'normal',
      description: lines.join('\n'),
      status_reason:
        stage.endsWith('_error') || stage === 'fatal_error'
          ? this.asString(event.error) || 'cost summary captured from failed run'
          : null,
    });
  }

  private async maybeUpdateDailySummary(runWorkspaceId: string): Promise<void> {
    const today = new Date().toISOString().slice(0, 10);
    if (this.lastDailySummaryDate === today) return;

    const internalWorkspaceId = await this.getInternalWorkspaceId();
    if (!internalWorkspaceId) return;

    type TaskLite = {
      id: string;
      title: string;
      description?: string;
      status: StageStatus;
      status_reason?: string | null;
    };

    const [internalTasks, runTasks] = await Promise.all([
      this.requestJson<TaskLite[]>(`/api/tasks?workspace_id=${encodeURIComponent(internalWorkspaceId)}`),
      this.requestJson<TaskLite[]>(`/api/tasks?workspace_id=${encodeURIComponent(runWorkspaceId)}`),
    ]);

    const allTasks = [...internalTasks, ...runTasks];
    const completed = allTasks.filter((task) => task.status === 'done');
    const blocked = allTasks.filter((task) => task.status === 'review');
    const retryTimeout = runTasks.filter(
      (task) =>
        /retry|timeout|stale/i.test(task.title) ||
        /retry|timeout|stale/i.test(task.status_reason || '') ||
        /retry|timeout|stale/i.test(task.description || '')
    );
    const costTokens = runTasks.filter(
      (task) => /cost|token/i.test(task.title) || /cost|token/i.test(task.description || '')
    );
    const approvals = blocked.filter(
      (task) =>
        /approval|deploy|irreversible|security|credential|sheet write/i.test(task.status_reason || '') ||
        /approval|deploy|irreversible|security|credential|sheet write/i.test(task.description || '')
    );

    const asBullets = (items: TaskLite[], withReason = false): string =>
      items.length === 0
        ? '- none'
        : items
            .map((task) =>
              withReason
                ? `- ${task.title}: ${task.status_reason || 'review pending'}`
                : `- ${task.title}`
            )
            .join('\n');

    const title = `Daily Ops Summary ${today}`;
    const description = [
      `Daily operator summary for ${today}.`,
      '',
      'Completed cards:',
      asBullets(completed),
      '',
      'Blocked cards with root causes:',
      asBullets(blocked, true),
      '',
      'Retries/timeouts:',
      asBullets(retryTimeout),
      '',
      'Token/cost by phase cards:',
      asBullets(costTokens),
      '',
      'Required approvals only:',
      asBullets(approvals),
    ].join('\n');

    const existing = internalTasks.find((task) => task.title === title);
    if (existing) {
      await this.patchTask(existing.id, {
        description,
        status: 'in_progress',
        priority: 'normal',
      });
      this.lastDailySummaryDate = today;
      return;
    }

    await this.requestJson('/api/tasks', {
      method: 'POST',
      body: JSON.stringify({
        workspace_id: internalWorkspaceId,
        title,
        description,
        status: 'in_progress',
        priority: 'normal',
      }),
      headers: { 'Content-Type': 'application/json' },
    });

    this.lastDailySummaryDate = today;
  }

  private async requestJson<T = unknown>(
    path: string,
    init: RequestInit = {}
  ): Promise<T> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

    try {
      const response = await fetch(`${this.baseUrl}${path}`, {
        ...init,
        headers: {
          ...(init.headers || {}),
          ...(MC_API_TOKEN ? { Authorization: `Bearer ${MC_API_TOKEN}` } : {}),
        },
        signal: controller.signal,
      });

      if (!response.ok) {
        throw new Error(`Mission Control API ${response.status} for ${path}`);
      }

      const text = await response.text();
      if (!text) return {} as T;
      return JSON.parse(text) as T;
    } finally {
      clearTimeout(timeout);
    }
  }

  private asString(value: unknown): string | null {
    return typeof value === 'string' && value.length > 0 ? value : null;
  }

  private asNumber(value: unknown): number | null {
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
  }
}

const missionControlSync = new MissionControlSync(SYNC_ENABLED, MC_BASE_URL, RUN_WORKSPACE_SLUG);

export function syncMissionControlEvent(event: LogEvent): void {
  missionControlSync.enqueue(event);
}

