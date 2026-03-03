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
const REQUEST_TIMEOUT_MS = Number(process.env.MISSION_CONTROL_SYNC_TIMEOUT_MS || 5000);
const WORKER_HEARTBEAT_MIN_INTERVAL_MS = Number(
  process.env.MISSION_CONTROL_WORKER_HEARTBEAT_INTERVAL_MS || 5000
);

class MissionControlSync {
  private readonly enabled: boolean;
  private readonly baseUrl: string;
  private readonly runWorkspaceSlug: string;
  private readonly runTaskIdsByKey = new Map<string, string>();
  private readonly namedTaskIds = new Map<string, string>();
  private workspaceId: string | null = null;
  private sequence = Promise.resolve();
  private warnedUnavailable = false;
  private lastWorkerHeartbeatPush = 0;

  constructor(enabled: boolean, baseUrl: string, runWorkspaceSlug: string) {
    this.enabled = enabled;
    this.baseUrl = baseUrl;
    this.runWorkspaceSlug = runWorkspaceSlug;
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
    });
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
      const retryTaskId = await this.getOrCreateNamedTask(
        workspaceId,
        'worker-retries',
        'Worker Retry Monitor',
        this.buildWorkerDescription(event),
        'testing',
        'high'
      );
      await this.patchTask(retryTaskId, {
        status: 'testing',
        priority: 'high',
        description: this.buildWorkerDescription(event),
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
      status: stage === 'worker_complete' || stage === 'worker_shutdown' ? 'done' : 'in_progress',
      description: this.buildWorkerDescription(event),
    });
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

    if (records !== null) lines.push(`records: ${records}`);
    if (enqueued !== null) lines.push(`records_enqueued: ${enqueued}`);
    if (pending !== null) lines.push(`total_pending: ${pending}`);
    if (duration !== null) lines.push(`duration_seconds: ${duration}`);
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
    if (stage.endsWith('_complete')) return 'done';
    if (stage.endsWith('_error') || stage === 'fatal_error') return 'review';
    return 'in_progress';
  }

  private stageToPriority(stage: string): 'low' | 'normal' | 'high' | 'urgent' {
    if (stage.endsWith('_error') || stage === 'fatal_error') return 'urgent';
    if (stage === 'enqueue_complete') return 'high';
    return 'normal';
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

