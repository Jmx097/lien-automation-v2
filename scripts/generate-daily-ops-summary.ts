import dotenv from 'dotenv';

dotenv.config();

type TaskStatus = 'planning' | 'inbox' | 'assigned' | 'in_progress' | 'testing' | 'review' | 'done';

type Task = {
  id: string;
  title: string;
  description: string | null;
  status: TaskStatus;
  priority: 'low' | 'normal' | 'high' | 'urgent';
  status_reason?: string | null;
  updated_at?: string;
};

type Workspace = {
  id: string;
  slug: string;
  name: string;
};

const MC_URL = (process.env.MISSION_CONTROL_URL || 'http://127.0.0.1:4000').replace(/\/+$/, '');
const MC_TOKEN = process.env.MC_API_TOKEN || '';
const INTERNAL_WORKSPACE_SLUG = process.env.MISSION_CONTROL_INTERNAL_WORKSPACE_SLUG || 'internal-dev-hardening';
const RUN_WORKSPACE_SLUG = process.env.MISSION_CONTROL_RUN_WORKSPACE_SLUG || 'run-tracking-operations';

if (!MC_TOKEN) {
  throw new Error('MC_API_TOKEN is required for daily ops summary generation.');
}

function todayLabel(): string {
  return new Date().toISOString().slice(0, 10);
}

async function requestJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(`${MC_URL}${path}`, {
    ...init,
    headers: {
      ...(init.headers || {}),
      Authorization: `Bearer ${MC_TOKEN}`,
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Mission Control API ${response.status} for ${path}: ${text}`);
  }

  const body = await response.text();
  return (body ? JSON.parse(body) : {}) as T;
}

async function getWorkspace(slug: string): Promise<Workspace> {
  return requestJson<Workspace>(`/api/workspaces/${encodeURIComponent(slug)}`);
}

async function getWorkspaceTasks(workspaceId: string): Promise<Task[]> {
  return requestJson<Task[]>(`/api/tasks?workspace_id=${encodeURIComponent(workspaceId)}`);
}

function listTitles(tasks: Task[]): string {
  if (tasks.length === 0) return '- none';
  return tasks.map((t) => `- ${t.title}`).join('\n');
}

function buildSummaryMessage(internalTasks: Task[], runTasks: Task[]): string {
  const completed = [...internalTasks, ...runTasks].filter((t) => t.status === 'done');
  const blocked = [...internalTasks, ...runTasks].filter((t) => t.status === 'review');
  const retryOrTimeout = runTasks.filter(
    (t) =>
      /retry|timeout|stale/i.test(t.title) ||
      /retry|timeout|stale/i.test(t.status_reason || '') ||
      /retry|timeout|stale/i.test(t.description || '')
  );
  const costCards = runTasks.filter((t) => /cost|token/i.test(t.title) || /cost|token/i.test(t.description || ''));
  const approvals = blocked.filter(
    (t) =>
      /approval|deploy|irreversible|security|credential|sheet write/i.test(t.status_reason || '') ||
      /approval|deploy|irreversible|security|credential|sheet write/i.test(t.description || '')
  );

  return [
    `Daily operator summary for ${todayLabel()}.`,
    '',
    'Completed cards:',
    listTitles(completed),
    '',
    'Blocked cards with root causes:',
    blocked.length === 0
      ? '- none'
      : blocked.map((t) => `- ${t.title}: ${t.status_reason || 'review pending'}`).join('\n'),
    '',
    'Retries/timeouts:',
    listTitles(retryOrTimeout),
    '',
    'Token/cost by phase cards:',
    listTitles(costCards),
    '',
    'Required approvals only:',
    listTitles(approvals),
  ].join('\n');
}

async function upsertDailySummaryTask(workspaceId: string, message: string): Promise<void> {
  const title = `Daily Ops Summary ${todayLabel()}`;
  const tasks = await getWorkspaceTasks(workspaceId);
  const existing = tasks.find((task) => task.title === title);

  if (existing) {
    await requestJson(`/api/tasks/${encodeURIComponent(existing.id)}`, {
      method: 'PATCH',
      body: JSON.stringify({
        description: message,
        status: 'in_progress',
        priority: 'normal',
      }),
    });
    return;
  }

  await requestJson('/api/tasks', {
    method: 'POST',
    body: JSON.stringify({
      workspace_id: workspaceId,
      title,
      description: message,
      status: 'in_progress',
      priority: 'normal',
    }),
  });
}

async function main(): Promise<void> {
  const internalWorkspace = await getWorkspace(INTERNAL_WORKSPACE_SLUG);
  const runWorkspace = await getWorkspace(RUN_WORKSPACE_SLUG);

  const [internalTasks, runTasks] = await Promise.all([
    getWorkspaceTasks(internalWorkspace.id),
    getWorkspaceTasks(runWorkspace.id),
  ]);

  const message = buildSummaryMessage(internalTasks, runTasks);
  await upsertDailySummaryTask(internalWorkspace.id, message);

  console.log(`Daily summary updated in workspace: ${internalWorkspace.slug}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
