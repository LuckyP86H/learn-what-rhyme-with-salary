// Shapes returned by the celery_viz API. They mirror celery_viz/derive.py and states.json.

export interface StateSetting { name: string; why: string; url: string }

export interface StateInfo {
  key: string;
  label: string;
  /** "celery": Celery reports it. "derived": this monitor infers it. "fallback": unknown state. */
  kind: "celery" | "derived" | "fallback";
  celery: { state: string | null; event: string | null };
  station: string;
  phase: string;
  tone: string;
  summary: string;
  detail: string;
  settings: StateSetting[];
}

export interface Station { key: string; label: string; hint: string }

export interface StateTable {
  docs: Record<string, string>;
  stations: Station[];
  phases: Record<string, string>;
  states: StateInfo[];
}

export interface TimelineStep { ts: number; state: string; worker: string | null; note: string | null; transition: boolean }

export interface TaskView {
  key: string;
  uuid: string;
  name: string | null;
  state: string;
  raw_state: string;
  station: string;
  phase: string;
  note: string | null;
  queue: string | null;
  worker: string | null;
  retries: number;
  eta: number | null;
  redelivered: number;
  duplicates: number;
  runtime: number | null;
  exception: string | null;
  sent_at: number | null;
  started_at: number | null;
  ended_at: number | null;
  parent_key: string | null;
  replaced_from: string | null;
  timeline: TimelineStep[];
}

export interface Edge { from: string; to: string; kind: "spawned" | "replaced" | "inferred_fanin" }

export interface RunSummary {
  id: string;
  title: string;
  status: string;
  has_failures: boolean;
  started_at: number;
  updated_at: number;
  counts: Record<string, number>;
  task_count: number;
}

export interface RunView extends Omit<RunSummary, "task_count"> { tasks: TaskView[]; edges: Edge[] }

export interface WorkerView {
  hostname: string;
  status: "online" | "offline" | "lost";
  software: string | null;
  freq: number | null;
  silent_for: number;
  active: number | null;
  processed: number | null;
}

export interface QueueRow { queue: string; ready: number; unacked: number }
export interface QueuesResponse { available: boolean; queues: QueueRow[]; reason?: string }

export interface ConfigResponse {
  settings: Record<string, { value: unknown; workers: number; mixed: boolean }>;
  workers: Record<string, { prefetch_count: number | null; concurrency: number | null }>;
  error: string | null;
}

export interface ParamSpec { name: string; label: string; type: "int" | "select"; default: unknown; options?: string[]; min?: number; max?: number }
export interface Scenario { id: string; title: string; description: string; params: ParamSpec[] }
export interface CatalogResponse { enabled: boolean; scenarios: Scenario[] }
