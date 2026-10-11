import type { Edge, RunView, TaskView } from "./types";

export function task(key: string, extra: Partial<TaskView> = {}): TaskView {
  return {
    key, uuid: key.split(":")[0], name: `tasks.${key}`, state: "SUCCESS", raw_state: "SUCCESS", station: "outcome",
    phase: "done_ok", note: null, queue: null, worker: null, retries: 0, eta: null, redelivered: 0, duplicates: 0,
    runtime: null, exception: null, sent_at: 0, started_at: 0, ended_at: 1, parent_key: null, replaced_from: null,
    timeline: [], ...extra,
  };
}

export function run(tasks: TaskView[], edges: Edge[]): RunView {
  return { id: "r", title: "r", status: "COMPLETED", has_failures: false, started_at: 0, updated_at: 0, counts: {}, tasks, edges };
}

export const spawned = (from: string, to: string): Edge => ({ from, to, kind: "spawned" });
