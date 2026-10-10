// Works out a run's shape (single / chain / parallel / fan-out then join) and row layout from its task graph alone.
import type { Edge, RunView, TaskView } from "./types";

export type ShapeKind = "single" | "chain" | "parallel" | "fanout_join";

export interface Row { task: TaskView; depth: number }

export interface Analysis {
  kind: ShapeKind;
  title: string;
  detail: string;
  rows: Row[];
  /** Most tasks executing at the same moment. */
  peakConcurrency: number;
}

/**
 * Work out, from the graph alone, whether a run is a single task, a chain, parallel tasks or a
 * fan-out with a join. Nothing here knows the names of any scenario, so it works for any workflow.
 */
export function analyzeRun(run: RunView): Analysis {
  const tasks = run.tasks;
  const spawned = run.edges.filter((e) => e.kind === "spawned");
  const dependency = run.edges.filter((e) => e.kind !== "replaced");  // replaced edges only link identities

  const children = new Map<string, string[]>();
  for (const e of spawned) children.set(e.from, [...(children.get(e.from) ?? []), e.to]);
  const widest = Math.max(1, ...[...children.values()].map((c) => c.length));
  const hasJoin = run.edges.some((e) => e.kind === "inferred_fanin");

  const depthOf = computeDepths(tasks, dependency);
  const rows = tasks
    .map((task) => ({ task, depth: depthOf.get(task.key) ?? 0 }))
    .sort((a, b) => a.depth - b.depth || (a.task.sent_at ?? 0) - (b.task.sent_at ?? 0) || a.task.key.localeCompare(b.task.key));

  const peakConcurrency = peakRunning(tasks);

  if (tasks.length <= 1) {
    return { kind: "single", title: "Single task", detail: "One task on its own: no fan-out and nothing depends on it.", rows, peakConcurrency };
  }
  if (hasJoin) {
    return { kind: "fanout_join", title: `${widest} in parallel, then one join`, rows, peakConcurrency,
      detail: "A callback runs once ALL of them have succeeded (Celery: chord). If any one fails, the callback never runs." };
  }
  if (widest > 1) {
    // Celery announces a chord's callback only AFTER the last header task succeeds, so while the run is
    // still going a chord is indistinguishable from a group. Say so rather than assert "nothing waits".
    const stillGoing = run.status === "RUNNING" || run.status === "ATTENTION";
    return { kind: "parallel", title: `${widest} tasks at the same time`, rows, peakConcurrency,
      detail: stillGoing
        ? "Independent tasks started together. No join seen yet: if a callback is waiting for ALL of them (Celery: chord) it only appears after the last one succeeds; otherwise it is a group."
        : "Independent tasks started together and nothing waited for them (Celery: group)." };
  }
  return { kind: "chain", title: `Chain of ${tasks.length}`, rows, peakConcurrency,
    detail: "Each task starts only after the previous one succeeds (Celery: chain)." };
}

function computeDepths(tasks: TaskView[], edges: Edge[]): Map<string, number> {
  const incoming = new Map<string, string[]>();
  for (const e of edges) incoming.set(e.to, [...(incoming.get(e.to) ?? []), e.from]);
  const memo = new Map<string, number>();
  const visiting = new Set<string>();
  const depth = (key: string): number => {
    const known = memo.get(key);
    if (known !== undefined) return known;
    if (visiting.has(key)) return 0;  // defensive: a cycle must never hang the page
    visiting.add(key);
    const parents = incoming.get(key) ?? [];
    const d = parents.length ? 1 + Math.max(...parents.map(depth)) : 0;
    visiting.delete(key);
    memo.set(key, d);
    return d;
  };
  for (const t of tasks) depth(t.key);
  return memo;
}

function peakRunning(tasks: TaskView[]): number {
  const points: Array<[number, number]> = [];
  for (const t of tasks) {
    if (t.started_at == null) continue;
    points.push([t.started_at, 1]);
    points.push([t.ended_at ?? Number.POSITIVE_INFINITY, -1]);
  }
  points.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  let current = 0;
  let peak = 0;
  for (const [, delta] of points) {
    current += delta;
    peak = Math.max(peak, current);
  }
  return peak;
}
