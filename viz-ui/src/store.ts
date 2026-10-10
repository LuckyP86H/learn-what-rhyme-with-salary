// Pure reducer that folds the server's live messages into the UI's state.
import type { RunSummary, RunView, StateTable, WorkerView } from "./types";

export interface StreamState {
  connected: boolean;
  table: StateTable | null;
  summaries: Record<string, RunSummary>;
  runs: Record<string, RunView>;
  workers: WorkerView[];
}

export type StreamMsg =
  | { type: "connected"; value: boolean }
  | { type: "snapshot"; data: { states: StateTable; workers: WorkerView[]; runs: RunSummary[] } }
  | { type: "run"; data: RunView }
  | { type: "workers"; data: WorkerView[] };

export const initialState: StreamState = { connected: false, table: null, summaries: {}, runs: {}, workers: [] };

export function summarize(run: RunView): RunSummary {
  const { tasks, edges: _edges, ...rest } = run;
  return { ...rest, task_count: tasks.length };
}

/** Pure reducer for the SSE feed: the UI is always a fold of the messages the server sent. */
export function reduce(state: StreamState, msg: StreamMsg): StreamState {
  switch (msg.type) {
    case "connected":
      return { ...state, connected: msg.value };
    case "snapshot":
      return {
        ...state,
        table: msg.data.states,
        workers: msg.data.workers,
        summaries: Object.fromEntries(msg.data.runs.map((r) => [r.id, r])),
        runs: {},  // full views follow as `run` events; a reconnect must not keep stale ones
      };
    case "run":
      return {
        ...state,
        runs: { ...state.runs, [msg.data.id]: msg.data },
        summaries: { ...state.summaries, [msg.data.id]: summarize(msg.data) },
      };
    case "workers":
      return { ...state, workers: msg.data };
  }
}
