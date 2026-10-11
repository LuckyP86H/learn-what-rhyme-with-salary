import { describe, expect, it } from "vitest";
import { initialState, reduce } from "./store";
import { run, task } from "./testkit";
import type { StateTable } from "./types";

const table = { docs: {}, stations: [], phases: {}, states: [] } as StateTable;

describe("stream reducer", () => {
  it("starts disconnected and tracks the connection", () => {
    expect(initialState.connected).toBe(false);
    expect(reduce(initialState, { type: "connected", value: true }).connected).toBe(true);
  });

  it("a snapshot installs the state table and run summaries, and drops stale full views", () => {
    const stale = reduce(initialState, { type: "run", data: run([task("a:0")], []) });
    const next = reduce(stale, { type: "snapshot", data: { states: table, workers: [], runs: [] } });
    expect(next.table).toBe(table);
    expect(next.runs).toEqual({});
    expect(next.summaries).toEqual({});
  });

  it("a run event stores the full view and a matching summary", () => {
    const next = reduce(initialState, { type: "run", data: run([task("a:0"), task("b:0")], []) });
    expect(next.runs.r.tasks).toHaveLength(2);
    expect(next.summaries.r.task_count).toBe(2);
    expect("tasks" in next.summaries.r).toBe(false);
  });

  it("a newer view of the same run replaces the older one", () => {
    const a = reduce(initialState, { type: "run", data: run([task("a:0", { state: "STARTED" })], []) });
    const b = reduce(a, { type: "run", data: run([task("a:0", { state: "SUCCESS" })], []) });
    expect(b.runs.r.tasks[0].state).toBe("SUCCESS");
  });
});
