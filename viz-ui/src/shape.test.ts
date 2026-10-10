import { describe, expect, it } from "vitest";
import { analyzeRun } from "./shape";
import { run, spawned, task } from "./testkit";

describe("analyzeRun", () => {
  it("recognises a single task", () => {
    const a = analyzeRun(run([task("a:0")], []));
    expect(a.kind).toBe("single");
    expect(a.rows).toHaveLength(1);
  });

  it("recognises a chain and indents each step one level deeper", () => {
    const a = analyzeRun(run([task("a:0", { sent_at: 0 }), task("b:0", { sent_at: 1 }), task("c:0", { sent_at: 2 })], [spawned("a:0", "b:0"), spawned("b:0", "c:0")]));
    expect(a.kind).toBe("chain");
    expect(a.title).toBe("Chain of 3");
    expect(a.rows.map((r) => r.depth)).toEqual([0, 1, 2]);
  });

  it("recognises parallel tasks that nothing joins (a group) and counts the fan-out", () => {
    const tasks = [task("p:0"), task("x:0", { sent_at: 1 }), task("y:0", { sent_at: 2 }), task("z:0", { sent_at: 3 })];
    const a = analyzeRun(run(tasks, [spawned("p:0", "x:0"), spawned("p:0", "y:0"), spawned("p:0", "z:0")]));
    expect(a.kind).toBe("parallel");
    expect(a.title).toBe("3 tasks at the same time");
    expect(a.rows.slice(1).every((r) => r.depth === 1)).toBe(true);
  });

  it("does not claim 'nothing waits' while a parallel run is still going (a chord's join is invisible until the end)", () => {
    const tasks = [task("p:0"), task("x:0", { sent_at: 1 }), task("y:0", { sent_at: 2 })];
    const edges = [spawned("p:0", "x:0"), spawned("p:0", "y:0")];
    const going = analyzeRun({ ...run(tasks, edges), status: "RUNNING" });
    const done = analyzeRun({ ...run(tasks, edges), status: "COMPLETED" });
    expect(going.kind).toBe("parallel");
    expect(going.detail).toContain("No join seen yet");
    expect(going.detail).not.toContain("nothing waited");
    expect(done.detail).toContain("nothing waited");
  });

  it("recognises fan-out with a join (a chord) from the inferred join edges", () => {
    const tasks = [task("p:0"), task("x:0", { sent_at: 1 }), task("y:0", { sent_at: 2 }), task("m:1", { sent_at: 9 })];
    const edges = [spawned("p:0", "x:0"), spawned("p:0", "y:0"), spawned("y:0", "m:1"),
      { from: "x:0", to: "m:1", kind: "inferred_fanin" as const }, { from: "p:0", to: "m:1", kind: "replaced" as const }];
    const a = analyzeRun(run(tasks, edges));
    expect(a.kind).toBe("fanout_join");
    expect(a.title).toBe("2 in parallel, then one join");
    expect(a.rows.at(-1)?.task.key).toBe("m:1");
    expect(a.rows.at(-1)?.depth).toBe(2);  // after the parallel tasks, regardless of the 'replaced' link
  });

  it("measures how many tasks truly ran at once", () => {
    const tasks = [task("a:0", { started_at: 0, ended_at: 10 }), task("b:0", { started_at: 2, ended_at: 8 }), task("c:0", { started_at: 20, ended_at: 25 })];
    expect(analyzeRun(run(tasks, [])).peakConcurrency).toBe(2);
  });

  it("treats a task that is still running as running until now", () => {
    const tasks = [task("a:0", { started_at: 0, ended_at: null }), task("b:0", { started_at: 5, ended_at: null })];
    expect(analyzeRun(run(tasks, [])).peakConcurrency).toBe(2);
  });

  it("never hangs on a cyclic graph", () => {
    const a = analyzeRun(run([task("a:0"), task("b:0")], [spawned("a:0", "b:0"), spawned("b:0", "a:0")]));
    expect(a.rows).toHaveLength(2);
  });
});
