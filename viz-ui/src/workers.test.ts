import { describe, expect, it } from "vitest";
import { splitWorkers } from "./workers";
import type { WorkerView } from "./types";

const w = (hostname: string, status: WorkerView["status"], silent_for: number): WorkerView =>
  ({ hostname, status, software: null, freq: 2, silent_for, active: null, processed: null });

describe("splitWorkers", () => {
  it("keeps live workers and recently silent ones, folds the long-dead into a count", () => {
    const { shown, hidden } = splitWorkers([w("a", "online", 0), w("b", "lost", 7), w("c", "lost", 3600), w("d", "offline", 900)]);
    expect(shown.map((x) => x.hostname)).toEqual(["a", "b"]);
    expect(hidden).toBe(2);
  });
  it("never hides an online worker, however quiet it is", () => {
    expect(splitWorkers([w("a", "online", 9999)]).shown).toHaveLength(1);
  });
});
