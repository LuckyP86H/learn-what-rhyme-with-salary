import type { WorkerView } from "./types";

/**
 * Containers come and go (deploys, Spot evictions) and a dead worker never announces its funeral, so the
 * list would fill with ghosts. Show live workers plus recently-silent ones (the interesting case) and
 * fold the long-dead ones into a count.
 */
export function splitWorkers(workers: WorkerView[], hideAfterSeconds = 300): { shown: WorkerView[]; hidden: number } {
  const shown = workers.filter((w) => w.status === "online" || w.silent_for < hideAfterSeconds);
  return { shown, hidden: workers.length - shown.length };
}
