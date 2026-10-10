// Side panels: the run list, the workers, and the measured queue depths.
import { fmtDuration, shortHost, shortName } from "../format";
import type { ConfigResponse, QueuesResponse, RunSummary, WorkerView } from "../types";
import { splitWorkers } from "../workers";

export function RunList({ runs, selectedId, now, onSelect, filter }: { runs: RunSummary[]; selectedId: string | null; now: number; onSelect: (id: string) => void; filter: string }) {
  const shown = runs.filter((r) => r.title.toLowerCase().includes(filter.toLowerCase()));
  if (!shown.length) return <p className="muted">No runs yet. Launch one, or run <code>make trigger</code>.</p>;
  return (
    <ul className="runs">
      {shown.map((r) => (
        <li key={r.id}>
          <button className={`run ${selectedId === r.id ? "selected" : ""}`} onClick={() => onSelect(r.id)}>
            <span className={`status ${r.status.toLowerCase()}`}>{r.status}</span>
            <span className="run-title">{shortName(r.title)}</span>
            <span className="muted">{r.task_count} task{r.task_count === 1 ? "" : "s"} · {fmtDuration(now - r.started_at)} ago</span>
          </button>
        </li>
      ))}
    </ul>
  );
}

export function WorkerPanel({ workers }: { workers: WorkerView[] }) {
  if (!workers.length) return <p className="muted">No workers seen yet. Are they running with events on (<code>-E</code>)?</p>;
  const { shown, hidden } = splitWorkers(workers);
  return (
    <>
      <ul className="workers">
        {shown.map((w) => (
          <li key={w.hostname}>
            <span className={`dot-status ${w.status}`} aria-hidden="true" />
            <strong>{shortHost(w.hostname)}</strong> <span className="muted">{w.status === "offline" ? "shutting down / offline" : w.status}{w.status === "lost" ? ` · silent ${fmtDuration(w.silent_for)}` : ""}</span>
          </li>
        ))}
      </ul>
      {hidden > 0 && <p className="muted">{hidden} long-gone worker{hidden === 1 ? "" : "s"} hidden (silent over 5 min).</p>}
    </>
  );
}

/** Measured numbers from the broker, with the sentence that explains them chosen by the live acks_late value. */
export function QueuePanel({ queues, config }: { queues: QueuesResponse | null; config: ConfigResponse | null }) {
  if (!queues) return null;
  if (!queues.available) return <p className="muted">Queue depth unavailable ({queues.reason ?? "not configured"}).</p>;
  const acksLate = config?.settings.task_acks_late?.value;
  const max = Math.max(1, ...queues.queues.map((q) => q.ready + q.unacked));
  return (
    <div>
      <ul className="queues">
        {queues.queues.map((q) => (
          <li key={q.queue}>
            <span className="q-name">{q.queue}</span>
            <span className="bar" title={`${q.ready} ready, ${q.unacked} unacked`}>
              <span className="ready" style={{ width: `${(q.ready / max) * 100}%` }} />
              <span className="unacked" style={{ width: `${(q.unacked / max) * 100}%` }} />
            </span>
            <span className="muted">{q.ready} ready · {q.unacked} unacked</span>
          </li>
        ))}
      </ul>
      <p className="muted">
        <strong>Ready</strong> = waiting for a worker. <strong>Unacked</strong> = handed to a worker, not yet acknowledged.{" "}
        {acksLate === true && "With task_acks_late = True a message stays unacked for the whole run, so a dead worker's task goes back on the queue."}
        {acksLate === false && "With task_acks_late = False the message is acknowledged as the task starts, so a dead worker's running task is lost."}
        {acksLate === undefined && "Whether running tasks count as unacked depends on task_acks_late."}
      </p>
    </div>
  );
}
