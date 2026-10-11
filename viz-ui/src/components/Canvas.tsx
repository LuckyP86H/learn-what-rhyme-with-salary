import { fmtDuration, shortHost, shortId, shortName } from "../format";
import type { RowLimit } from "../shape";
import type { StateInfo, StateTable, TaskView } from "../types";

interface Props {
  rows: RowLimit;
  table: StateTable;
  now: number;
  selectedKey: string | null;
  onSelect: (key: string) => void;
  onShowAll: () => void;
}

/** Text next to the ball: how long it has been in this state, from facts we have (never invented). */
function liveLabel(task: TaskView, info: StateInfo, now: number): string {
  if (info.key === "WAITING_ETA" && task.eta) return `${info.label} · runs in ${fmtDuration(task.eta - now)}`;
  if ((info.key === "STARTED" || info.key === "SUSPECTED_HUNG") && task.started_at) return `${info.label} · ${fmtDuration(now - task.started_at)}`;
  if (info.key === "QUEUED" && task.sent_at) return `${info.label} · ${fmtDuration(now - task.sent_at)}`;
  if (info.key === "SUCCESS" && task.runtime != null) return `${info.label} · ${task.runtime.toFixed(1)}s`;
  return info.label;
}

export function Canvas({ rows, table, now, selectedKey, onSelect, onShowAll }: Props) {
  const stationIndex = new Map(table.stations.map((s, i) => [s.key, i]));
  const stateStation = new Map(table.states.map((s) => [s.key, s.station]));
  const byKey = new Map(table.states.map((s) => [s.key, s]));
  const columns = table.stations.length;

  return (
    <div className="canvas" role="list">
      <div className="lane header" aria-hidden="true">
        <div className="lane-label" />
        <div className="track" style={{ gridTemplateColumns: `repeat(${columns}, 1fr)` }}>
          {table.stations.map((s) => (
            <div key={s.key} className={`station-name station-${s.key}`} title={s.hint}>
              {s.label}
            </div>
          ))}
        </div>
      </div>
      {rows.shown.map(({ task, depth }) => {
        const info = byKey.get(task.state) ?? byKey.get("UNKNOWN")!;
        const at = stationIndex.get(task.station) ?? columns - 1;
        const visited = new Set(task.timeline.filter((t) => t.transition).map((t) => stateStation.get(t.state)));
        return (
          <div
            key={task.key}
            role="listitem"
            className={`lane phase-${task.phase} ${selectedKey === task.key ? "selected" : ""}`}
            tabIndex={0}
            aria-current={selectedKey === task.key ? "true" : undefined}
            aria-label={`${shortName(task.name)}: ${info.label}`}
            onClick={() => onSelect(task.key)}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                onSelect(task.key);
              }
            }}
          >
            <div className="lane-label" style={{ paddingLeft: depth * 16 }}>
              {depth > 0 && <span className="elbow" aria-hidden="true">└</span>}
              <div>
                <div className="task-name">{shortName(task.name)}</div>
                <div className="task-meta">
                  {shortId(task.uuid)} · {shortHost(task.worker)}
                  {task.retries > 0 && <span className="chip retry">retry ×{task.retries}</span>}
                  {task.redelivered > 0 && <span className="chip warn">redelivered</span>}
                  {task.duplicates > 0 && <span className="chip muted">duplicate ×{task.duplicates}</span>}
                </div>
              </div>
            </div>
            <div className="track" style={{ gridTemplateColumns: `repeat(${columns}, 1fr)` }}>
              {table.stations.map((s) => (
                <div key={s.key} className={`slot station-${s.key}`}>
                  {visited.has(s.key) && s.key !== task.station && <span className="trail" />}
                </div>
              ))}
              <div className={`ball tone-${info.tone} phase-${info.phase} kind-${info.kind}`} style={{ left: `${((at + 0.5) / columns) * 100}%` }}>
                <span className="dot" aria-hidden="true" />
                <span className="ball-label">{liveLabel(task, info, now)}</span>
              </div>
            </div>
          </div>
        );
      })}
      {rows.hidden > 0 && (
        <div className="folded">
          <span>
            {rows.hidden} more task{rows.hidden === 1 ? "" : "s"} folded away (
            {Object.entries(rows.hiddenByState).map(([state, n]) => `${n} ${byKey.get(state)?.label.toLowerCase() ?? state}`).join(", ")}). Anything that failed or needs attention is always shown.
          </span>
          <button onClick={onShowAll}>Show all</button>
        </div>
      )}
    </div>
  );
}
