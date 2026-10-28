import type { RunView, StateTable } from "../types";

interface Props { table: StateTable; run: RunView | null; selected: string | null; onPick: (key: string) => void }

/** Every state the monitor can show, grouped by the stage it belongs to. Built from the API, not hard-coded. */
export function Legend({ table, run, selected, onPick }: Props) {
  const present = new Map<string, number>();
  for (const t of run?.tasks ?? []) present.set(t.state, (present.get(t.state) ?? 0) + 1);
  return (
    <div className="legend">
      {table.stations.map((station) => {
        const states = table.states.filter((s) => s.station === station.key && s.key !== "UNKNOWN");
        if (!states.length) return null;
        return (
          <div key={station.key} className="legend-group">
            <div className="legend-title" title={station.hint}>{station.label}</div>
            {states.map((s) => (
              <button
                key={s.key}
                className={`pill tone-${s.tone} ${s.kind} ${present.has(s.key) ? "present" : ""} ${selected === s.key ? "selected" : ""}`}
                onClick={() => onPick(s.key)}
                title={s.summary}
              >
                {s.kind === "derived" && <span aria-label="inferred by the monitor" title="inferred by the monitor">≈ </span>}
                {s.label}
                {present.has(s.key) && <span className="count">{present.get(s.key)}</span>}
              </button>
            ))}
          </div>
        );
      })}
      <div className="legend-note"><span className="muted">≈ = inferred by this monitor; Celery does not report it. Filled pills occur in the selected run.</span></div>
    </div>
  );
}
