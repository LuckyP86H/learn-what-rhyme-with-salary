import { fmtDuration, shortHost, shortName } from "../format";
import type { ConfigResponse, StateInfo, StateTable, TaskView } from "../types";

function SettingValue({ name, config }: { name: string; config: ConfigResponse | null }) {
  const found = config?.settings[name];
  if (!found) return <span className="muted">value not visible</span>;
  return (
    <span>
      <code>{String(found.value)}</code> <span className="muted">{found.mixed ? "(differs between workers!)" : "(global; a task may override it)"}</span>
    </span>
  );
}

function StateCard({ info, config, table }: { info: StateInfo; config: ConfigResponse | null; table: StateTable }) {
  return (
    <>
      <h2 className={`state-title tone-${info.tone}`}>
        <span className="swatch" /> {info.label}
        <span className={`badge ${info.kind}`}>{info.kind === "celery" ? "reported by Celery" : info.kind === "derived" ? "inferred by this monitor" : "unknown"}</span>
      </h2>
      <p className="summary">{info.summary}</p>
      <p>{info.detail}</p>
      <dl className="facts">
        <dt>Celery state</dt>
        <dd>{info.celery.state ?? <span className="muted">none: not a Celery state</span>}</dd>
        <dt>Event</dt>
        <dd>{info.celery.event ?? <span className="muted">none</span>}</dd>
        <dt>Stage</dt>
        <dd title={table.stations.find((s) => s.key === info.station)?.hint}>{table.stations.find((s) => s.key === info.station)?.label}</dd>
      </dl>
      {info.settings.length > 0 && (
        <>
          <h3>Settings that change this</h3>
          <ul className="settings">
            {info.settings.map((s) => (
              <li key={s.name}>
                <a href={s.url} target="_blank" rel="noreferrer"><code>{s.name}</code></a> = <SettingValue name={s.name} config={config} />
                <div className="muted">{s.why}</div>
              </li>
            ))}
          </ul>
        </>
      )}
    </>
  );
}

interface Props {
  table: StateTable;
  config: ConfigResponse | null;
  task: TaskView | null;
  legendKey: string | null;
}

export function Inspector({ table, config, task, legendKey }: Props) {
  const byKey = new Map(table.states.map((s) => [s.key, s]));
  const info = task ? (byKey.get(task.state) ?? byKey.get("UNKNOWN")!) : legendKey ? byKey.get(legendKey) : undefined;
  if (!info) return <p className="muted">Click a task row, or a state in the legend below, to see what it means in Celery terms.</p>;

  const t0 = task?.timeline[0]?.ts ?? 0;
  return (
    <div className="inspector">
      {task && (
        <p className="task-head">
          <strong>{shortName(task.name)}</strong> <span className="muted">{task.uuid.slice(0, 8)} · attempt {task.retries + 1} · on {shortHost(task.worker)}</span>
        </p>
      )}
      {task?.note && <p className="note">{task.note}</p>}
      <StateCard info={info} config={config} table={table} />
      {task && (
        <>
          <h3>Timeline</h3>
          <ol className="timeline">
            {task.timeline.map((s, i) => {
              const step = byKey.get(s.state);
              return (
                <li key={i} className={s.transition ? "move" : "annotation"}>
                  <span className="when">+{fmtDuration(s.ts - t0)}</span>
                  <span className={`tag tone-${step?.tone ?? "muted"}`}>{s.transition ? step?.label ?? s.state : "note"}</span>
                  <span className="muted">{shortHost(s.worker)}</span> {s.note}
                </li>
              );
            })}
          </ol>
          {task.exception && <p className="exception">Exception: <code>{task.exception}</code></p>}
        </>
      )}
    </div>
  );
}
