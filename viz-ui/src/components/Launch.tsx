// Launches the demo scenarios the backend's catalog offers (only shown when the catalog is enabled).
import { useState } from "react";
import type { CatalogResponse, Scenario } from "../types";

function ScenarioCard({ scenario, onLaunched }: { scenario: Scenario; onLaunched: () => void }) {
  const [values, setValues] = useState<Record<string, unknown>>(Object.fromEntries(scenario.params.map((p) => [p.name, p.default])));
  const [message, setMessage] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function launch() {
    setBusy(true);
    setMessage(null);
    try {
      const res = await fetch("/api/launch", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ scenario: scenario.id, params: values }) });
      if (!res.ok) throw new Error(((await res.json()) as { detail?: string }).detail ?? res.statusText);
      onLaunched();
      setMessage("Started: watch it appear in the runs list.");
    } catch (e) {
      setMessage(`Could not start: ${(e as Error).message}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <details className="scenario">
      <summary>{scenario.title}</summary>
      <p className="muted">{scenario.description}</p>
      {scenario.params.map((p) => (
        <label key={p.name} className="param">
          <span>{p.label}</span>
          {p.type === "select" ? (
            <select value={String(values[p.name])} onChange={(e) => setValues({ ...values, [p.name]: e.target.value })}>
              {p.options?.map((o) => <option key={o}>{o}</option>)}
            </select>
          ) : (
            <input type="number" min={p.min} max={p.max} value={Number(values[p.name])} onChange={(e) => setValues({ ...values, [p.name]: Number(e.target.value) })} />
          )}
        </label>
      ))}
      <button className="primary" disabled={busy} onClick={launch}>{busy ? "Starting…" : "Launch"}</button>
      {message && <p className="muted" role="status">{message}</p>}
    </details>
  );
}

export function Launch({ catalog, onLaunched }: { catalog: CatalogResponse | null; onLaunched: () => void }) {
  if (!catalog) return null;
  if (!catalog.enabled) return <p className="muted">Read-only mode. Start the monitor with <code>--catalog</code> to launch demo workflows from here.</p>;
  return <div>{catalog.scenarios.map((s) => <ScenarioCard key={s.id} scenario={s} onLaunched={onLaunched} />)}</div>;
}
