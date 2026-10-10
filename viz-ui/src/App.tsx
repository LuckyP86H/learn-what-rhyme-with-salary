import { useEffect, useMemo, useRef, useState } from "react";
import { Canvas } from "./components/Canvas";
import { Inspector } from "./components/Inspector";
import { Launch } from "./components/Launch";
import { Legend } from "./components/Legend";
import { QueuePanel, RunList, WorkerPanel } from "./components/Panels";
import { useNow, usePoll, useStream } from "./hooks";
import { analyzeRun } from "./shape";
import type { CatalogResponse, ConfigResponse, QueuesResponse } from "./types";

export default function App() {
  const stream = useStream();
  const now = useNow();
  const queues = usePoll<QueuesResponse>("/api/queues", 2000);
  const config = usePoll<ConfigResponse>("/api/config", 15000);
  const catalog = usePoll<CatalogResponse>("/api/catalog", 60000);

  const [selectedRun, setSelectedRun] = useState<string | null>(null);
  const [selectedTask, setSelectedTask] = useState<string | null>(null);
  const [legendKey, setLegendKey] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const followSince = useRef<number | null>(null);  // after a launch, jump to the first run that appears

  const summaries = useMemo(() => Object.values(stream.summaries).sort((a, b) => b.updated_at - a.updated_at), [stream.summaries]);

  useEffect(() => {
    if (followSince.current !== null) {
      const fresh = summaries.find((r) => r.started_at >= (followSince.current as number) - 2);
      if (fresh) {
        followSince.current = null;
        setSelectedRun(fresh.id);
        setSelectedTask(null);
      }
    } else if (selectedRun === null && summaries.length) {
      setSelectedRun(summaries[0].id);
    }
  }, [summaries, selectedRun]);

  const run = selectedRun ? (stream.runs[selectedRun] ?? null) : null;
  const analysis = useMemo(() => (run ? analyzeRun(run) : null), [run]);
  const task = run?.tasks.find((t) => t.key === selectedTask) ?? null;

  return (
    <div className="app">
      <header>
        <h1>Celery workflow visualizer</h1>
        <span className={`conn ${stream.connected ? "up" : "down"}`} role="status">{stream.connected ? "live" : "reconnecting…"}</span>
        <span className="spacer" />
        <a href={`http://${location.hostname}:5555`} target="_blank" rel="noreferrer" title="Fleet-level dashboards and metrics">Flower ↗</a>
        <a href={`http://${location.hostname}:15672`} target="_blank" rel="noreferrer" title="Queues, exchanges, consumers">RabbitMQ ↗</a>
      </header>

      <aside className="left">
        <section><h2>Launch a workflow</h2><Launch catalog={catalog} onLaunched={() => { followSince.current = Date.now() / 1000; }} /></section>
        <section>
          <h2>Runs</h2>
          <input className="search" placeholder="filter by name…" value={filter} onChange={(e) => setFilter(e.target.value)} aria-label="filter runs" />
          <RunList runs={summaries} selectedId={selectedRun} now={now} filter={filter} onSelect={(id) => { setSelectedRun(id); setSelectedTask(null); }} />
        </section>
        <section><h2>Workers</h2><WorkerPanel workers={stream.workers} /></section>
      </aside>

      <main>
        {analysis && run ? (
          <>
            <div className={`banner shape-${analysis.kind}`}>
              <div>
                <strong>{analysis.title}</strong>
                <span className={`status ${run.status.toLowerCase()}`}>{run.status}</span>
                <div className="muted">{analysis.detail}</div>
              </div>
              <div className="muted peak">peak at once: <strong>{analysis.peakConcurrency}</strong></div>
            </div>
            {stream.table && <Canvas analysis={analysis} table={stream.table} now={now} selectedKey={selectedTask} onSelect={(k) => { setSelectedTask(k); setLegendKey(null); }} />}
          </>
        ) : (
          <p className="muted empty">{summaries.length ? "Loading run…" : "Waiting for the first run. Launch one on the left."}</p>
        )}
        <section className="queues-section"><h2>Queues (measured by the broker)</h2><QueuePanel queues={queues} config={config} /></section>
      </main>

      <aside className="right">
        <h2>{task ? "Task" : "What does this state mean?"}</h2>
        {stream.table ? <Inspector table={stream.table} config={config} task={task} legendKey={legendKey} /> : <p className="muted">Connecting…</p>}
      </aside>

      <footer>
        {stream.table && <Legend table={stream.table} run={run} selected={legendKey} onPick={(k) => { setLegendKey(k); setSelectedTask(null); }} />}
      </footer>
    </div>
  );
}
