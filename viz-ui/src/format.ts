/** "4s", "2m 05s": short, human durations. */
export function fmtDuration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${String(s % 60).padStart(2, "0")}s`;
}

/** "analyze_chunk" from "tasks.analyze_chunk": the part after the last dot. */
export function shortName(name: string | null): string {
  if (!name) return "(unknown task)";
  return name.slice(name.lastIndexOf(".") + 1);
}

export function shortId(id: string): string {
  return id.slice(0, 6);
}

export function shortHost(host: string | null): string {
  if (!host) return "-";
  const [role, id] = host.split("@");
  return id ? `${role}·${id.slice(0, 4)}` : host;
}
