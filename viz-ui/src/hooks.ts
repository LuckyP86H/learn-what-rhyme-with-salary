import { useEffect, useReducer, useState } from "react";
import { initialState, reduce, type StreamState } from "./store";

/** Live feed: one snapshot, then run/workers updates. EventSource reconnects by itself. */
export function useStream(): StreamState {
  const [state, dispatch] = useReducer(reduce, initialState);
  useEffect(() => {
    const source = new EventSource("/api/stream");
    source.onopen = () => dispatch({ type: "connected", value: true });
    source.onerror = () => dispatch({ type: "connected", value: false });
    const on = (name: "snapshot" | "run" | "workers") =>
      source.addEventListener(name, (e) => dispatch({ type: name, data: JSON.parse((e as MessageEvent).data) } as never));
    on("snapshot");
    on("run");
    on("workers");
    return () => source.close();
  }, []);
  return state;
}

/** Poll a JSON endpoint; keeps the last good value if a request fails. */
export function usePoll<T>(url: string, everyMs: number): T | null {
  const [data, setData] = useState<T | null>(null);
  useEffect(() => {
    let alive = true;
    const tick = async () => {
      try {
        const res = await fetch(url);
        if (res.ok && alive) setData((await res.json()) as T);
      } catch {
        /* keep the previous value */
      }
    };
    void tick();
    const id = setInterval(tick, everyMs);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [url, everyMs]);
  return data;
}

/** Seconds since the epoch, refreshed every second, for live "running for 4s" labels. */
export function useNow(everyMs = 1000): number {
  const [now, setNow] = useState(() => Date.now() / 1000);
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now() / 1000), everyMs);
    return () => clearInterval(id);
  }, [everyMs]);
  return now;
}
