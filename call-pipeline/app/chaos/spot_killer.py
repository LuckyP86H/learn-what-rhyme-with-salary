#!/usr/bin/env python3
"""Spot Instance chaos simulator.

Starts a call, waits until about half the chunks are done and one is mid-flight, then "evicts" the
worker running it, boots a replacement, and reports what happened to the in-flight chunk.

Runs on your HOST (standard library only, shells out to `docker`). From call-pipeline/:

    python3 app/chaos/spot_killer.py                    # SIGKILL: the instance vanishes
    python3 app/chaos/spot_killer.py --mode term-kill   # SIGTERM, then SIGKILL after a short grace period
    python3 app/chaos/spot_killer.py --mode sigterm     # warm shutdown only (NOT data loss; see below)
    python3 app/chaos/spot_killer.py --mode child       # kill only the pool child (an OOM-kill)
    python3 app/chaos/spot_killer.py --pause            # narrate and wait for Enter at each step

Kill modes and what they teach:
  sigkill    Whole container dies instantly, AMQP connection drops. With acks_late RabbitMQ requeues the
             unacked message. With Celery's defaults the message was already ACKed: the chunk is gone.
  term-kill  What a real Spot eviction looks like: a notice (SIGTERM), a short window, then power-off.
  sigterm    Celery treats SIGTERM as a *warm shutdown*: stop taking work, finish what is running, exit.
             The in-flight chunk completes. Great graceful behaviour, but not an eviction.
  child      Only the pool child process dies; the parent worker lives on and keeps its broker
             connection, so RabbitMQ never notices. This is what task_reject_on_worker_lost is for.

Exit code: 0 = call completed on its own, 2 = completed only because the Task Police intervened,
1 = call failed or never finished.
"""
from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
T0 = time.monotonic()

# Run inside the container: SIGKILL every direct child of PID 1 (the celery parent), i.e. the pool child.
KILL_POOL_CHILD = """
import os, signal
for p in os.listdir('/proc'):
    if not p.isdigit() or int(p) in (1, os.getpid()):
        continue
    try:
        ppid = int(open(f'/proc/{p}/stat').read().rsplit(')', 1)[1].split()[1])
    except Exception:
        continue
    if ppid == 1:
        os.kill(int(p), signal.SIGKILL)
        print('killed pool child pid', p)
"""


def say(msg: str = "") -> None:
    print(f"[t+{time.monotonic() - T0:5.1f}s] {msg}" if msg else "", flush=True)


def api(base: str, path: str, body: dict | None = None) -> dict | list:
    req = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"content-type": "application/json"},
        method="POST" if body is not None else "GET",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def docker(*args: str, check: bool = True) -> str:
    proc = subprocess.run(["docker", *args], cwd=PROJECT_DIR, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise SystemExit(f"docker {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout.strip()


def spot_containers() -> list[str]:
    out = docker("ps", "--filter", "label=com.docker.compose.service=worker-spot", "--format", "{{.ID}}")
    return out.split()


def container_of(worker: str) -> str | None:
    """'spot@63175ed398dc' -> container id (the hostname of a container IS its short id)."""
    host = worker.split("@", 1)[1]
    found = docker("ps", "-a", "--filter", f"id={host}", "--format", "{{.ID}}", check=False)
    return found.splitlines()[0] if found else None


def describe(state: dict) -> str:
    chunks = state["chunks"]
    done = sum(c["status"] == "COMPLETED" for c in chunks)
    running = [f"c{c['index']}@{(c['worker'] or '?').split('@')[-1][:6]} {c['progress']:.0%}" for c in chunks if c["status"] == "PROCESSING"]
    pending = sum(c["status"] == "PENDING" for c in chunks)
    failed = sum(c["status"] == "FAILED" for c in chunks)
    return f"{state['status']:<10} done={done}/{len(chunks)} pending={pending} failed={failed} running=[{', '.join(running)}]"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default="http://localhost:8100")
    ap.add_argument("--chunks", type=int, default=8)
    ap.add_argument("--mode", choices=["sigkill", "term-kill", "sigterm", "child"], default="sigkill")
    ap.add_argument("--kill-at", type=float, default=0.5, help="fraction of chunks that must be done before the kill")
    ap.add_argument("--grace", type=float, default=2, help="term-kill: seconds between SIGTERM and SIGKILL")
    ap.add_argument("--replace-after", type=float, default=5, help="seconds before the replacement worker boots")
    ap.add_argument("--stall-timeout", type=float, default=20, help="declare the call STUCK after this long without any change")
    ap.add_argument("--wait-for-police", type=float, default=0, help="after a stall, keep waiting this long for the Task Police")
    ap.add_argument("--timeout", type=float, default=240)
    ap.add_argument("--pause", action="store_true", help="wait for Enter before each step")
    args = ap.parse_args()

    def pause(msg: str) -> None:
        if args.pause:
            input(f"      ↳ {msg} [Enter] ")

    try:
        cfg = api(args.api, "/config")
    except (urllib.error.URLError, OSError) as exc:
        print(f"cannot reach the API at {args.api} ({exc}). Is the stack up? Try: make up", file=sys.stderr)
        return 1

    say("== Spot eviction simulator ==")
    say(f"reliability mode: {cfg['reliability_mode']}  acks_late={cfg['task_acks_late']}  "
        f"reject_on_worker_lost={cfg['task_reject_on_worker_lost']}  prefetch={cfg['worker_prefetch_multiplier']}")
    say(f"kill mode: {args.mode}")
    pause("start a call")

    call_id = api(args.api, "/calls", {"title": "chaos call", "num_chunks": args.chunks})["call_id"]
    say(f"started call {call_id} with {args.chunks} chunks")

    need_done = math.ceil(args.kill_at * args.chunks)
    victim: dict | None = None
    containers_before: list[str] = []
    stuck_since: float | None = None
    last_sig = None
    last_change = time.monotonic()
    last_line = ""
    deadline = time.monotonic() + args.timeout
    stalled_reported = False
    polls_without_victim = 0

    while time.monotonic() < deadline:
        state = api(args.api, f"/calls/{call_id}")
        line = describe(state)
        if line != last_line:
            say(line)
            last_line = line
        if state["status"] in ("COMPLETED", "FAILED"):
            break

        chunks = state["chunks"]
        sig = [(c["status"], c["attempts"], round(c["progress"], 1)) for c in chunks] + [state["status"], state["recovery_count"]]
        if sig != last_sig:
            last_sig, last_change = sig, time.monotonic()

        done = sum(c["status"] == "COMPLETED" for c in chunks)
        if victim is None and done >= need_done:
            running = [c for c in chunks if c["status"] == "PROCESSING" and c["worker"]]
            # term-kill must interrupt the chunk, so pick one early enough that it cannot finish in the grace window.
            upper = 0.5 if args.mode == "term-kill" else 0.85
            midflight = [c for c in running if 0.2 <= c["progress"] <= upper]
            polls_without_victim += 1
            pick = midflight[0] if midflight else (running[0] if running and polls_without_victim > 5 else None)
            if pick:
                victim = dict(pick, container=container_of(pick["worker"]))
                containers_before = spot_containers()
                say(f"{done}/{args.chunks} chunks done; chunk {victim['index']} is {victim['progress']:.0%} through on "
                    f"{victim['worker']} (container {victim['container']}, attempt {victim['attempts']})")
                pause(f"evict {victim['worker']} using mode '{args.mode}'")
                evict(args, victim, say)
                pause("boot the replacement worker")
                if args.mode != "child":
                    replace(args, victim, containers_before, say)
                last_change = time.monotonic()

        if victim is not None and time.monotonic() - last_change > args.stall_timeout and not stalled_reported:
            stalled_reported = True
            stuck_since = time.monotonic()
            say(f"!! no progress for {args.stall_timeout:.0f}s: the call is STUCK. The chord is waiting for a task that no worker owns.")
            if args.wait_for_police <= 0:
                break
            say(f"   waiting up to {args.wait_for_police:.0f}s for the Task Police (beat runs it every {cfg['police_interval_seconds']:.0f}s; "
                f"stall threshold {cfg['stall_threshold_seconds']:.0f}s)")
            deadline = min(deadline, time.monotonic() + args.wait_for_police)
        time.sleep(1)

    final = api(args.api, f"/calls/{call_id}")
    events = api(args.api, f"/calls/{call_id}/events")
    return report(args, cfg, victim, final, events, stuck_since is not None)


def evict(args: argparse.Namespace, victim: dict, say) -> None:
    cid = victim["container"]
    if not cid:
        raise SystemExit("could not map the worker to a container (is it running under docker compose?)")
    if args.mode == "sigkill":
        say(f"docker kill -s SIGKILL {cid}   <- the instance just vanished")
        docker("kill", "-s", "SIGKILL", cid)
    elif args.mode == "term-kill":
        say(f"docker kill -s SIGTERM {cid}   <- 'spot interruption notice': Celery starts a warm shutdown")
        docker("kill", "-s", "SIGTERM", cid)
        time.sleep(args.grace)
        say(f"docker kill -s SIGKILL {cid}   <- {args.grace:.0f}s later the instance is powered off")
        docker("kill", "-s", "SIGKILL", cid, check=False)
    elif args.mode == "sigterm":
        say(f"docker kill -s SIGTERM {cid}   <- warm shutdown: the worker should finish chunk {victim['index']} then exit")
        docker("kill", "-s", "SIGTERM", cid)
    else:
        say(f"killing only the pool child inside {cid}; the parent worker stays alive")
        out = docker("exec", cid, "python", "-c", KILL_POOL_CHILD)
        say(f"   {out}")


def replace(args: argparse.Namespace, victim: dict, before: list[str], say) -> None:
    cid = victim["container"]
    if args.mode == "sigterm":
        say("waiting for the warm shutdown to finish and the container to exit...")
        for _ in range(60):
            if docker("ps", "-q", "--filter", f"id={cid}", check=False) == "":
                break
            time.sleep(1)
    say(f"booting a replacement worker in {args.replace_after:.0f}s (AWS would launch a fresh instance)")
    time.sleep(args.replace_after)
    docker("rm", "-f", cid, check=False)  # a replacement is a NEW instance, not a restart of the dead one
    docker("compose", "up", "-d", "--no-deps", "worker-spot")
    new = [c for c in spot_containers() if c not in before]
    say(f"replacement worker running: {', '.join(new) or '(none detected)'}")


def report(args: argparse.Namespace, cfg: dict, victim: dict | None, final: dict, events: list, was_stuck: bool) -> int:
    say()
    say("== Verification ==")
    say(f"final call status: {final['status']}" + (f"   error: {final['error']}" if final.get("error") else ""))
    if victim is None:
        say("no chunk was ever in flight at the kill threshold; nothing to verify (try more --chunks).")
        return 1

    chunk = next(c for c in final["chunks"] if c["index"] == victim["index"])
    mine = [e for e in events if e["chunk_index"] == victim["index"]]
    police = [e for e in events if e["event"] == "POLICE_RECOVERY"]
    redelivered = any("redelivered=True" in (e["detail"] or "") for e in mine)
    completes = [e for e in events if e["event"] == "CALL_COMPLETED"]
    skipped_merges = [e for e in events if e["event"] == "MERGE_SKIPPED_ALREADY_DONE"]
    new_worker = chunk["worker"]

    say(f"victim chunk {victim['index']}: was on {victim['worker']} at {victim['progress']:.0%}; "
        f"now {chunk['status']} after {chunk['attempts']} attempt(s), last run on {new_worker}")
    for e in mine:
        say(f"    {e['at'][11:23]}  {e['event']:<24} {e['worker'] or '':<20} {e['detail'] or ''}")

    say()
    say("Q1  Was the in-flight chunk dropped or re-queued?")
    finished = chunk["status"] == "COMPLETED"
    duplicate = [e for e in mine if e["event"] == "CHUNK_SKIPPED_ALREADY_DONE"]
    if finished and chunk["attempts"] == 1:
        say("    NEITHER. It simply finished (warm shutdown let it complete before the worker exited).")
        if duplicate:
            say("    But note: the worker died after storing the result and before ACKing, so RabbitMQ redelivered "
                "the message anyway.\n"
                "    The idempotency check turned that duplicate into a no-op (CHUNK_SKIPPED_ALREADY_DONE). "
                "That is at-least-once delivery in action.")
    elif finished and not police:
        how = ("RabbitMQ put the dead worker's unacked message back on the queue (redelivered=True)"
               if redelivered else "the chunk was re-run")
        say(f"    RE-QUEUED. {how}; {new_worker} took the chunk over once the dead worker's lease expired and finished it.")
    elif finished:
        say("    DROPPED by the broker. Only the Task Police got it finished (a police recovery re-dispatched it).")
    else:
        say("    DROPPED. The message is gone and nothing has brought it back.")

    say("Q2  Did acks_late + reject_on_worker_lost prevent data loss?")
    flags = f"acks_late={cfg['task_acks_late']}, reject_on_worker_lost={cfg['task_reject_on_worker_lost']}"
    if finished and chunk["attempts"] > 1 and not police:
        say(f"    YES ({flags}). Nothing was lost and no police were needed.")
    elif finished and chunk["attempts"] == 1:
        say(f"    Not exercised: the chunk survived the shutdown by itself ({flags}).")
    elif finished:
        say(f"    NO ({flags}). Data was lost at the broker; the police band-aid recovered it.")
    else:
        say(f"    NO ({flags}). Work was lost.")

    say("Q3  Did the chord callback (merge_and_finalize) fire once the replacement finished the chunk?")
    if len(completes) == 1:
        extra = f" ({len(skipped_merges)} extra callback delivery was a harmless no-op)" if skipped_merges else ""
        say(f"    YES, exactly once{extra}.")
    elif final["status"] == "FAILED":
        say("    NO. The call FAILED instead, so the success callback never ran.")
    else:
        say("    NO. The chord is still waiting; it will never fire on its own.")

    if police:
        say(f"    (Task Police recoveries: {len(police)}; {police[0]['detail']})")

    if final["status"] == "COMPLETED":
        return 2 if police else 0
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
