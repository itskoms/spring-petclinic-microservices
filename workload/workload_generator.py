#!/usr/bin/env python3
"""
PetClinic Workload Generator
Reads traffic configuration from workload_config.yaml and drives HTTP load
against the PetClinic API Gateway to simulate realistic usage patterns.

Usage:
    python workload_generator.py [--config workload_config.yaml] [--dry-run]
"""

import argparse
import csv
import random
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import requests
import yaml

# ── helpers ─────────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def resolve_path(template: str, path_params: dict) -> str:
    result = template
    for key, spec in path_params.items():
        if spec["type"] == "range":
            value = random.randint(spec["min"], spec["max"])
        elif spec["type"] == "choice":
            value = random.choice(spec["values"])
        else:
            value = spec.get("value", 1)
        result = result.replace(f"{{{key}}}", str(value))
    return result


def build_body(template: dict) -> dict:
    """Return body dict; extend here for dynamic field generation."""
    return dict(template)


def current_phase(phases: list, elapsed: float) -> dict | None:
    for phase in phases:
        if phase["start_second"] <= elapsed < phase["end_second"]:
            return phase
    return None


# ── request worker ───────────────────────────────────────────────────────────

class RequestWorker(threading.Thread):
    def __init__(self, worker_id: int, task_queue, results: list,
                 lock: threading.Lock, base_url: str, timeout: int):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.results = results
        self.lock = lock
        self.base_url = base_url
        self.timeout = timeout

    def run(self):
        while True:
            task = self.task_queue.get()
            if task is None:
                break
            self._execute(task)
            self.task_queue.task_done()

    def _execute(self, endpoint: dict):
        path = endpoint["path"]
        if "path_params" in endpoint:
            path = resolve_path(path, endpoint["path_params"])

        url = self.base_url + path
        method = endpoint["method"].upper()
        body = build_body(endpoint.get("body_template", {})) if method in ("POST", "PUT") else None

        t0 = time.perf_counter()
        status = 0
        error = ""
        try:
            resp = requests.request(
                method, url,
                json=body,
                timeout=self.timeout,
                headers={"Content-Type": "application/json"}
            )
            status = resp.status_code
        except requests.exceptions.Timeout:
            error = "TIMEOUT"
        except requests.exceptions.ConnectionError:
            error = "CONNECTION_ERROR"
        except Exception as e:
            error = str(e)

        latency_ms = (time.perf_counter() - t0) * 1000

        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "endpoint": endpoint["name"],
            "method": method,
            "url": url,
            "status_code": status,
            "latency_ms": round(latency_ms, 2),
            "error": error,
            "expected_status": endpoint.get("expected_status", 200),
            "passed": str(status == endpoint.get("expected_status", 200) and not error),
        }
        with self.lock:
            self.results.append(record)


# ── scheduler ────────────────────────────────────────────────────────────────

def build_weighted_pool(endpoints: list) -> list:
    pool = []
    for ep in endpoints:
        pool.extend([ep] * ep["weight"])
    return pool


def run_workload(config: dict, dry_run: bool = False):
    g = config["global"]
    base_url = g["base_url"]
    total_duration = g["duration_seconds"]
    timeout = g["request_timeout_seconds"]
    log_file = g.get("log_file", "workload_results.csv")
    phases = config["phases"]
    endpoints = config["endpoints"]

    pool = build_weighted_pool(endpoints)
    results = []
    lock = threading.Lock()

    import queue as qmod
    task_queue = qmod.Queue()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting workload — duration: {total_duration}s, target: {base_url}")
    if dry_run:
        print("  [DRY RUN] No requests will be sent.")

    start_time = time.time()
    active_workers: list[RequestWorker] = []

    def adjust_workers(target_count: int):
        nonlocal active_workers
        current = len([w for w in active_workers if w.is_alive()])
        while current < target_count:
            w = RequestWorker(current, task_queue, results, lock, base_url, timeout)
            w.start()
            active_workers.append(w)
            current += 1

    tick = 0.1  # seconds between scheduler ticks
    last_phase_name = None

    try:
        while True:
            elapsed = time.time() - start_time
            if elapsed >= total_duration:
                break

            phase = current_phase(phases, elapsed)
            if phase is None:
                time.sleep(tick)
                continue

            if phase["name"] != last_phase_name:
                print(f"  [{elapsed:6.1f}s] Phase → {phase['name']}  "
                      f"({phase['requests_per_second']} req/s, "
                      f"{phase['concurrent_users']} users)")
                last_phase_name = phase["name"]

            adjust_workers(phase["concurrent_users"])

            # Enqueue enough tasks for this tick interval
            tasks_this_tick = max(1, int(phase["requests_per_second"] * tick))
            for _ in range(tasks_this_tick):
                ep = random.choice(pool)
                if not dry_run:
                    task_queue.put(ep)

            time.sleep(tick)

    except KeyboardInterrupt:
        print("\n  Interrupted — flushing results...")

    # Shutdown workers
    for _ in active_workers:
        task_queue.put(None)
    task_queue.join()

    _write_results(results, log_file)
    _print_summary(results)


def _write_results(results: list, path: str):
    if not results:
        print("No results to write.")
        return
    fields = list(results[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)
    print(f"\nResults written to: {path}  ({len(results)} requests)")


def _print_summary(results: list):
    if not results:
        return
    total = len(results)
    passed = sum(1 for r in results if r["passed"] == "True")
    errors = [r for r in results if r["error"]]
    latencies = [r["latency_ms"] for r in results if not r["error"]]

    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)] if latencies else 0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
    avg = sum(latencies) / len(latencies) if latencies else 0

    print("\n── Workload Summary ─────────────────────────────────")
    print(f"  Total requests : {total}")
    print(f"  Passed         : {passed}  ({100*passed/total:.1f}%)")
    print(f"  Errors         : {len(errors)}")
    print(f"  Avg latency    : {avg:.1f} ms")
    print(f"  p50 latency    : {p50:.1f} ms")
    print(f"  p95 latency    : {p95:.1f} ms")
    print(f"  p99 latency    : {p99:.1f} ms")
    print("─────────────────────────────────────────────────────")

    # Per-endpoint breakdown
    from collections import defaultdict
    by_ep: dict = defaultdict(list)
    for r in results:
        by_ep[r["endpoint"]].append(r)

    print("\n  Per-endpoint breakdown:")
    print(f"  {'Endpoint':<30} {'Reqs':>6} {'Pass%':>7} {'Avg ms':>8} {'p95 ms':>8}")
    for name, reqs in sorted(by_ep.items()):
        lat = sorted(r["latency_ms"] for r in reqs if not r["error"])
        ep_avg = sum(lat) / len(lat) if lat else 0
        ep_p95 = lat[int(len(lat) * 0.95)] if lat else 0
        ep_pass = sum(1 for r in reqs if r["passed"] == "True")
        print(f"  {name:<30} {len(reqs):>6} {100*ep_pass/len(reqs):>6.1f}% {ep_avg:>8.1f} {ep_p95:>8.1f}")


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PetClinic workload generator")
    parser.add_argument("--config", default="workload_config.yaml",
                        help="Path to YAML config file (default: workload_config.yaml)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse config and print phases without sending requests")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    config = load_config(str(config_path))
    run_workload(config, dry_run=args.dry_run)
