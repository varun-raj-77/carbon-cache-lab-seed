#!/usr/bin/env python3
"""Create a reproducible, disposable Carbon relay/cache deployment.

Run from the root of a clean graphite-project/carbon checkout after installing
Carbon and Graphite Whisper in the active Python environment. This script only
creates data under a fresh temporary directory and starts localhost services.
It never edits the checked-out repository or existing application data.
"""

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

import whisper


REPO = Path.cwd().resolve()
PORTS = (2013, 2014, 2203, 2204, 2303, 2304, 2403, 2404)


def config(destinations):
    return f"""[cache]
USER =
LINE_RECEIVER_INTERFACE = 127.0.0.1
PICKLE_RECEIVER_INTERFACE = 127.0.0.1
CACHE_QUERY_INTERFACE = 127.0.0.1
ENABLE_TAGS = False
MAX_CREATES_PER_MINUTE = 1000

[cache:a]
LINE_RECEIVER_PORT = 2203
PICKLE_RECEIVER_PORT = 2204
CACHE_QUERY_PORT = 7202

[cache:b]
LINE_RECEIVER_PORT = 2303
PICKLE_RECEIVER_PORT = 2304
CACHE_QUERY_PORT = 7302

[cache:c]
LINE_RECEIVER_PORT = 2403
PICKLE_RECEIVER_PORT = 2404
CACHE_QUERY_PORT = 7402

[relay]
USER =
LINE_RECEIVER_INTERFACE = 127.0.0.1
LINE_RECEIVER_PORT = 2013
PICKLE_RECEIVER_INTERFACE = 127.0.0.1
PICKLE_RECEIVER_PORT = 2014
RELAY_METHOD = consistent-hashing
REPLICATION_FACTOR = 1
DESTINATIONS = {destinations}
TAG_RELAY_NORMALIZED = False
"""


def wait_for(predicate, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.3)
    return False


def port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.25):
            return True
    except OSError:
        return False


def launch(lab, config_path, kind, instance):
    env = dict(os.environ)
    env["GRAPHITE_STORAGE_DIR"] = str(lab / instance if kind == "cache" else lab / "relay")
    command = [
        sys.executable,
        str(REPO / "bin" / f"carbon-{kind}.py"),
        f"--config={config_path}",
        f"--instance={instance}",
        f"--pidfile={lab / (kind + '-' + instance + '.pid')}",
        "--debug",
        "start",
    ]
    log_path = lab / f"{kind}-{instance}-{config_path.stem}.log"
    with log_path.open("w", encoding="utf-8") as logfile:
        process = subprocess.Popen(
            command,
            cwd=REPO,
            env=env,
            stdout=logfile,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    return process


def stop_own_process(process):
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def send_series(value, timestamp):
    payload = "".join(
        f"traffic.service.m{i} {value} {timestamp}\n" for i in range(100)
    )
    with socket.create_connection(("127.0.0.1", 2013), timeout=5) as sock:
        sock.sendall(payload.encode("ascii"))


def files_for(lab, instance):
    return list((lab / instance / "whisper" / "traffic" / "service").glob("*.wsp"))


def at_timestamp(path, timestamp):
    if not path.is_file():
        return None
    (start, _end, step), values = whisper.fetch(
        str(path), timestamp - 10, timestamp + 10
    )
    aligned = timestamp - (timestamp % step)
    position = (aligned - start) // step
    return values[position] if 0 <= position < len(values) else None


def all_series_present(lab, timestamp, expected):
    for i in range(100):
        name = f"m{i}.wsp"
        if not any(
            at_timestamp(lab / node / "whisper" / "traffic" / "service" / name,
                         timestamp) == expected
            for node in ("a", "b", "c")
        ):
            return False
    return True


def main():
    if not (REPO / "bin" / "carbon-cache.py").is_file():
        raise SystemExit("Run this script from the pinned Carbon repository root.")
    occupied = [port for port in PORTS if port_open(port)]
    if occupied:
        raise SystemExit(f"Local ports already in use: {occupied}; use a clean lane.")

    lab = Path(tempfile.mkdtemp(prefix="johto-carbon-"))
    config_dir = lab / "conf"
    config_dir.mkdir()
    (config_dir / "storage-schemas.conf").write_text(
        "[default]\npattern = .*\nretentions = 10s:6h\n", encoding="utf-8"
    )
    two_nodes = config_dir / "carbon.conf"
    three_nodes = config_dir / "carbon-three.conf"
    two_nodes.write_text(
        config("127.0.0.1:2204:a, 127.0.0.1:2304:b"), encoding="utf-8"
    )
    three_nodes.write_text(
        config("127.0.0.1:2204:a, 127.0.0.1:2304:b, 127.0.0.1:2404:c"),
        encoding="utf-8",
    )

    processes = []
    try:
        for instance, port in (("a", 2204), ("b", 2304)):
            process = launch(lab, two_nodes, "cache", instance)
            processes.append(process)
            if not wait_for(lambda p=port: port_open(p), 15):
                raise RuntimeError(f"Carbon cache {instance} did not start")

        relay = launch(lab, two_nodes, "relay", "a")
        processes.append(relay)
        if not wait_for(lambda: port_open(2013), 15):
            raise RuntimeError("Initial relay did not start")

        clock = int(time.time() // 10) * 10
        baseline, before_cutover, after_cutover = clock - 40, clock - 19, clock - 12
        send_series(1.0, baseline)
        if not wait_for(lambda: all_series_present(lab, baseline, 1.0)):
            raise RuntimeError("Initial metric data did not reach the caches")

        send_series(3.0, before_cutover)
        if not wait_for(lambda: all_series_present(lab, before_cutover, 3.0)):
            raise RuntimeError("Pre-cutover data did not reach the caches")

        third = launch(lab, three_nodes, "cache", "c")
        processes.append(third)
        if not wait_for(lambda: port_open(2404), 15):
            raise RuntimeError("Third cache did not start")

        stop_own_process(relay)
        if not wait_for(lambda: not port_open(2013), 15):
            raise RuntimeError("Original relay did not stop cleanly")
        processes.remove(relay)
        relay = launch(lab, three_nodes, "relay", "a")
        processes.append(relay)
        if not wait_for(lambda: port_open(2013), 15):
            raise RuntimeError("New relay did not start")

        send_series(0.0, after_cutover)
        if not wait_for(lambda: all_series_present(lab, after_cutover, 0.0)):
            raise RuntimeError("Post-cutover data did not reach the caches")
        if len(files_for(lab, "c")) < 10:
            raise RuntimeError("Third cache has too few metric files")

        state = {
            "directory": str(lab),
            "configuration": str(three_nodes),
            "relay_port": 2013,
            "cache_ports": {"a": 2204, "b": 2304, "c": 2404},
            "metrics": "traffic.service.m0 through traffic.service.m99",
            "timestamps": [baseline, before_cutover, after_cutover],
            "processes": {
                "cache_a": processes[0].pid,
                "cache_b": processes[1].pid,
                "cache_c": third.pid,
                "relay": relay.pid,
            },
        }
        (lab / "state.json").write_text(json.dumps(state, indent=2), encoding="utf-8")
        print("Carbon test deployment is running with three caches and one relay.")
        print(f"State and service logs: {lab}")
        print(f"Setup manifest: {lab / 'state.json'}")
        print("Metrics: traffic.service.m0 through traffic.service.m99")
    except Exception:
        for process in reversed(processes):
            stop_own_process(process)
        print(f"Setup failed. Inspect service logs in {lab}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
