#!/usr/bin/env python3
"""Disposable JetStream operations lab. Python 3.12; no pip dependencies.

Commands:
    python johto_nats_lab_seed.py setup [--server /path/to/nats-server]
    python johto_nats_lab_seed.py status /tmp/johto-nats-.../state.json

The background publisher and all NATS daemons bind only to loopback. This is
a fixture, not a migration or repair tool. See the accompanying README.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
import uuid

NATS_VERSION = "2.15.1"
NATS_AMD64_SHA256 = "bf5edc7b1ce98e885c368cb43a886eb148dc5d0551d99f3cf6141fb5e1954c70"
NATS_URL = (
    f"https://github.com/nats-io/nats-server/releases/download/v{NATS_VERSION}/"
    f"nats-server-v{NATS_VERSION}-linux-amd64.tar.gz"
)
BASELINE_EVENTS = 240
ACKED_EVENTS = 81


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".new")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def append_jsonl(path: Path, item: dict) -> None:
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(item, separators=(",", ":"), sort_keys=True) + "\n")
        file.flush()
        os.fsync(file.fileno())


def free_ports(quantity: int) -> list[int]:
    held: list[socket.socket] = []
    try:
        for _ in range(quantity):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            held.append(sock)
        return [sock.getsockname()[1] for sock in held]
    finally:
        for sock in held:
            sock.close()


@dataclasses.dataclass
class Message:
    subject: str
    reply: str | None
    data: bytes
    headers: bytes = b""
    sid: int = 0

    @property
    def status(self) -> int | None:
        for line in self.headers.split(b"\r\n"):
            if line.startswith(b"NATS/1.0 "):
                try:
                    return int(line.split()[1])
                except (IndexError, ValueError):
                    pass
        return None


class Nats:
    """Minimal blocking NATS client for seeding/observing a local cluster."""

    def __init__(self, port: int, timeout: float = 7.0):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.socket.settimeout(timeout)
        self.file = self.socket.makefile("rb")
        self.sid = 0
        greeting = self._read_event()
        if greeting != "INFO":
            raise RuntimeError(f"unexpected NATS greeting on {port}: {greeting}")
        self._send(b'CONNECT {"verbose":false,"pedantic":false,"headers":true,"protocol":1,"name":"johto-fixture"}\r\n')
        self.flush()

    def __enter__(self) -> "Nats":
        return self

    def __exit__(self, *ignored) -> None:
        try:
            self.file.close()
        finally:
            self.socket.close()

    def _send(self, data: bytes) -> None:
        self.socket.sendall(data)

    def _read_event(self) -> str | Message:
        while True:
            line = self.file.readline()
            if not line:
                raise ConnectionError("NATS connection closed")
            pieces = line.strip().split()
            if not pieces:
                continue
            cmd = pieces[0]
            if cmd == b"PING":
                self._send(b"PONG\r\n")
                continue
            if cmd == b"INFO":
                return "INFO"
            if cmd == b"PONG":
                return "PONG"
            if cmd == b"+OK":
                continue
            if cmd == b"-ERR":
                raise RuntimeError(f"NATS error: {line.decode(errors='replace').strip()}")
            if cmd not in (b"MSG", b"HMSG"):
                raise RuntimeError(f"unexpected NATS protocol line: {line!r}")
            if cmd == b"MSG":
                # MSG <subject> <sid> [reply] <bytes>
                size = int(pieces[-1])
                raw = self.file.read(size + 2)
                if len(raw) != size + 2 or not raw.endswith(b"\r\n"):
                    raise ConnectionError("truncated NATS message")
                return Message(
                    pieces[1].decode(), pieces[3].decode() if len(pieces) == 5 else None,
                    raw[:-2], sid=int(pieces[2]),
                )
            # HMSG <subject> <sid> [reply] <header bytes> <total bytes>
            header_size, size = int(pieces[-2]), int(pieces[-1])
            raw = self.file.read(size + 2)
            if len(raw) != size + 2 or not raw.endswith(b"\r\n"):
                raise ConnectionError("truncated NATS header message")
            return Message(
                pieces[1].decode(),
                pieces[3].decode() if len(pieces) == 6 else None,
                raw[header_size:-2],
                raw[:header_size],
                int(pieces[2]),
            )

    def flush(self) -> None:
        self._send(b"PING\r\n")
        while True:
            event = self._read_event()
            if event == "PONG":
                return

    def publish(self, subject: str, data: bytes, reply: str | None = None, message_id: str | None = None) -> None:
        route = f"{subject} {reply}" if reply else subject
        if message_id:
            header = f"NATS/1.0\r\nNats-Msg-Id: {message_id}\r\n\r\n".encode()
            self._send(f"HPUB {route} {len(header)} {len(header) + len(data)}\r\n".encode() + header + data + b"\r\n")
        else:
            self._send(f"PUB {route} {len(data)}\r\n".encode() + data + b"\r\n")

    def request(self, subject: str, data: bytes = b"", timeout: float = 7.0, message_id: str | None = None) -> Message:
        self.sid += 1
        reply = f"_INBOX.JOHTO.{uuid.uuid4().hex}"
        sid = self.sid
        self._send(f"SUB {reply} {sid}\r\n".encode())
        self.publish(subject, data, reply, message_id)
        try:
            deadline = time.monotonic() + timeout
            while True:
                self.socket.settimeout(max(0.2, deadline - time.monotonic()))
                response = self._read_event()
                # Pull-consumer deliveries may retain their original event
                # subject. The subscription SID, not the subject spelling,
                # identifies the inbox that received this response.
                if isinstance(response, Message) and response.sid == sid:
                    return response
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"no response to {subject}")
        finally:
            # A protocol rejection may already have closed the socket. Do not
            # hide the original failure with an unrelated BrokenPipe here.
            try:
                self._send(f"UNSUB {sid}\r\n".encode())
            except OSError:
                pass
            self.socket.settimeout(7.0)

    def api(self, subject: str, body: dict | None = None, timeout: float = 7.0) -> dict:
        answer = self.request(subject, json.dumps(body or {}).encode(), timeout)
        if answer.status:
            raise RuntimeError(f"{subject}: JetStream HTTP-style status {answer.status}")
        response = json.loads(answer.data)
        if response.get("error"):
            raise RuntimeError(f"{subject}: {response['error']}")
        return response


def server_binary(explicit: str | None, root: Path) -> Path:
    if explicit:
        binary = Path(explicit).expanduser().resolve()
    elif shutil.which("nats-server"):
        binary = Path(shutil.which("nats-server") or "").resolve()
    else:
        if platform.system() != "Linux" or platform.machine() not in ("x86_64", "amd64"):
            raise RuntimeError("auto-download supports Linux x86_64 only; provide --server PATH")
        package = root / "nats-server.tar.gz"
        print(f"Downloading pinned NATS Server v{NATS_VERSION} ({NATS_URL})", flush=True)
        urllib.request.urlretrieve(NATS_URL, package)
        with package.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        if digest != NATS_AMD64_SHA256:
            raise RuntimeError(f"NATS release SHA256 mismatch: {digest}")
        with tarfile.open(package, "r:gz") as archive:
            member = next((m for m in archive.getmembers() if m.name.endswith("/nats-server") and m.isfile()), None)
            if member is None:
                raise RuntimeError("nats-server binary missing from release archive")
            payload = archive.extractfile(member)
            if payload is None:
                raise RuntimeError("could not read release binary")
            binary = root / "nats-server"
            with binary.open("wb") as destination:
                shutil.copyfileobj(payload, destination)
            binary.chmod(0o755)
    result = subprocess.run([str(binary), "--version"], check=True, capture_output=True, text=True)
    if NATS_VERSION not in result.stdout:
        raise RuntimeError(f"expected NATS {NATS_VERSION}, found {result.stdout.strip()}")
    return binary


def config_text(name: str, ports: dict, storage: Path, routes: list[int]) -> str:
    lines = [
        f'server_name: "{name}"',
        f'listen: "127.0.0.1:{ports["client"]}"',
        f'http: "127.0.0.1:{ports["monitor"]}"',
        "jetstream {",
        f'  store_dir: "{storage}"',
        "  max_mem_store: 128Mb",
        "  max_file_store: 2Gb",
        "}",
    ]
    if routes:
        routes_text = ", ".join(f'"nats://127.0.0.1:{port}"' for port in routes)
        lines.extend([
            "cluster {",
            '  name: "JOHTO_NEW"',
            f'  listen: "127.0.0.1:{ports["route"]}"',
            f"  routes: [{routes_text}]",
            "}",
        ])
    return "\n".join(lines) + "\n"


def await_port(port: int, deadline: float = 18.0) -> None:
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        try:
            with Nats(port, timeout=1.0):
                return
        except (OSError, RuntimeError, ConnectionError):
            time.sleep(0.15)
    raise RuntimeError(f"NATS port {port} did not become ready")


def create_stream(client: Nats, replicas: int) -> dict:
    return client.api("$JS.API.STREAM.CREATE.EVENTS", {
        "name": "EVENTS", "subjects": ["events.>"], "storage": "file",
        "retention": "limits", "num_replicas": replicas,
        "duplicate_window": 600_000_000_000,
    }, timeout=8.0)


def create_consumer(client: Nats) -> dict:
    return client.api("$JS.API.CONSUMER.DURABLE.CREATE.EVENTS.BILLING", {
        "stream_name": "EVENTS",
        "config": {"durable_name": "BILLING", "ack_policy": "explicit", "deliver_policy": "all", "ack_wait": 30_000_000_000},
    })


def event_payload(seq: int) -> dict:
    return {
        "event_id": f"order-{seq:09d}",
        "order_id": f"acme-{seq:09d}",
        "amount_cents": 1400 + (seq * 37) % 8300,
        "event_type": "order.placed",
    }


def publish_jetstream(client: Nats, seq: int) -> dict:
    event = event_payload(seq)
    reply = client.request(
        "events.orders", json.dumps(event, separators=(",", ":")).encode(),
        message_id=event["event_id"], timeout=7.0,
    )
    answer = json.loads(reply.data)
    if answer.get("error") or answer.get("stream") != "EVENTS":
        raise RuntimeError(f"unexpected JetStream publish acknowledgment: {answer}")
    return {"event_id": event["event_id"], "stream_seq": answer["seq"], "timestamp": time.time()}


def pull_one(client: Nats) -> Message:
    response = client.request(
        "$JS.API.CONSUMER.MSG.NEXT.EVENTS.BILLING",
        b'{"batch":1,"expires":2500000000}', timeout=5.0,
    )
    if response.status:
        raise RuntimeError(f"consumer pull failed: {response.status} {response.headers!r}")
    if not response.reply:
        raise RuntimeError("consumer delivery had no acknowledgment reply")
    return response


def seed(root: Path, binary: Path) -> dict:
    ports = free_ports(11)
    old = {"client": ports[0], "monitor": ports[1]}
    cluster = [
        {"client": ports[2 + i], "monitor": ports[5 + i], "route": ports[8 + i]}
        for i in range(3)
    ]
    data = root / "data"
    data.mkdir(parents=True, exist_ok=True)
    entries = [("old", old, [])]
    for i, item in enumerate(cluster):
        entries.append((f"new-{i + 1}", item, [p["route"] for j, p in enumerate(cluster) if i != j]))
    processes: list[subprocess.Popen] = []
    publisher: subprocess.Popen | None = None
    try:
        for name, node_ports, routes in entries:
            storage = data / name
            storage.mkdir()
            conf = root / f"{name}.conf"
            conf.write_text(config_text(name, node_ports, storage, routes), encoding="utf-8")
            logfile = (root / f"{name}.log").open("ab")
            try:
                proc = subprocess.Popen(
                    [str(binary), "-c", str(conf)], cwd=root, stdin=subprocess.DEVNULL,
                    stdout=logfile, stderr=subprocess.STDOUT, start_new_session=True,
                )
            finally:
                logfile.close()
            processes.append(proc)
        for _, node_ports, _ in entries:
            await_port(node_ports["client"])
        with Nats(old["client"]) as src:
            create_stream(src, 1)
            for i in range(1, BASELINE_EVENTS + 1):
                publish_jetstream(src, i)
            create_consumer(src)
            for i in range(1, ACKED_EVENTS + 1):
                delivery = pull_one(src)
                payload = json.loads(delivery.data)
                if payload["event_id"] != event_payload(i)["event_id"]:
                    raise RuntimeError(f"unexpected old consumer delivery: {payload}")
                src.publish(delivery.reply or "", b"+ACK")
            src.flush()
            old_consumer = src.api("$JS.API.CONSUMER.INFO.EVENTS.BILLING")
            ack_floor = old_consumer["ack_floor"]["stream_seq"]
            for _ in range(25):
                if ack_floor == ACKED_EVENTS:
                    break
                time.sleep(0.1)
                ack_floor = src.api("$JS.API.CONSUMER.INFO.EVENTS.BILLING")["ack_floor"]["stream_seq"]
            if ack_floor != ACKED_EVENTS:
                raise RuntimeError(f"initial consumer acknowledgment floor {ack_floor}, expected {ACKED_EVENTS}")
        target_error = None
        for attempt in range(35):
            try:
                with Nats(cluster[0]["client"]) as dst:
                    create_stream(dst, 3)
                    info = dst.api("$JS.API.STREAM.INFO.EVENTS")
                    replicas = info["config"]["num_replicas"]
                    if replicas != 3 or info["state"]["messages"] != 0:
                        raise RuntimeError(f"target not empty R3 stream: {info}")
                target_error = None
                break
            except (OSError, RuntimeError, TimeoutError) as error:
                target_error = error
                time.sleep(0.25)
        if target_error:
            raise RuntimeError(f"target stream bootstrap failed: {target_error}")

        state = {
            "directory": str(root), "nats_version": NATS_VERSION,
            "source": old, "target_nodes": cluster,
            "servers": [{"name": row[0], "pid": proc.pid} for row, proc in zip(entries, processes)],
            "initial_events": BASELINE_EVENTS, "initially_acked": ACKED_EVENTS,
            "created_at": time.time(), "route_file": str(root / "traffic-route.json"),
            "producer_events": str(root / "producer-events.jsonl"),
        }
        atomic_json(root / "traffic-route.json", {"destination": "source"})
        atomic_json(root / "state.json", state)
        publisher_log = (root / "producer.log").open("ab")
        try:
            publisher = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "produce", str(root / "state.json")],
                cwd=root, stdin=subprocess.DEVNULL, stdout=publisher_log,
                stderr=subprocess.STDOUT, start_new_session=True,
            )
        finally:
            publisher_log.close()
        state["producer_pid"] = publisher.pid
        atomic_json(root / "state.json", state)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if (root / "producer-events.jsonl").exists() and (root / "producer-events.jsonl").stat().st_size:
                break
            if publisher.poll() is not None:
                raise RuntimeError(f"producer exited; read {root / 'producer.log'}")
            time.sleep(0.2)
        else:
            raise RuntimeError(f"producer did not send a confirmed event; read {root / 'producer.log'}")
        return state
    except BaseException:
        if publisher is not None and publisher.poll() is None:
            publisher.terminate()
        for proc in reversed(processes):
            if proc.poll() is None:
                proc.terminate()
        raise


def producer(state_file: Path) -> None:
    state = read_json(state_file)
    root = Path(state["directory"])
    sequence = state["initial_events"] + 1
    connection: Nats | None = None
    last_port = None
    try:
        while True:
            route = read_json(root / "traffic-route.json")["destination"]
            if route not in ("source", "target"):
                print(f"invalid route {route}; waiting", file=sys.stderr, flush=True)
                time.sleep(0.3)
                continue
            port = state["source"]["client"] if route == "source" else state["target_nodes"][0]["client"]
            if port != last_port:
                if connection:
                    connection.__exit__(None, None, None)
                connection = Nats(port)
                last_port = port
            try:
                assert connection is not None
                ack = publish_jetstream(connection, sequence)
                ack["destination"] = route
                append_jsonl(root / "producer-events.jsonl", ack)
                sequence += 1
            except (OSError, RuntimeError, TimeoutError, ValueError) as error:
                print(f"publish retry for {sequence}: {error}", file=sys.stderr, flush=True)
                if connection:
                    connection.__exit__(None, None, None)
                connection, last_port = None, None
            time.sleep(0.35)
    finally:
        if connection:
            connection.__exit__(None, None, None)


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def report(state_file: Path) -> dict:
    state = read_json(state_file)
    root = Path(state["directory"])
    result = {
        "directory": str(root),
        "route": read_json(root / "traffic-route.json")["destination"],
        "servers_alive": {s["name"]: alive(s["pid"]) for s in state["servers"]},
        "producer_alive": alive(state["producer_pid"]),
        "producer_confirmations": 0,
    }
    log = root / "producer-events.jsonl"
    if log.exists():
        result["producer_confirmations"] = sum(bool(line.strip()) for line in log.open(encoding="utf-8"))
    for side, port in (("source", state["source"]["client"]), ("target", state["target_nodes"][0]["client"])):
        try:
            with Nats(port) as connection:
                stream = connection.api("$JS.API.STREAM.INFO.EVENTS")
                result[f"{side}_stream"] = {
                    "messages": stream["state"]["messages"],
                    "last_seq": stream["state"]["last_seq"],
                    "replicas": stream["config"]["num_replicas"],
                }
                try:
                    consumer = connection.api("$JS.API.CONSUMER.INFO.EVENTS.BILLING")
                    result[f"{side}_consumer"] = {
                        "ack_floor": consumer["ack_floor"],
                        "delivered": consumer["delivered"],
                    }
                except RuntimeError as error:
                    result[f"{side}_consumer"] = str(error)
        except (OSError, RuntimeError, TimeoutError) as error:
            result[f"{side}_error"] = str(error)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    setup = sub.add_parser("setup")
    setup.add_argument("--server", help=f"path to official nats-server v{NATS_VERSION} executable")
    status = sub.add_parser("status")
    status.add_argument("state", type=Path)
    prod = sub.add_parser("produce", help=argparse.SUPPRESS)
    prod.add_argument("state", type=Path)
    args = parser.parse_args()
    if args.command == "setup":
        root = Path(tempfile.mkdtemp(prefix="johto-nats-"))
        binary = server_binary(args.server, root)
        state = seed(root, binary)
        print("NATS migration lab live. Original data and durable consumer still on standalone source.")
        print(f"State: {root / 'state.json'}")
        print(f"Logs/data/configs: {root}")
        print(json.dumps(report(root / "state.json"), indent=2))
    elif args.command == "status":
        print(json.dumps(report(args.state), indent=2))
    else:
        producer(args.state)


if __name__ == "__main__":
    main()
