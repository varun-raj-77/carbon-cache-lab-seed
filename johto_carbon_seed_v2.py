#!/usr/bin/env python3
"""Reproduce a three-cache Carbon expansion with an ongoing metric producer.

Setup is isolated to a new temporary directory and localhost services. The
checkout is not modified. Run this from the pinned Carbon repository with
Carbon and Graphite Whisper installed in the current Python environment.

    python /path/to/johto_carbon_seed_v2.py
    python /path/to/johto_carbon_seed_v2.py --check /tmp/johto-carbon-.../state.json

The metric producer deliberately remains active after setup exits so that a
later operator works on a deployment that is still accepting application data.
All processes are local to the isolated Docker lane.
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
METRIC_COUNT = 100
PRODUCER_INTERVAL_SECONDS = 2.0
PORTS = (2013, 2014, 2203, 2204, 2303, 2304, 2403, 2404,
         7202, 7302, 7402)


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
        with socket.create_connection(('127.0.0.1', port), timeout=0.25):
            return True
    except OSError:
        return False


def launch(lab, config_path, kind, instance):
    env = dict(os.environ)
    env['GRAPHITE_STORAGE_DIR'] = str(
        lab / instance if kind == 'cache' else lab / 'relay'
    )
    command = [
        sys.executable,
        str(REPO / 'bin' / f'carbon-{kind}.py'),
        f'--config={config_path}',
        f'--instance={instance}',
        f'--pidfile={lab / (kind + '-' + instance + '.pid')}',
        '--debug',
        'start',
    ]
    log_path = lab / f'{kind}-{instance}-{config_path.stem}.log'
    with log_path.open('w', encoding='utf-8') as logfile:
        return subprocess.Popen(
            command, cwd=REPO, env=env,
            stdout=logfile, stderr=subprocess.STDOUT,
            start_new_session=True,
        )


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
    payload = ''.join(
        f'traffic.service.m{i} {value} {timestamp}\n'
        for i in range(METRIC_COUNT)
    )
    with socket.create_connection(('127.0.0.1', 2013), timeout=5) as sock:
        sock.sendall(payload.encode('ascii'))


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
    for i in range(METRIC_COUNT):
        name = f'm{i}.wsp'
        if not any(
            at_timestamp(
                lab / node / 'whisper' / 'traffic' / 'service' / name,
                timestamp,
            ) == expected
            for node in ('a', 'b', 'c')
        ):
            return False
    return True


def run_producer(lab):
    """Stream new readings to the relay until this local process is stopped."""
    events = lab / 'producer-events.jsonl'
    sequence = 0
    while True:
        sent_at = time.time()
        timestamp = int(sent_at // 10) * 10
        # Distinct values make consecutive batches recognizable in the
        # producer log and allow a later operator to check what was transmitted.
        value = float(1000 + sequence)
        event = {
            'sequence': sequence,
            'sent_at': sent_at,
            'timestamp': timestamp,
            'value': value,
            'metric_count': METRIC_COUNT,
        }
        try:
            send_series(value, timestamp)
            event['status'] = 'transmitted'
        except OSError as exc:
            event['status'] = 'connection_failed'
            event['error'] = str(exc)
        with events.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(event, sort_keys=True) + '\n')
        sequence += 1
        time.sleep(PRODUCER_INTERVAL_SECONDS)


def producer_events(lab):
    events = lab / 'producer-events.jsonl'
    if not events.exists():
        return []
    with events.open('r', encoding='utf-8') as handle:
        return [json.loads(line) for line in handle if line.strip()]


def launch_producer(lab):
    log = lab / 'producer.log'
    with log.open('w', encoding='utf-8') as outfile:
        return subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), '--stream', str(lab)],
            cwd=REPO, stdout=outfile, stderr=subprocess.STDOUT,
            start_new_session=True,
        )


def check(lab_state):
    state = json.loads(lab_state.read_text(encoding='utf-8'))
    lab = Path(state['directory'])
    events = producer_events(lab)
    successes = [event for event in events
                 if event['status'] == 'transmitted']
    try:
        os.kill(int(state['processes']['producer']), 0)
        producer_alive = True
    except (OSError, ValueError):
        producer_alive = False
    freshness = (time.time() - successes[-1]['sent_at']) if successes else None
    latest_failed = bool(events and events[-1]['status'] != 'transmitted')
    result = {
        'deployment': str(lab),
        'producer_alive': producer_alive,
        'transmitted_batches': len(successes),
        'connection_failures': len(events) - len(successes),
        'seconds_since_last_transmission': (
            round(freshness, 1) if freshness is not None else None
        ),
        'latest_batch_failed': latest_failed,
        'most_recent_timestamp': successes[-1]['timestamp'] if successes else None,
        'relay_line_port_open': port_open(2013),
        'cache_pickle_ports_open': {
            node: port_open(port)
            for node, port in (('a', 2204), ('b', 2304), ('c', 2404))
        },
    }
    print(json.dumps(result, indent=2))
    if (
        not producer_alive
        or not result['relay_line_port_open']
        or not all(result['cache_pickle_ports_open'].values())
        or freshness is None
        or freshness > 15
        or latest_failed
    ):
        raise SystemExit('Deployment requires investigation before use')


def main():
    if not (REPO / 'bin' / 'carbon-cache.py').is_file():
        raise SystemExit('Run from the pinned Carbon repository root')
    occupied = [port for port in PORTS if port_open(port)]
    if occupied:
        raise SystemExit(f'Local ports already in use: {occupied}; use a clean lane')

    lab = Path(tempfile.mkdtemp(prefix='johto-carbon-v2-'))
    config_dir = lab / 'conf'
    config_dir.mkdir()
    # Two standard retention levels are representative of common Graphite
    # deployments and preserve both granular and consolidated history.
    (config_dir / 'storage-schemas.conf').write_text(
        '[default]\npattern = .*\nretentions = 10s:6h,1m:7d\n',
        encoding='utf-8',
    )
    two_nodes = config_dir / 'carbon.conf'
    three_nodes = config_dir / 'carbon-three.conf'
    two_nodes.write_text(
        config('127.0.0.1:2204:a, 127.0.0.1:2304:b'),
        encoding='utf-8',
    )
    three_nodes.write_text(
        config('127.0.0.1:2204:a, 127.0.0.1:2304:b, 127.0.0.1:2404:c'),
        encoding='utf-8',
    )

    processes = []
    try:
        for instance, port in (('a', 2204), ('b', 2304)):
            process = launch(lab, two_nodes, 'cache', instance)
            processes.append(process)
            if not wait_for(lambda p=port: port_open(p), 15):
                raise RuntimeError(f'Carbon cache {instance} did not start')

        relay = launch(lab, two_nodes, 'relay', 'a')
        processes.append(relay)
        if not wait_for(lambda: port_open(2013), 15):
            raise RuntimeError('Initial relay did not start')

        clock = int(time.time() // 10) * 10
        baseline, before_cutover, after_cutover = clock - 40, clock - 19, clock - 12
        send_series(1.0, baseline)
        if not wait_for(lambda: all_series_present(lab, baseline, 1.0)):
            raise RuntimeError('Initial metric data did not reach the caches')

        send_series(3.0, before_cutover)
        if not wait_for(lambda: all_series_present(lab, before_cutover, 3.0)):
            raise RuntimeError('Pre-cutover data did not reach the caches')

        third = launch(lab, three_nodes, 'cache', 'c')
        processes.append(third)
        if not wait_for(lambda: port_open(2404), 15):
            raise RuntimeError('Third cache did not start')

        stop_own_process(relay)
        if not wait_for(lambda: not port_open(2013), 15):
            raise RuntimeError('Original relay did not stop cleanly')
        processes.remove(relay)
        relay = launch(lab, three_nodes, 'relay', 'a')
        processes.append(relay)
        if not wait_for(lambda: port_open(2013), 15):
            raise RuntimeError('Expanded relay did not start')

        send_series(0.0, after_cutover)
        if not wait_for(lambda: all_series_present(lab, after_cutover, 0.0)):
            raise RuntimeError('Post-cutover data did not reach the caches')

        files_on_c = list((lab / 'c' / 'whisper' / 'traffic' / 'service').glob('*.wsp'))
        if len(files_on_c) < 10:
            raise RuntimeError('Third cache has too few metric files')

        producer = launch_producer(lab)
        processes.append(producer)
        if not wait_for(
            lambda: len([e for e in producer_events(lab)
                         if e['status'] == 'transmitted']) >= 2,
            timeout=20,
        ):
            raise RuntimeError('Continuing metric producer did not start')
        if producer.poll() is not None:
            raise RuntimeError('Continuing metric producer exited unexpectedly')

        state = {
            'directory': str(lab),
            'configuration': str(three_nodes),
            'relay_port': 2013,
            'cache_ports': {'a': 2204, 'b': 2304, 'c': 2404},
            'metrics': f'traffic.service.m0 through traffic.service.m{METRIC_COUNT-1}',
            'timestamps': [baseline, before_cutover, after_cutover],
            'producer_events': str(lab / 'producer-events.jsonl'),
            'processes': {
                'cache_a': processes[0].pid,
                'cache_b': processes[1].pid,
                'cache_c': third.pid,
                'relay': relay.pid,
                'producer': producer.pid,
            },
        }
        (lab / 'state.json').write_text(
            json.dumps(state, indent=2), encoding='utf-8'
        )
        print('Carbon test deployment is running with three caches and a relay.')
        print('A separate metric producer continues transmitting after setup.')
        print(f'State and service logs: {lab}')
        print(f'Setup manifest: {lab / "state.json"}')
        print(f'Producer activity: {lab / "producer-events.jsonl"}')
        print('Metrics: traffic.service.m0 through traffic.service.m99')
    except Exception:
        for process in reversed(processes):
            stop_own_process(process)
        print(f'Setup failed. Inspect service logs in {lab}', file=sys.stderr)
        raise


if __name__ == '__main__':
    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 3 and sys.argv[1] == '--stream':
        run_producer(Path(sys.argv[2]))
    elif len(sys.argv) == 3 and sys.argv[1] == '--check':
        check(Path(sys.argv[2]))
    else:
        raise SystemExit('Usage: seed.py [--check STATE_JSON]')
