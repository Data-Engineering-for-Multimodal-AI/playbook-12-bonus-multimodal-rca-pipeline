"""
Stage 2: Streaming. Moves sensor events from the line to the stream processor.

memory: an in-process queue (the default; no services needed).
kafka:  a Kafka topic (docker compose up -d kafka). Events are JSON messages keyed by cycle counter.
"""

import json
import threading
import time
import uuid
from typing import Dict, Iterable, Iterator


class MemoryTransport:
    name = "memory"

    def __init__(self):
        self.published = 0
        self.bytes = 0

    def stream(self, events: Iterable[Dict], speed: float = 0.0) -> Iterator[Dict]:
        return _paced(self._count(events), speed)

    def _count(self, events: Iterable[Dict]) -> Iterator[Dict]:
        for event in events:
            self.published += 1
            yield event


class KafkaTransport:
    name = "kafka"

    def __init__(self, bootstrap_servers: str, topic: str):
        from confluent_kafka import Consumer, Producer
        from confluent_kafka.admin import AdminClient, NewTopic

        run = uuid.uuid4().hex[:8]
        self.topic = f"{topic}.{run}"
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        futures = admin.create_topics([NewTopic(self.topic, num_partitions=1, replication_factor=1)])
        futures[self.topic].result(timeout=30)
        self.producer = Producer({"bootstrap.servers": bootstrap_servers, "linger.ms": 20,
                                  "compression.type": "lz4", "queue.buffering.max.messages": 500_000})
        self.consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": f"ff-rca-{run}",
                                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
        self.published = 0
        self.bytes = 0

    def _publish(self, events: Iterable[Dict], speed: float) -> None:
        for event in _paced(events, speed):
            payload = json.dumps(event).encode()
            while True:
                try:
                    self.producer.produce(self.topic, payload, key=str(event["Q_Cell_CycleCount"]))
                    break
                except BufferError:
                    self.producer.poll(0.05)
            self.producer.poll(0)
            self.published += 1
            self.bytes += len(payload)
        self.producer.produce(self.topic, b'{"_end": true}')
        self.producer.flush(60)

    def stream(self, events: Iterable[Dict], speed: float = 0.0) -> Iterator[Dict]:
        """Publish from a background thread while consuming here, as two services would."""
        publisher = threading.Thread(target=self._publish, args=(events, speed), daemon=True)
        publisher.start()
        self.consumer.subscribe([self.topic])
        try:
            while True:
                message = self.consumer.poll(1.0)
                if message is None:
                    if not publisher.is_alive() and self.published == 0:
                        raise RuntimeError("Nothing was published to Kafka")
                    continue
                if message.error():
                    raise RuntimeError(message.error())
                event = json.loads(message.value())
                if event.get("_end"):
                    return
                yield event
        finally:
            self.consumer.close()
            publisher.join(timeout=60)


def _paced(events: Iterable[Dict], speed: float) -> Iterator[Dict]:
    """speed 0: as fast as possible; speed N: N times real time, by the events' timestamps."""
    if not speed:
        yield from events
        return
    started = first = None
    for event in events:
        if first is None:
            started, first = time.perf_counter(), event["ts_ms"]
        wait = (event["ts_ms"] - first) / 1000 / speed - (time.perf_counter() - started)
        if wait > 0:
            time.sleep(wait)
        yield event


def make_transport(kind: str, config: Dict):
    if kind == "memory":
        return MemoryTransport()
    if kind == "kafka":
        return KafkaTransport(config["kafka"]["bootstrap_servers"], config["kafka"]["topic"])
    raise ValueError(f"Unknown transport {kind!r}: memory or kafka")
