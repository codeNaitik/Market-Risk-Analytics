"""Live crypto trades: Coinbase Exchange WebSocket -> Kafka.

Subscribes to the public `matches` channel (every executed trade, no API key) and
publishes each trade to Kafka keyed by product_id, so all trades for one pair land
in the same partition and stay ordered.

Production behaviours worth pointing out in an interview:
  * idempotent producer (enable.idempotence) -> no duplicates on broker retries
  * reconnect with exponential backoff when the socket drops
  * delivery callbacks + periodic throughput log (msgs/sec)
  * --simulate mode generates realistic random-walk trades for offline demos/CI

Run:
    python -m mra.streaming.crypto_producer             # live Coinbase feed
    python -m mra.streaming.crypto_producer --simulate  # no internet needed
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import signal
import time
from datetime import UTC, datetime

from mra.config import get_settings
from mra.logging_utils import get_logger

log = get_logger(__name__)


class KafkaSink:
    def __init__(self, bootstrap: str, topic: str):
        from confluent_kafka import Producer  # lazy import: not needed for batch-only users

        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,             # small batching window -> higher throughput
            "compression.type": "lz4",
        })
        self.sent = self.failed = 0
        self._window_start, self._window_count = time.monotonic(), 0

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed: %s", err)

    def send(self, key: str, value: dict) -> None:
        self.producer.produce(self.topic, key=key.encode(), value=json.dumps(value).encode(),
                              on_delivery=self._on_delivery)
        self.producer.poll(0)
        self.sent += 1
        self._window_count += 1
        elapsed = time.monotonic() - self._window_start
        if elapsed >= 30:
            log.info("throughput: %.1f msgs/sec | total sent=%d failed=%d",
                     self._window_count / elapsed, self.sent, self.failed)
            self._window_start, self._window_count = time.monotonic(), 0

    def close(self) -> None:
        self.producer.flush(10)


async def stream_coinbase(sink: KafkaSink, url: str, products: list[str], stop: asyncio.Event) -> None:
    import websockets

    subscribe = {"type": "subscribe", "product_ids": products, "channels": ["matches", "heartbeat"]}
    backoff = 1
    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, max_size=2**22) as ws:
                await ws.send(json.dumps(subscribe))
                log.info("subscribed to %s on %s", products, url)
                backoff = 1
                while not stop.is_set():
                    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                    if msg.get("type") in ("match", "last_match"):
                        msg["_received_at"] = datetime.now(UTC).isoformat()
                        sink.send(msg["product_id"], msg)
                    elif msg.get("type") == "error":
                        log.error("coinbase error: %s", msg)
        except Exception as exc:  # noqa: BLE001 - any socket/parse failure -> reconnect
            if stop.is_set():
                break
            log.warning("socket dropped (%s); reconnecting in %ss", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def simulate(sink: KafkaSink, products: list[str], stop: asyncio.Event, rate: float = 50.0) -> None:
    """Random-walk trades with the same JSON shape as Coinbase `match` messages."""
    prices = {"BTC-USD": 95_000.0, "ETH-USD": 3_400.0, "SOL-USD": 180.0}
    trade_id = 0
    log.info("SIMULATED feed: ~%.0f trades/sec for %s", rate, products)
    while not stop.is_set():
        pid = random.choice(products)
        prices[pid] = prices.get(pid, 100.0) * (1 + random.gauss(0, 0.0004))
        trade_id += 1
        sink.send(pid, {
            "type": "match", "trade_id": trade_id, "product_id": pid,
            "side": random.choice(["buy", "sell"]), "price": f"{prices[pid]:.2f}",
            "size": f"{random.expovariate(1 / 0.05):.8f}",
            "time": datetime.now(UTC).isoformat(), "sequence": trade_id, "_simulated": True,
            "_received_at": datetime.now(UTC).isoformat(),
        })
        await asyncio.sleep(random.expovariate(rate))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--simulate", action="store_true")
    p.add_argument("--rate", type=float, default=50.0, help="simulated trades/sec")
    args = p.parse_args()

    s = get_settings()
    cfg = s.raw["streaming"]
    sink = KafkaSink(s.kafka_bootstrap, cfg["kafka_topic"])
    stop = asyncio.Event()

    async def runner():
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        if args.simulate:
            await simulate(sink, cfg["product_ids"], stop, args.rate)
        else:
            await stream_coinbase(sink, cfg["websocket_url"], cfg["product_ids"], stop)

    try:
        asyncio.run(runner())
    finally:
        sink.close()
        log.info("producer stopped: sent=%d failed=%d", sink.sent, sink.failed)


if __name__ == "__main__":
    main()
