# Kraken v2 L2 order-book synchronizer

A public-data WebSocket v2 client and independent Decimal-based order-book state
machine. The default command runs offline tests. A live connection requires an
explicit `--live` flag; importing modules never connects. No account credentials,
order placement, risk sizing or trading methods are implemented.

## Build and offline tests

```sh
docker build -t kraken-book .
docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges:true --tmpfs /tmp:rw,nosuid,nodev,size=67108864 kraken-book
```

Manual public-data use, only when network activation is intended:

```sh
docker run --rm kraken-book python3 client.py --live --symbol BTC/USD --depth 10
```

This command is documentation, not an activated service or proof of live operation.
The local qualification uses the published Kraken checksum vector and fake sockets.

## Integrity state machine

AWAITING_SNAPSHOT -> SYNCED after a valid complete snapshot. A checksum mismatch,
malformed/crossed book, invalid update, stale view, disconnect or subscription error
invalidates the book. The client reconnects with bounded exponential backoff and
requests a fresh snapshot; stale data is never published as synchronized.

Decode JSON numbers using Decimal, preserving trailing-zero precision. Apply all
updates in message order, including repeated prices; qty zero removes a level.
Truncate to subscribed depth. Compute CRC32 over the top ten asks ascending then
top ten bids descending, removing decimal points and leading zeros from each price
and quantity. Compare the unsigned 32-bit result before committing the update.
The official example's expected CRC32 is 3310070434.

## Protocol limits

The v2 book channel does NOT define a sequence-number field. Consequently this
implementation does not invent sequence-gap detection. It resynchronizes on
observable checksum/connection/protocol/freshness failures. CRC32 covers only the
top ten levels, regardless of subscription depth; a missed deeper-level change is
not necessarily detectable. This is L2 aggregated price-level data, not L3 orders.
No low-latency benchmark, production SLA, financial edge or live-feed qualification
is claimed. Freshness uses monotonic receipt time, not exchange clock accuracy.

Tests cover precision, the independent reference vector, repeated updates, deletes,
depth truncation, malformed input, stale-state denial, atomic failure and mocked
reconnection/resnapshot behavior. Dependencies: Python 3.11+, websockets 15.0.1.
CI repeats the offline Docker tests and never enables a live feed.

Official protocol: https://docs.kraken.com/api/docs/websocket-v2/book/
Checksum: https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2

No first-party license grant is included. Third-party components retain their own
licenses. No background service is installed automatically.
