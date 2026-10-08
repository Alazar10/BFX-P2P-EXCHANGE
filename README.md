# P2P Matching Engine

## Runtime configuration

The engine requires an etcd v3 cluster and explicit secrets; it does not ship with usable defaults:

- `ETCD_ENDPOINTS`: comma-separated etcd v3 JSON gateway URLs. Remote endpoints must use HTTPS; plain HTTP is accepted only for localhost development.
- `ETCD_AUTH_TOKEN`: required for remote etcd endpoints. Configure an etcd user with permissions limited to the engine's key prefix and issue a distinct token per node.
- `ETCD_PREFIX`: optional isolated key prefix; defaults to `/p2p-exchange/v1`.
- `CLIENT_API_KEYS`: a JSON object mapping each API key to its own unique secret, for example `{"trader-a":"<secret-a>","trader-b":"<secret-b>"}`. Each secret must be at least 32 characters. Configure the same mapping on every engine node.
- `NODE_ID`: this node's unique ID.
- `GRAPE_URL` and `PORT`: the node's Grape endpoint and listening port.
- Node.js 20 or later is required.

Start the engine after exporting those values with `npm start`. Client programs must receive only their own `API_KEY` and `API_SECRET`; the pair must match an entry in `CLIENT_API_KEYS`. Never put a peer secret in a client or commit any real secret to the repository.

## Order input and numeric guarantees

Client order IDs, prices, and amounts must be canonical unsigned decimal strings (no signs, decimal points, exponent notation, or leading zeroes), with at most 256 digits. Prices and amounts must be greater than zero. Every request must include a UUID v4 `requestId`, reused on retries. The API rejects unknown fields, unsafe JavaScript numbers, invalid sides, and requests that disable self-trade prevention. User identity is derived from the authenticated API key; a client-supplied user ID is not accepted.

The order pool uses preallocated columns for active-order fields and preserves values as JavaScript `BigInt`, including values larger than 64 bits. This is not a zero-allocation engine: `BigInt` arithmetic, matching fill records, maps, and price-level updates allocate. Benchmark and profile the target workload before making latency or GC guarantees.

## Deployment limitations

The in-repository Raft-like path is no longer used for order ordering. All engine nodes append commands through an etcd transaction that atomically advances the sequence and records the command; nodes recover by replaying that ordered log. Deploy a properly secured, odd-sized etcd cluster (normally three or five voting members) and preserve its quorum. The etcd command log is currently retained indefinitely; compaction and snapshot-based etcd log retention are not implemented. Existing local WAL/snapshot state from the prior protocol must be migrated before switching: startup rejects an etcd sequence that is behind local applied state.

RPC frames use MessagePack over WebSocket with a 1 MiB frame cap. Client payloads and etcd access are authenticated, but configure HTTPS, etcd authentication, and network policy for all remote links. The polling-based recovery loop and local in-process tests do not replace live multi-node partition, failover, recovery, and load testing.

## Test scripts

- `npm test` runs the in-process master regression suite. Its etcd ordering checks use a mock etcd API.
- `node src/test-distributed-divergence.js` runs a legacy in-process Raft simulation with isolated temporary WAL files. It does not test the production etcd ordering path.
- `node src/test-race-conditions.js` is an integration test: start the etcd-backed engine and Grenache network first, then set `API_KEY` and `API_SECRET` to a client account present in the engine's `CLIENT_API_KEYS`. Set `GRAPE_URL` if Grape is not at `http://127.0.0.1:30001`.
