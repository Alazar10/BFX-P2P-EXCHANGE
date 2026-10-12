# Project Guide: P2P Matching Engine

This is a file-by-file guide to the current repository. It explains the runtime path, what each module is responsible for, how to run the available workflows, and which older components are simulations rather than production dependencies.

## 1. What the project does

The application accepts authenticated order-create and order-cancel RPC requests, appends each accepted command to an etcd-backed ordered log, then applies committed commands in sequence to a local in-memory order book. Each engine node replays that shared log into its own local state. A local write-ahead log (WAL) and snapshot support restart recovery.

At a high level:

```text
Client
  -> HMAC-authenticated command
  -> Grenache DHT lookup + MessagePack/WebSocket RPC
  -> src/index.js request handler
  -> schema/numeric validation and API-key identity
  -> etcd transaction (sequence CAS + command + idempotency keys)
  -> local catch-up from etcd ordered log
  -> Sequencer WAL append
  -> deterministic MatchingEngine / OrderBook mutation
  -> RPC response with sequence and fills
```

The Grenache network routes RPCs; it does not establish consensus. In the current production entry point, **etcd is the ordering authority**. The custom Raft-like implementation and synchronization manager remain in the repository for legacy simulations and are not wired into `src/index.js`.

## 2. How to run it

### Prerequisites

- Node.js 20.6 or later.
- Dependencies installed with `npm ci`.
- An etcd v3 server/cluster reachable at `ETCD_ENDPOINTS`.
- Grenache Grape node(s) reachable at `GRAPE_URL`.
- A client API key and secret configured in `CLIENT_API_KEYS`.

### Local development

1. Copy [`.env.example`](./.env.example) to `.env`.
2. Generate a fresh secret, then set it identically in `API_SECRET` and the JSON value for that key in `CLIENT_API_KEYS`. Do not commit `.env`.
3. Use an `ETCD_PREFIX` and `NODE_ID` that are new or already belong to this node's matching state. Do not change the prefix while reusing a node's WAL/snapshot.
4. Start etcd and Grape.
5. In the project directory, run:

   ```bash
   npm ci
   npm start
   ```

6. In another terminal, run the integration race test:

   ```bash
   npm run test:race
   ```

Both `npm start` and `npm run test:race` load `.env`. Values already exported in the shell take precedence over values in that file. `npm test` is self-contained and does not load `.env`.

### Other useful commands

```bash
npm test
node src/test-distributed-divergence.js
node src/test-safe-synchronization.js
node src/test-compaction.js
```

The last three are standalone development simulations. Read their descriptions below before running them: they do not all test the production etcd path, and the safe-synchronization test deletes its named sandbox directories before use.

## 3. Core concepts and data

### Order command

- `ORDER_CREATE`: requires a positive canonical decimal order ID, price and amount; side `0` (buy) or `1` (sell); a supported self-trade-prevention mode; and a UUID v4 request ID at the client boundary.
- `ORDER_CANCEL`: requires an order ID and request ID. The server derives the user ID from the authenticated API key and applies ownership checks when the order is present.
- Monetary values are integer units, not decimal floating point. Use decimal strings at the RPC boundary; the engine converts them to `BigInt`. Inputs are capped at 256 digits.

### Order book

- Bids are sorted highest-price first; asks lowest-price first.
- Every price level contains a FIFO queue of order slots and aggregate remaining volume.
- Matching consumes the best eligible opposing price, in FIFO order within a price level.
- Self-trade prevention is applied before a fill is emitted.

### etcd ordering and local durability

For a submission, `EtcdOrderLog` reads the current head and atomically compares and writes the next sequence, immutable log entry, request-ID idempotency record, and (for creates) permanent order-ID record. Nodes poll and replay log entries in sequence through `Sequencer`. Each node's WAL/snapshot is local durability and acceleration; it is not a substitute for the shared etcd log.

Use the same etcd prefix and compatible log history across nodes in one deployment. If local sequence is ahead of etcd, startup rejects the state rather than silently resetting or forking it. Do not delete local persistence or clear etcd as a way to bypass that guard.

## 4. Root files

| File | Purpose / when to use |
|---|---|
| [`README.md`](./README.md) | Short setup/configuration overview, input rules, limitations and test commands. Start here for the current operational requirements. |
| [`PROJECT_GUIDE.md`](./PROJECT_GUIDE.md) | This expanded, file-by-file guide. |
| [`.env.example`](./.env.example) | Safe template for local configuration. Copy it to `.env`, replace placeholders and align the API secret in both settings. |
| [`.env`](./.env) | Local-only credentials/configuration. It is Git-ignored; never share or commit it. |
| [`.gitignore`](./.gitignore) | Excludes dependency installs, runtime `data/`, WAL files and `.env` from Git. |
| [`package.json`](./package.json) | Node version, runtime dependencies and npm scripts (`start`, `test`, `test:race`). |
| [`package-lock.json`](./package-lock.json) | Exact dependency resolution for repeatable `npm ci` installs. Keep it in sync with `package.json`. |
| [`production_engine.wal`](./production_engine.wal) | Existing root-level WAL-shaped binary/data artifact, not referenced by the current `src/index.js` path. Treat it as potentially valuable state; do not delete or edit without establishing its provenance. |

## 5. Runtime and client files

| File | Responsibility / use case |
|---|---|
| [`src/index.js`](./src/index.js) | Production process entry point. Loads environment values (via npm's Node `--env-file` script), validates client-key configuration, constructs the pool/book/engine/WAL/sequencer, wires authenticated RPC requests to `EtcdOrderLog`, starts etcd catch-up before announcing the RPC service, and handles shutdown. |
| [`src/client.js`](./src/client.js) | Example RPC client for a maker/taker workflow and a self-trade-prevention attempt. Requires client API credentials in its process environment. Its historical default Grape URL is `http://127.0.0.1:40001`; set `GRAPE_URL` explicitly to the Grape API used by the engine when running it manually. Its catch path currently exits with status 0 even after an RPC error, so inspect output rather than trusting only the exit code. |
| [`src/benchmark.js`](./src/benchmark.js) | Intended RPC throughput/latency load driver. It currently calls `crypto.randomUUID()` without importing `node:crypto`, so it fails when it attempts to build the first request; add that import before using it. Once fixed, it sends signed creates concurrently and reports successful requests, failures, throughput and latency percentiles. This is a workload tool, not a benchmark guarantee or production load profile. It defaults to Grape `:40001`; explicitly set `GRAPE_URL` for the local setup. |

## 6. Matching engine and input validation

| File | Responsibility / use case |
|---|---|
| [`src/engine/Integer.js`](./src/engine/Integer.js) | Converts canonical unsigned decimal strings or `BigInt` into bounded exact integers. Rejects signs, decimals, exponent notation, leading zeros and values over 256 digits. |
| [`src/engine/CommandValidation.js`](./src/engine/CommandValidation.js) | Strict client-facing command schema: allowed fields, command types, UUID v4 request IDs, positive numeric values, side and mandatory STP policy. Called by the server before a command enters the ordered log. |
| [`src/engine/MatchingEngine.js`](./src/engine/MatchingEngine.js) | Deterministic matching algorithm, fill construction and self-trade-prevention handling. Operates on the opposing book's best price level and rests unfilled quantity. |
| [`src/engine/OrderBook.js`](./src/engine/OrderBook.js) | In-memory price-level maps, sorted price arrays and order-ID-to-pool-slot lookup. Adds/cancels resting orders and maintains price-level volume. |
| [`src/engine/DoublyLinkedList.js`](./src/engine/DoublyLinkedList.js) | Intrusive FIFO queue operations per price level. Uses slot `prev`/`next` pointers for constant-time append/unlink. |
| [`src/memory/OrderPool.js`](./src/memory/OrderPool.js) | Fixed-capacity order-slot arena and free list. Numeric fields use JS arrays to preserve `BigInt`; typed arrays hold side, slot lifecycle flags and pointers. It preallocates storage but does not make the entire engine allocation-free. |

## 7. Ordering, persistence and recovery

| File | Responsibility / use case |
|---|---|
| [`src/consensus/EtcdOrderLog.js`](./src/consensus/EtcdOrderLog.js) | Current production ordering adapter. Uses etcd v3 JSON-gateway requests and transactions for sequence CAS, command append, idempotency, order-ID uniqueness, endpoint failover, catch-up and polling. Verify against a real secured etcd deployment; the master suite uses a mock. |
| [`src/consensus/Sequencer.js`](./src/consensus/Sequencer.js) | Validates committed commands, checks strict next-sequence execution, appends each applied entry to local WAL, mutates the matching state machine, and periodically checkpoints. Shared by the current ordered-log path and legacy simulations. |
| [`src/consensus/WriteAheadLog.js`](./src/consensus/WriteAheadLog.js) | Length-prefixed JSON record WAL with append/fsync, replay and reset operations. `Sequencer` uses it for locally applied commands; the legacy Raft implementation also stores prepare/vote records. |
| [`src/consensus/SnapshotManager.js`](./src/consensus/SnapshotManager.js) | Serializes resting orders and sequence to an atomic snapshot, hydrates the book/pool from snapshots, installs transferred snapshots, and compacts local WAL records. It is local node state, not the etcd command log. |
| [`src/consensus/RaftPeerNode.js`](./src/consensus/RaftPeerNode.js) | Earlier custom Raft-like peer implementation: elections, heartbeats, staged proposals, quorum acknowledgements and commit messages. It is **not used by `src/index.js`** and must not be treated as the production order sequencer. |
| [`src/consensus/SyncManager.js`](./src/consensus/SyncManager.js) | Earlier leader/follower delta-or-snapshot synchronization utility, with pause/resume and snapshot checksum checks. It is not wired into the current production service; current recovery is `EtcdOrderLog` catch-up. |

## 8. Network and security

| File | Responsibility / use case |
|---|---|
| [`src/network/Security.js`](./src/network/Security.js) | HMAC-SHA256 client request signing/verification, timestamp-drift and monotonic-nonce checks, API-key-to-user identity derivation, and optional peer-envelope signing/replay protection for the legacy peer protocol. |
| [`src/network/BinaryRPCTransports.js`](./src/network/BinaryRPCTransports.js) | Grenache WebSocket transport subclasses that encode/decode MessagePack frames, cap frame size at 1 MiB and disable WebSocket per-message compression. |
| [`src/network/GrenacheTransport.js`](./src/network/GrenacheTransport.js) | Starts the Grenache link and binary RPC server, announces service keys, dispatches requests to the registered handler, and provides RPC forwarding/shutdown support. In the current `index.js` path it is ingress/service discovery, not consensus. |

Client HMACs provide payload authentication/integrity at the application boundary. This project does not configure TLS for its Grenache WebSocket server/client; secure remote deployments with network controls/TLS termination as appropriate. etcd endpoints have separate HTTPS/token checks in `EtcdOrderLog`.

## 9. Test and diagnostic files

| File | What it exercises / caveats |
|---|---|
| [`src/test-master-suite.js`](./src/test-master-suite.js) | Main `npm test` harness. Nine groups cover split fills/STP, cancellation/replay behavior, a small in-process throughput run, WAL/snapshot recovery, legacy Raft convergence, legacy snapshot synchronization, wide integers/schema validation, MessagePack/HMAC behavior, and a mocked concurrent etcd ordering/catch-up case. The etcd case is not a real etcd cluster test. |
| [`src/test-race-conditions.js`](./src/test-race-conditions.js) | Live RPC integration script; `npm run test:race` runs it with `.env`. Exercises concurrent cancels, match-vs-cancel, and lower/replayed nonce rejection. It requires running etcd, Grape and engine. The current script logs the cancel race outcome but does not inspect final book state; it is a smoke test, not a proof of every race invariant. |
| [`src/test-distributed-divergence.js`](./src/test-distributed-divergence.js) | Three in-process nodes and a local mesh transport exercise 1,000 transactions using the legacy custom Raft-like code and compare book hashes. It does not exercise production etcd or real network failure modes. It creates temporary test data and removes that directory at exit. |
| [`src/test-safe-synchronization.js`](./src/test-safe-synchronization.js) | Mock-transport leader/follower test for delta/snapshot recovery and book-hash convergence using `SyncManager`. It deletes `data/leader_sandbox` and `data/follower_sandbox` at startup; keep unrelated data out of those named paths. |
| [`src/test-compaction.js`](./src/test-compaction.js) | Standalone local checkpoint/WAL-compaction and cold-start timing experiment. Uses `data/test_compaction.wal`; repeated runs may encounter prior fixture state/order IDs, and the timing assertion is a local measurement rather than an SLA guarantee. Its final exit status is currently 0 regardless of whether the timing target is met. |

## 10. Generated and persistent data

The `data/` directory is Git-ignored and can contain local etcd database files, node WALs, snapshots and test fixtures. The active node derives its paths from `NODE_ID`:

```text
data/<NODE_ID>.wal
data/<NODE_ID>_data/engine.snapshot
```

These files are not disposable caches: they may represent applied financial state. Keep each node ID associated with its own durable local state, back up/secure persistence, and do not clear or rename it to evade sequence checks. `data/etcd-local/` is an etcd data directory when running a local etcd process; do not delete it while etcd is using it.

Files observed in the current workspace include:

- `data/engine.snapshot` and `data/engine_node_1337.wal`: default/older engine artifacts; not the `local-dev` state paths.
- `data/local-dev.wal` and `data/local-dev_data/`: state created with the local-dev node ID.
- `data/local-test.wal`, `data/local-test_data/`, and corresponding dated local-test artifacts: state from earlier etcd/prefix experiments. The old local-test WAL was found at sequence 9 while its etcd prefix was behind; it should not be reused with an empty/different etcd prefix.
- `data/peer_alpha.wal`, `data/peer_beta.wal`, `data/peer_gamma.wal`: legacy distributed-divergence test artifacts from earlier runs.
- `data/leader_sandbox/` and `data/follower_sandbox/`: safe-sync test fixtures; test script removes and rebuilds them.
- `data/master_test_sandbox/` and `data/test_compaction.wal`: master/compaction test artifacts.
- `production_engine.wal`: a root-level WAL-like artifact, not used by current production `index.js`; preserve it until its provenance is known.

This inventory describes the workspace when this guide was generated. It is not a guarantee that every listed artifact exists in a fresh checkout. Runtime-generated state can contain order/account information; protect it accordingly.

## 11. Important limitations and what not to claim

- The production submission path is ordered by etcd transactions; do not advertise `RaftPeerNode` as production consensus.
- The main etcd test uses a mock HTTP API. Run integration, restart, network-partition and quorum-loss tests against a real etcd cluster before production.
- Log entries, request-id keys and permanent order-id keys currently have no retention/compaction policy in etcd.
- Snapshot/WAL migration from the previous ordering protocol is not automatic. A local sequence ahead of etcd causes startup failure by design.
- BigInt makes integer arithmetic exact within validated bounds, not unbounded; floating-point prices are not accepted by the client schema.
- The order pool has preallocated slots, but BigInt work, fills, maps, sorted-price array operations and other state transitions allocate. There is no zero-GC guarantee.
- Sorted-array insertion/removal can cost O(number of distinct price levels).
- Binary RPC payloads are size-capped, but transport security/TLS and comprehensive socket/backpressure limits remain deployment concerns.
- The existing tests establish useful local properties only. They do not establish production readiness, multi-region consensus guarantees, a recovery SLA, or target-market throughput.

## 12. Suggested reading order

1. [README.md](./README.md) and this guide.
2. [`src/index.js`](./src/index.js) to see composition and request flow.
3. [`src/engine/CommandValidation.js`](./src/engine/CommandValidation.js) and [`src/network/Security.js`](./src/network/Security.js) for the trust boundary.
4. [`src/consensus/EtcdOrderLog.js`](./src/consensus/EtcdOrderLog.js) and [`src/consensus/Sequencer.js`](./src/consensus/Sequencer.js) for ordering and application.
5. [`src/engine/MatchingEngine.js`](./src/engine/MatchingEngine.js), [`src/engine/OrderBook.js`](./src/engine/OrderBook.js), [`src/engine/DoublyLinkedList.js`](./src/engine/DoublyLinkedList.js) and [`src/memory/OrderPool.js`](./src/memory/OrderPool.js) for matching/storage.
6. [`src/consensus/WriteAheadLog.js`](./src/consensus/WriteAheadLog.js) and [`src/consensus/SnapshotManager.js`](./src/consensus/SnapshotManager.js) for local recovery.
7. Read the tests last, distinguishing the mocked etcd test from legacy local Raft/synchronization simulations and live RPC smoke tests.
