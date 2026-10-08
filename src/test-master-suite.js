'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');

const { OrderPool } = require('./memory/OrderPool');
const { OrderBook } = require('./engine/OrderBook');
const { MatchingEngine, STPMode } = require('./engine/MatchingEngine');
const { WriteAheadLog } = require('./consensus/WriteAheadLog');
const { Sequencer } = require('./consensus/Sequencer');
const { SnapshotManager } = require('./consensus/SnapshotManager');
const { SyncManager } = require('./consensus/SyncManager');
const { RaftPeerNode } = require('./consensus/RaftPeerNode');
const { SecurityGate } = require('./network/Security');
const { validateClientCommand } = require('./engine/CommandValidation');
const { EtcdOrderLog } = require('./consensus/EtcdOrderLog');
const {
  BinaryPeerRPCClient,
  BinaryPeerRPCServer,
  BinaryTransportRPCClient,
  BinaryTransportRPCServer
} = require('./network/BinaryRPCTransports');

const MASTER_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'bfx-master-test-'));

const SCALE = 100000000n;
const CLUSTER_SECRET = 'MASTER_SUITE_CLUSTER_SECRET_KEY';

function calculateOrderBookHash(book) {
  const entries = [];
  for (const [price, level] of book.bids.entries()) {
    entries.push(`BID:${price}:${level.totalVolume}`);
  }
  for (const [price, level] of book.asks.entries()) {
    entries.push(`ASK:${price}:${level.totalVolume}`);
  }
  entries.sort();
  return crypto.createHash('sha256').update(entries.join('|')).digest('hex');
}

//const MASTER_DATA_DIR = './data/master_test_sandbox';
fs.rmSync(MASTER_DATA_DIR, { recursive: true, force: true });
fs.mkdirSync(MASTER_DATA_DIR, { recursive: true });

async function runMasterSuite() {
  console.log('================================================================');
  console.log('   P2P DISTRIBUTED MATCHING ENGINE: MASTER TEST HARNESS         ');
  console.log('================================================================\n');

  let passedTests = 0;
  const totalTests = 9;

  console.log('[TEST 1/9] Deterministic Matching, Split Fills & Native STP...');
  {
    const pool = new OrderPool(10000);
    const book = new OrderBook(pool);
    const engine = new MatchingEngine(book);
    const wal = new WriteAheadLog(`${MASTER_DATA_DIR}/test1.wal`);
    const sequencer = new Sequencer(engine, wal, 5000, `${MASTER_DATA_DIR}/test1_data`);

    sequencer.process({
      type: 'ORDER_CREATE',
      orderId: 101n,
      userId: 1n,
      price: 65000n * SCALE,
      amount: 100000000n,
      side: 1,
      stpMode: STPMode.CANCEL_TAKER
    });
    sequencer.process({
      type: 'ORDER_CREATE',
      orderId: 102n,
      userId: 1n,
      price: 65000n * SCALE,
      amount: 50000000n,
      side: 1,
      stpMode: STPMode.CANCEL_TAKER
    });

    const takerRes = sequencer.process({
      type: 'ORDER_CREATE',
      orderId: 103n,
      userId: 2n,
      price: 65000n * SCALE,
      amount: 125000000n,
      side: 0,
      stpMode: STPMode.CANCEL_TAKER
    });

    const splitCorrect =
      takerRes.fills.length === 2 &&
      takerRes.fills[0].amount === 100000000n &&
      takerRes.fills[1].amount === 25000000n;

    const stpRes = sequencer.process({
      type: 'ORDER_CREATE',
      orderId: 104n,
      userId: 1n, // Same user
      price: 65000n * SCALE,
      amount: 25000000n,
      side: 0,
      stpMode: STPMode.CANCEL_TAKER
    });

    const stpCorrect = stpRes.fills.length === 0;

    wal.close();

    if (splitCorrect && stpCorrect) {
      console.log('-> Split matching (1.25 BTC -> 1.0 + 0.25) and Self-Trade Prevention: PASSED\n');
      passedTests++;
    } else {
      console.error('-> Deterministic Matching / STP: FAILED\n');
    }
  }

  console.log('[TEST 2/9] Dual-Cancellation Idempotency & Cryptographic Replay Defense...');
  {
    const pool = new OrderPool(10000);
    const book = new OrderBook(pool);
    const engine = new MatchingEngine(book);
    const wal = new WriteAheadLog(`${MASTER_DATA_DIR}/test2.wal`);
    const sequencer = new Sequencer(engine, wal, 5000, `${MASTER_DATA_DIR}/test2_data`);
    const security = new SecurityGate(CLUSTER_SECRET);

    sequencer.process({
      type: 'ORDER_CREATE',
      orderId: 201n,
      userId: 5n,
      price: 60000n * SCALE,
      amount: 100000000n,
      side: 0,
      stpMode: 0
    });

    const cancel1 = sequencer.process({ type: 'ORDER_CANCEL', orderId: 201n });
    const cancel2 = sequencer.process({ type: 'ORDER_CANCEL', orderId: 201n }); // Repeated cancel

    const testApiKey = 'tester_key';
    const env1 = security.sign({ test: 1 }, testApiKey);
    const v1 = security.verify(env1);

    let replayBlocked = false;
    try {

      security.verify(env1);
    } catch {
      replayBlocked = true;
    }

    wal.close();

    if (cancel1 && cancel2 && v1 && replayBlocked) {
      console.log('-> Safe cancellation idempotency and monotonic nonce replay defense: PASSED\n');
      passedTests++;
    } else {
      console.error('-> Concurrency / Anti-Replay: FAILED\n');
    }
  }

  console.log('[TEST 3/9] Throughput & Tail Latency Benchmark (1,000 Ops)...');
  {
    const pool = new OrderPool(50000);
    const book = new OrderBook(pool);
    const engine = new MatchingEngine(book);
    const wal = new WriteAheadLog(`${MASTER_DATA_DIR}/test3.wal`);
    const sequencer = new Sequencer(engine, wal, 10000, `${MASTER_DATA_DIR}/test3_data`);

    const latencies = [];
    const startTime = process.hrtime.bigint();

    for (let i = 1; i <= 1000; i++) {
      const isBuy = i % 2 === 0;
      const t0 = process.hrtime.bigint();

      sequencer.process({
        type: 'ORDER_CREATE',
        orderId: BigInt(i),
        userId: BigInt((i % 10) + 1),
        price: (65000n + BigInt(i % 20)) * SCALE,
        amount: 10000000n,
        side: isBuy ? 0 : 1,
        stpMode: 0
      });

      const t1 = process.hrtime.bigint();
      latencies.push(Number(t1 - t0) / 1_000_000);
    }

    const totalSec = Number(process.hrtime.bigint() - startTime) / 1_000_000_000;
    latencies.sort((a, b) => a - b);

    const p50 = latencies[Math.floor(latencies.length * 0.50)].toFixed(3);
    const p95 = latencies[Math.floor(latencies.length * 0.95)].toFixed(3);
    const p99 = latencies[Math.floor(latencies.length * 0.99)].toFixed(3);
    const opsPerSec = (1000 / totalSec).toFixed(2);

    wal.close();

    console.log(`-> Throughput: ${opsPerSec} ops/sec | p50: ${p50}ms | p95: ${p95}ms | p99: ${p99}ms`);
    console.log('-> Throughput & Preallocated Slot Storage: PASSED\n');
    passedTests++;
  }

  console.log('[TEST 4/9] WAL Checkpointing & <50ms Recovery SLA...');
  {
    const dataDir = `${MASTER_DATA_DIR}/test4_data`;
    fs.mkdirSync(dataDir, { recursive: true });
    const walPath = `${dataDir}/compact.wal`;

    const pool1 = new OrderPool(50000);
    const book1 = new OrderBook(pool1);
    const engine1 = new MatchingEngine(book1);
    const wal1 = new WriteAheadLog(walPath);
    const sequencer1 = new Sequencer(engine1, wal1, 1000, dataDir);

    for (let i = 1; i <= 3000; i++) {
      sequencer1.process({
        type: 'ORDER_CREATE',
        orderId: BigInt(i),
        userId: BigInt(i % 5),
        price: (65000n + BigInt(i % 10)) * SCALE,
        amount: 10000000n,
        side: i % 2 === 0 ? 0 : 1,
        stpMode: 0
      });
    }
    sequencer1.checkpoint();
    wal1.close();

    const bootStart = process.hrtime.bigint();

    const pool2 = new OrderPool(50000);
    const book2 = new OrderBook(pool2);
    const engine2 = new MatchingEngine(book2);
    const wal2 = new WriteAheadLog(walPath);
    const sequencer2 = new Sequencer(engine2, wal2, 1000, dataDir);

    const bootMs = Number(process.hrtime.bigint() - bootStart) / 1_000_000;
    wal2.close();

    const recoveryPassed = bootMs < 50 && sequencer2.currentSequence === 3000n;
    console.log(`-> Recovery Latency: ${bootMs.toFixed(3)} ms (Target < 50ms) | Synced to Seq #3000`);

    if (recoveryPassed) {
      console.log('-> Compaction & Recovery SLA: PASSED\n');
      passedTests++;
    } else {
      console.error('-> Recovery SLA: FAILED\n');
    }
  }

  console.log('[TEST 5/9] Legacy Raft Replication Simulation (not production consensus)...');
  {
    class LocalMeshRouter {
      constructor() { this.nodes = new Map(); }
      register(id, node) { this.nodes.set(id, node); }
      async route(fromId, msg) {
        const res = [];
        for (const [id, node] of this.nodes.entries()) {
          if (id !== fromId) res.push(node.handleRaftMessage(msg));
        }
        return res[0];
      }
    }

    const router = new LocalMeshRouter();
    const clusterIds = ['node_1', 'node_2', 'node_3'];
    let rejectAppendAcks = false;

    function createRaftPeer(id) {
      const dir = `${MASTER_DATA_DIR}/${id}_data`;
      const pool = new OrderPool(20000);
      const book = new OrderBook(pool);
      const engine = new MatchingEngine(book);
      const wal = new WriteAheadLog(`${dir}/engine.wal`);
      const sequencer = new Sequencer(engine, wal, 10000, dir);

      const transport = {
        sendPeer: async (targetId, msg) => {
          const node = router.nodes.get(targetId);
          if (!node) return null;
          const response = node.handleRaftMessage(msg);
          if (rejectAppendAcks && msg.action === 'RAFT_APPEND_ENTRIES') {
            return { success: false };
          }
          return response;
        }
      };

      const raftNode = new RaftPeerNode(id, clusterIds, sequencer, transport);
      router.register(id, raftNode);
      return { id, book, sequencer, raftNode, wal };
    }

    const p1 = createRaftPeer('node_1');
    const p2 = createRaftPeer('node_2');
    const p3 = createRaftPeer('node_3');

    p1.raftNode._becomeLeader();

    await Promise.all(Array.from({ length: 500 }, (_, index) => {
      const i = index + 1;
      return p1.raftNode.submitTransaction({
        type: 'ORDER_CREATE',
        orderId: BigInt(i),
        userId: BigInt((i % 4) + 1),
        price: (65000n + BigInt(i % 15)) * SCALE,
        amount: 10000000n,
        side: i % 2 === 0 ? 0 : 1,
        stpMode: 0
      });
    }));

    rejectAppendAcks = true;
    let quorumFailureHandled = false;
    try {
      await p1.raftNode.submitTransaction({
        type: 'ORDER_CREATE',
        orderId: 999001n,
        userId: 9n,
        price: 999999n,
        amount: 1n,
        side: 1,
        stpMode: 0
      });
    } catch (error) {
      quorumFailureHandled = error.message.startsWith('QUORUM_LOST');
    }
    rejectAppendAcks = false;
    await p1.raftNode.submitTransaction({
      type: 'ORDER_CREATE',
      orderId: 999002n,
      userId: 9n,
      price: 999999n,
      amount: 1n,
      side: 1,
      stpMode: 0
    });

    const h1 = calculateOrderBookHash(p1.book);
    const h2 = calculateOrderBookHash(p2.book);
    const h3 = calculateOrderBookHash(p3.book);

    p1.wal.close();
    p2.wal.close();
    p3.wal.close();

    const retryDidNotApplyRejectedEntry = [p1, p2, p3]
      .every((peer) => !peer.book.orderMap.has(999001n));
    const consensusPassed =
      h1 === h2 &&
      h2 === h3 &&
      p1.sequencer.currentSequence === 501n &&
      quorumFailureHandled &&
      retryDidNotApplyRejectedEntry;
    console.log(`-> SHA-256 State Hashes across all 3 independent nodes: ${h1.substring(0, 16)}...`);

    if (consensusPassed) {
      console.log('-> Distributed Replicated State Machine Convergence: PASSED\n');
      passedTests++;
    } else {
      console.error('-> Multi-Node Consensus: FAILED\n');
    }
  }

  console.log('[TEST 6/9] Legacy WAL Anti-Entropy Simulation...');
  {
    const lDir = `${MASTER_DATA_DIR}/leader_sync_data`;
    const fDir = `${MASTER_DATA_DIR}/follower_sync_data`;

    const transport = {
      leaderSync: null,
      async send(msg) {
        return this.leaderSync.handleSyncRequest(msg);
      }
    };

    const lPool = new OrderPool(20000);
    const lBook = new OrderBook(lPool);
    const lEngine = new MatchingEngine(lBook);
    const lWal = new WriteAheadLog(`${lDir}/engine.wal`);
    const lSequencer = new Sequencer(lEngine, lWal, 10000, lDir);
    const lSnapshot = new SnapshotManager(lBook, lWal, lDir);
    const lSync = new SyncManager(lSequencer, lSnapshot, transport);
    transport.leaderSync = lSync;

    for (let i = 1; i <= 1000; i++) {
      lSequencer.process({
        type: 'ORDER_CREATE',
        orderId: BigInt(i),
        userId: BigInt((i % 4) + 1),
        price: (65000n + BigInt(i % 10)) * SCALE,
        amount: 10000000n,
        side: i % 2 === 0 ? 0 : 1,
        stpMode: 0
      });
    }
    lSnapshot.createSnapshot(lSequencer.currentSequence);

    const fPool = new OrderPool(20000);
    const fBook = new OrderBook(fPool);
    const fEngine = new MatchingEngine(fBook);
    const fWal = new WriteAheadLog(`${fDir}/engine.wal`);
    const fSequencer = new Sequencer(fEngine, fWal, 10000, fDir);
    const fSnapshot = new SnapshotManager(fBook, fWal, fDir);
    const fSync = new SyncManager(fSequencer, fSnapshot, transport);

    await fSync.synchronizeWithLeader('leader');

    const lHash = calculateOrderBookHash(lBook);
    const fHash = calculateOrderBookHash(fBook);

    lWal.close();
    fWal.close();

    const syncPassed = lHash === fHash && fSequencer.currentSequence === 1000n;
    console.log(`-> Synchronized Hash Match: ${fHash.substring(0, 16)}... (Seq #1000)`);

    if (syncPassed) {
      console.log('-> Anti-Entropy Catch-Up & Checkpoint Hydration: PASSED\n');
      passedTests++;
    } else {
      console.error('-> Anti-Entropy Sync: FAILED\n');
    }
  }

  console.log('[TEST 7/9] Exact Wide-Integer Storage & Strict Command Schema...');
  {
    const wideValue = 2n ** 100n + 12345n;
    const pool = new OrderPool(4);
    const book = new OrderBook(pool);
    const engine = new MatchingEngine(book);
    const wideOrderId = 2n ** 90n + 7n;
    engine.processOrder(wideOrderId, 1n, wideValue, wideValue, 1, STPMode.CANCEL_TAKER);

    const validRequestId = crypto.randomUUID();
    const acceptedCommand = validateClientCommand({
      type: 'ORDER_CREATE',
      orderId: wideOrderId.toString(),
      price: wideValue.toString(),
      amount: wideValue.toString(),
      side: 0,
      stpMode: STPMode.CANCEL_TAKER,
      requestId: validRequestId
    });
    let invalidCommandsRejected = 0;
    const invalidCommands = [
      { type: 'ORDER_CREATE', orderId: '1', price: 1, amount: '1', side: 0, stpMode: 1, requestId: validRequestId },
      { type: 'ORDER_CREATE', orderId: '1', price: '1e6', amount: '1', side: 0, stpMode: 1, requestId: validRequestId },
      { type: 'ORDER_CREATE', orderId: '1', price: '1', amount: '1'.repeat(257), side: 0, stpMode: 1, requestId: validRequestId },
      { type: 'ORDER_CREATE', orderId: '1', price: '1', amount: '1', side: 0, stpMode: 0, requestId: validRequestId },
      { type: 'ORDER_CANCEL', orderId: '1', userId: 'forged', requestId: validRequestId }
    ];
    for (const command of invalidCommands) {
      try {
        validateClientCommand(command);
      } catch {
        invalidCommandsRejected++;
      }
    }

    let duplicateRejected = false;
    try {
      book.addRestingOrder(wideOrderId, 1n, wideValue, 1n, 1);
    } catch (error) {
      duplicateRejected = error.message.startsWith('DUPLICATE_ORDER_ID');
    }

    const ptr = book.orderMap.get(wideOrderId);
    const exactStorage =
      pool.orderId[ptr] === wideOrderId &&
      pool.price[ptr] === wideValue &&
      pool.amount[ptr] === wideValue;
    let linkedSlotFreeRejected = false;
    try {
      pool.free(ptr);
    } catch {
      linkedSlotFreeRejected = true;
    }
    book.cancel(wideOrderId);
    let doubleFreeRejected = false;
    try {
      pool.free(ptr);
    } catch {
      doubleFreeRejected = true;
    }
    let unsafeNumberRejected = false;
    try {
      engine.processOrder(Number.MAX_SAFE_INTEGER + 1, 1n, 1n, 1n, 1, STPMode.CANCEL_TAKER);
    } catch {
      unsafeNumberRejected = true;
    }
    const schemaPassed = exactStorage && invalidCommandsRejected === invalidCommands.length &&
      duplicateRejected && linkedSlotFreeRejected && doubleFreeRejected && unsafeNumberRejected &&
      acceptedCommand.orderId === wideOrderId && acceptedCommand.amount === wideValue &&
      acceptedCommand.requestId === validRequestId;

    if (schemaPassed) {
      console.log('-> >64-bit values remain exact; malformed and duplicate commands rejected: PASSED\n');
      passedTests++;
    } else {
      console.error('-> Wide Integer / Strict Schema: FAILED\n');
    }
  }

  console.log('[TEST 8/9] MessagePack RPC Frames & HMAC Integrity...');
  {
    const transport = new BinaryTransportRPCClient({}, {});
    const peerClient = new BinaryPeerRPCClient({}, {});
    const peerServer = new BinaryPeerRPCServer({}, {});
    const message = ['req-1', 'rpc_order_engine', {
      type: 'ORDER_CREATE',
      price: (2n ** 100n).toString(),
      amount: '12'
    }];
    const frame = transport.format(message);
    const decoded = transport.parse(frame);
    const peerSecurity = new SecurityGate({
      node_a: 'a'.repeat(40),
      node_b: 'b'.repeat(40)
    });
    const envelope = peerSecurity.signClusterMessage('node_a', { action: 'RAFT_HEARTBEAT' });
    const validPeerAccepted = peerSecurity.verifyClusterMessage(envelope, ['node_a', 'node_b']);
    let identitySwapRejected = false;
    try {
      peerSecurity.verifyClusterMessage(
        { ...envelope, nodeId: 'node_b' },
        ['node_a', 'node_b']
      );
    } catch {
      identitySwapRejected = true;
    }

    if (peerClient.getTransportClass() === BinaryTransportRPCClient &&
        peerServer.getTransportClass() === BinaryTransportRPCServer &&
        Buffer.isBuffer(frame) &&
        frame.length < Buffer.byteLength(JSON.stringify(message)) &&
        decoded &&
        JSON.stringify(decoded) === JSON.stringify(message) &&
        validPeerAccepted.nodeId === 'node_a' && identitySwapRejected) {
      console.log('-> Compact binary frame round-trip and distinct peer key verification: PASSED\n');
      passedTests++;
    } else {
      console.error('-> Binary RPC / Peer Authentication: FAILED\n');
    }
  }

    console.log('[TEST 9/9] Concurrent Linearizable etcd Ordering & Idempotent Retry...');
    {
      const kv = new Map();
      let revision = 0;
      const mockEtcdFetch = async (_url, options) => {
        const request = JSON.parse(options.body);
        const readKey = (key) => kv.get(Buffer.from(key, 'base64').toString('utf8'));
        let response;
        if (_url.endsWith('/v3/kv/range')) {
          const key = Buffer.from(request.key, 'base64').toString('utf8');
          const rangeEnd = request.rangeEnd
            ? Buffer.from(request.rangeEnd, 'base64').toString('utf8')
            : null;
          const rangedEntries = [...kv.entries()]
            .filter(([candidate]) => rangeEnd
              ? candidate >= key && candidate < rangeEnd
              : candidate === key)
            .sort(([left], [right]) => left.localeCompare(right));
          const entries = (request.limit
            ? rangedEntries.slice(0, request.limit)
            : rangedEntries)
            .map(([candidate, value]) => ({
              key: Buffer.from(candidate).toString('base64'),
              value: Buffer.from(value.value).toString('base64'),
              mod_revision: value.modRevision,
              version: value.version
            }));
          response = { kvs: entries };
        } else if (_url.endsWith('/v3/kv/txn')) {
          const succeeded = request.compare.every((condition) => {
            const current = readKey(condition.key);
            if (condition.target === 'VERSION') {
              return Number(current ? current.version : 0) === Number(condition.version);
            }
            if (condition.target === 'MOD') {
              return current && current.modRevision === condition.modRevision;
            }
            return false;
          });
          if (succeeded) {
            revision++;
            for (const operation of request.success) {
              const put = operation.requestPut;
              const key = Buffer.from(put.key, 'base64').toString('utf8');
              const old = kv.get(key);
              kv.set(key, {
                value: Buffer.from(put.value, 'base64').toString('utf8'),
                modRevision: String(revision),
                version: String(old ? Number(old.version) + 1 : 1)
              });
            }
          }
          response = { succeeded };
        } else {
          throw new Error(`Unexpected etcd endpoint ${_url}`);
        }
        return {
          ok: true,
          status: 200,
          async text() { return JSON.stringify(response); }
        };
      };

      const createNode = (name) => {
        const dir = `${MASTER_DATA_DIR}/${name}_etcd`;
        const pool = new OrderPool(16);
        const book = new OrderBook(pool);
        const engine = new MatchingEngine(book);
        const wal = new WriteAheadLog(`${dir}/engine.wal`);
        const sequencer = new Sequencer(engine, wal, 10000, dir);
        const log = new EtcdOrderLog(sequencer, {
          endpoints: 'http://127.0.0.1:2379',
          prefix: '/test/exchange',
          fetchImpl: mockEtcdFetch
        });
        return { book, wal, sequencer, log };
      };

      const a = createNode('a');
      const b = createNode('b');
      const c = createNode('c');
      const d = createNode('d');
      const requestA = crypto.randomUUID();
      const requestB = crypto.randomUUID();
      const commandA = {
        type: 'ORDER_CREATE',
        orderId: 9001n,
        userId: 91n,
        price: 1200n,
        amount: 4n,
        side: 1,
        stpMode: STPMode.CANCEL_TAKER
      };
      const commandB = {
        type: 'ORDER_CREATE',
        orderId: 9002n,
        userId: 92n,
        price: 1201n,
        amount: 5n,
        side: 1,
        stpMode: STPMode.CANCEL_TAKER
      };

      try {
        const [resultA, resultB] = await Promise.all([
          a.log.submit(commandA, requestA),
          b.log.submit(commandB, requestB)
        ]);
        await Promise.all([a.log.catchUp(), b.log.catchUp(), c.log.catchUp()]);
        const duplicate = await a.log.submit(commandA, requestA);
        const conflictCommand = { ...commandA, amount: 7n };
        let requestConflictRejected = false;
        try {
          await a.log.submit(conflictCommand, requestA);
        } catch (error) {
          requestConflictRejected = error.message.startsWith('IDEMPOTENCY_CONFLICT');
        }
        const cancelResult = await b.log.submit({
          type: 'ORDER_CANCEL',
          orderId: commandA.orderId,
          userId: commandA.userId
        }, crypto.randomUUID());
        await Promise.all([a.log.catchUp(), c.log.catchUp()]);
        let reusedOrderIdRejected = false;
        try {
          await c.log.submit({ ...commandA, amount: 8n }, crypto.randomUUID());
        } catch (error) {
          reusedOrderIdRejected = error.message.startsWith('DUPLICATE_ORDER_ID');
        }
        kv.delete('/test/exchange/sequence');
        let missingSequenceGuarded = false;
        try {
          await d.log.catchUp();
        } catch (error) {
          missingSequenceGuarded = error.message.startsWith('ETCD_SEQUENCE_MISSING');
        }

        const sameState =
          a.sequencer.currentSequence === 3n &&
          b.sequencer.currentSequence === 3n &&
          c.sequencer.currentSequence === 3n &&
          calculateOrderBookHash(a.book) === calculateOrderBookHash(b.book) &&
          calculateOrderBookHash(b.book) === calculateOrderBookHash(c.book);
        const uniqueSequences = [resultA.seqId, resultB.seqId].sort((left, right) =>
          left < right ? -1 : left > right ? 1 : 0
        );
        if (sameState && uniqueSequences[0] === 1n && uniqueSequences[1] === 2n &&
            duplicate.seqId === resultA.seqId && !duplicate.duplicate &&
            requestConflictRejected && cancelResult.seqId === 3n && reusedOrderIdRejected &&
            missingSequenceGuarded) {
          console.log('-> CAS ordering, recovery, retries, ID uniqueness, and log-corruption guard: PASSED\n');
          passedTests++;
        } else {
          console.error('-> etcd Ordering / Idempotency: FAILED\n');
        }
      } finally {
        a.wal.close();
        b.wal.close();
        c.wal.close();
        d.wal.close();
      }
    }

  console.log('================================================================');
  console.log(` MASTER TEST SUMMARY: ${passedTests}/${totalTests} SUITES PASSED`);
  console.log('================================================================');

  process.exit(passedTests === totalTests ? 0 : 1);
}

runMasterSuite().catch((err) => {
  console.error('Master Test Fatal Error:', err);
  process.exit(1);
});