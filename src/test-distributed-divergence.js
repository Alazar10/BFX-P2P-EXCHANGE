'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { OrderPool } = require('./memory/OrderPool');
const { OrderBook } = require('./engine/OrderBook');
const { MatchingEngine } = require('./engine/MatchingEngine');
const { WriteAheadLog } = require('./consensus/WriteAheadLog');
const { Sequencer } = require('./consensus/Sequencer');
const { RaftPeerNode } = require('./consensus/RaftPeerNode');

const SCALE = 100000000n;

class LocalMeshTransport {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this.router = null;
  }
  setRouter(router) { this.router = router; }
  async sendPeer(peerId, msg) { return this.router.route(this.nodeId, peerId, msg); }
}

class LocalMeshRouter {
  constructor() { this.nodes = new Map(); }
  register(nodeId, raftNode) { this.nodes.set(nodeId, raftNode); }
  async route(fromNodeId, peerId, msg) {
    if (fromNodeId === peerId) throw new Error('RAFT_TRANSPORT_ERROR: Cannot send to self');
    const peer = this.nodes.get(peerId);
    if (!peer) throw new Error(`RAFT_TRANSPORT_ERROR: Unknown peer ${peerId}`);
    return peer.handleRaftMessage(msg);
  }
}

function createPeer(nodeId, clusterPeers, router, dataDir) {
  const pool = new OrderPool(50000);
  const book = new OrderBook(pool);
  const engine = new MatchingEngine(book);
  const wal = new WriteAheadLog(path.join(dataDir, `${nodeId}.wal`));
  const sequencer = new Sequencer(engine, wal, 10000, path.join(dataDir, `${nodeId}_data`));
  const transport = new LocalMeshTransport(nodeId);
  transport.setRouter(router);

  const raftNode = new RaftPeerNode(nodeId, clusterPeers, sequencer, transport);
  router.register(nodeId, raftNode);

  return { nodeId, pool, book, sequencer, raftNode };
}

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

(async () => {
  console.log('=== TESTING LEGACY IN-PROCESS RAFT SIMULATION ===\n');

  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'p2p-divergence-test-'));
  const peers = [];
  try {
    const router = new LocalMeshRouter();
    const clusterPeerIds = ['peer_alpha', 'peer_beta', 'peer_gamma'];

    const peer1 = createPeer('peer_alpha', clusterPeerIds, router, dataDir);
    const peer2 = createPeer('peer_beta', clusterPeerIds, router, dataDir);
    const peer3 = createPeer('peer_gamma', clusterPeerIds, router, dataDir);

    peers.push(peer1, peer2, peer3);

    peer1.raftNode._becomeLeader();

    console.log('[Test] Sending 1,000 competing orders through the legacy Raft simulation...');

    for (let i = 1; i <= 1000; i++) {
      const isBuy = i % 2 === 0;
      const cmd = {
        type: 'ORDER_CREATE',
        orderId: BigInt(i),
        userId: BigInt((i % 5) + 1),
        price: (65000n + BigInt(i % 10)) * SCALE,
        amount: (BigInt(i % 3) + 1n) * 10000000n,
        side: isBuy ? 0 : 1,
        stpMode: 0
      };

      await peer1.raftNode.submitTransaction(cmd);
    }

    console.log('-> 1,000 Transactions Sequenced & Applied across all 3 nodes.\n');

    console.log('--- AUDITING IN-MEMORY ORDER BOOK CHECKSUMS ---');

    const hash1 = calculateOrderBookHash(peer1.book);
    const hash2 = calculateOrderBookHash(peer2.book);
    const hash3 = calculateOrderBookHash(peer3.book);

    console.log(`Peer Alpha State Hash: ${hash1} (Seq #${peer1.sequencer.currentSequence})`);
    console.log(`Peer Beta  State Hash: ${hash2} (Seq #${peer2.sequencer.currentSequence})`);
    console.log(`Peer Gamma State Hash: ${hash3} (Seq #${peer3.sequencer.currentSequence})`);

    const converged = hash1 === hash2 && hash2 === hash3;
    console.log(`\nState Convergence: ${converged ? '100% IDENTICAL (PASS)' : 'DIVERGENCE DETECTED (FAIL)'}`);
    if (!converged) process.exitCode = 1;
  } catch (error) {
    console.error('Distributed divergence test failed:', error);
    process.exitCode = 1;
  } finally {
    for (const peer of peers) {
      clearTimeout(peer.raftNode.timer);
      clearInterval(peer.raftNode.heartbeatTimer);
      peer.sequencer.wal.close();
    }

    fs.rmSync(dataDir, { recursive: true, force: true });
  }
})();