'use strict';

const fs = require('node:fs');
const crypto = require('node:crypto');
const { OrderPool } = require('./memory/OrderPool');
const { OrderBook } = require('./engine/OrderBook');
const { MatchingEngine } = require('./engine/MatchingEngine');
const { WriteAheadLog } = require('./consensus/WriteAheadLog');
const { Sequencer } = require('./consensus/Sequencer');
const { SyncManager } = require('./consensus/SyncManager');

const SCALE = 100000000n;

fs.rmSync('./data/leader_sandbox', { recursive: true, force: true });
fs.rmSync('./data/follower_sandbox', { recursive: true, force: true });

class MockSyncTransport {
  constructor() { this.leaderSyncManager = null; }
  setLeader(syncManager) { this.leaderSyncManager = syncManager; }
  async send(msg) {
    if (msg.action === 'SYNC_REQUEST') {
      return this.leaderSyncManager.handleSyncRequest(msg);
    }
  }
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
  console.log('=== TESTING SAFE LOG-DRIVEN STATE SYNCHRONIZATION ===\n');

  const transport = new MockSyncTransport();

  console.log('[Setup] Leader processing 5,000 transactions and creating snapshot...');
  const leaderPool = new OrderPool(100000);
  const leaderBook = new OrderBook(leaderPool);
  const leaderEngine = new MatchingEngine(leaderBook);
  const leaderWal = new WriteAheadLog('./data/leader_sandbox/engine.wal');
  const leaderSequencer = new Sequencer(leaderEngine, leaderWal, 2500, './data/leader_sandbox');
  const leaderSync = new SyncManager(leaderSequencer, leaderSequencer.snapshotManager, transport);

  transport.setLeader(leaderSync);

  for (let i = 1; i <= 5000; i++) {
    const isBuy = i % 2 === 0;
    leaderSequencer.process({
      type: 'ORDER_CREATE',
      orderId: BigInt(i),
      userId: BigInt((i % 5) + 1),
      price: (65000n + BigInt(i % 20)) * SCALE,
      amount: 10000000n,
      side: isBuy ? 0 : 1,
      stpMode: 0
    });
  }
  leaderSequencer.checkpoint();
  console.log(`-> Leader active at Seq #${leaderSequencer.currentSequence}\n`);

  console.log('[Test 1] Booting fresh, un-synced follower node (Seq #0)...');
  const followerPool = new OrderPool(100000);
  const followerBook = new OrderBook(followerPool);
  const followerEngine = new MatchingEngine(followerBook);
  const followerWal = new WriteAheadLog('./data/follower_sandbox/engine.wal');
  const followerSequencer = new Sequencer(followerEngine, followerWal, 2500, './data/follower_sandbox');
  const followerSync = new SyncManager(followerSequencer, followerSequencer.snapshotManager, transport);

  console.log(`-> Follower initialized at Seq #${followerSequencer.currentSequence}`);

  await followerSync.synchronizeWithLeader('leader_node');

  console.log('\n--- VERIFYING CRYPTOGRAPHIC STATE CONVERGENCE ---');
  const leaderHash = calculateOrderBookHash(leaderBook);
  const followerHash = calculateOrderBookHash(followerBook);

  console.log(`Leader State Checksum:   ${leaderHash} (Seq #${leaderSequencer.currentSequence})`);
  console.log(`Follower State Checksum: ${followerHash} (Seq #${followerSequencer.currentSequence})`);

  const passed = leaderHash === followerHash && followerSequencer.currentSequence === leaderSequencer.currentSequence;
  console.log(`\nAnti-Entropy Synchronization: ${passed ? '100% IDENTICAL (PASS)' : 'DIVERGENCE DETECTED (FAIL)'}`);

  leaderWal.close();
  followerWal.close();
  process.exit(passed ? 0 : 1);
})();