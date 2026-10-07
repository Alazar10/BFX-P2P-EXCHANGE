'use strict';

const { OrderPool } = require('./memory/OrderPool');
const { OrderBook } = require('./engine/OrderBook');
const { MatchingEngine } = require('./engine/MatchingEngine');
const { WriteAheadLog } = require('./consensus/WriteAheadLog');
const { Sequencer } = require('./consensus/Sequencer');

const SCALE = 100000000n;
const WAL_PATH = './data/test_compaction.wal';

function buildEngineInstance() {
  const pool = new OrderPool(100000);
  const book = new OrderBook(pool);
  const engine = new MatchingEngine(book);
  const wal = new WriteAheadLog(WAL_PATH);
  // Snapshot every 2,500 operations
  const sequencer = new Sequencer(engine, wal, 2500);
  return { pool, book, engine, wal, sequencer };
}

(async () => {
  console.log('=== WAL COMPACTION & 50MS RECOVERY TEST ===\n');

  console.log('[Phase 1] Processing 10,000 orders to trigger periodic checkpoints...');
  const instance1 = buildEngineInstance();

  for (let i = 1; i <= 10000; i++) {
    const isBuy = i % 3 === 0;
    instance1.sequencer.process({
      type: 'ORDER_CREATE',
      orderId: BigInt(i),
      userId: BigInt(i % 10),
      price: (65000n + BigInt(i % 50)) * SCALE,
      amount: 10000000n,
      side: isBuy ? 0 : 1,
      stpMode: 0
    });
  }

  instance1.sequencer.checkpoint();
  instance1.wal.close();
  console.log('[Phase 1] Engine 1 stopped.\n');

  console.log('[Phase 2] Cold Booting Engine 2 from snapshot + compacted WAL...');
  const bootStart = process.hrtime.bigint();

  const instance2 = buildEngineInstance();

  const bootEnd = process.hrtime.bigint();
  const totalBootMs = Number(bootEnd - bootStart) / 1_000_000;

  console.log(`[Phase 2] Total Startup Latency: ${totalBootMs.toFixed(3)} ms`);
  console.log(`Target Met (<50ms): ${totalBootMs < 50 ? 'PASS' : 'FAIL'}`);

  instance2.wal.close();
  process.exit(0);
})();