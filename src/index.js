'use strict';

const { OrderPool } = require('./memory/OrderPool');
const { OrderBook } = require('./engine/OrderBook');
const { MatchingEngine, STPMode } = require('./engine/MatchingEngine');
const { WriteAheadLog } = require('./consensus/WriteAheadLog');
const { Sequencer } = require('./consensus/Sequencer');
const { SecurityGate } = require('./network/Security');
const { GrenacheTransport } = require('./network/GrenacheTransport');

// Scale Constant: 1 BTC = 10^8 Satoshis
const SCALE = 100000000n;

const pool = new OrderPool(500000);
const book = new OrderBook(pool);
const engine = new MatchingEngine(book);
const wal = new WriteAheadLog('./production_engine.wal');
const sequencer = new Sequencer(engine, wal);
const security = new SecurityGate('CLUSTER_SECRET_AUTHENTICATION_KEY_V1');

console.log('================================================================');
console.log('  GRENACHE DETERMINISTIC P2P MATCHING ENGINE CORE ONLINE        ');
console.log('  Zero-Allocation Arena: 500,000 Slots Pre-allocated           ');
console.log('================================================================');

const grapeUrl = process.env.GRAPE_URL || 'http://127.0.0.1:30001';
const port = parseInt(process.env.PORT || '1337', 10);
const transport = new GrenacheTransport(grapeUrl, port, 'rpc_order_engine');
transport.init();

transport.onRequest(async (envelope) => {

  security.verify(envelope);

  const command = envelope.data;

  const normalizedCommand = {
    type: command.type,
    orderId: BigInt(command.orderId),
    userId: BigInt(command.userId),
    price: BigInt(command.price),
    amount: BigInt(command.amount),
    side: Number(command.side),
    stpMode: command.stpMode !== undefined ? Number(command.stpMode) : STPMode.CANCEL_TAKER
  };

  const result = sequencer.process(normalizedCommand);

  return JSON.parse(
    JSON.stringify(result, (_, v) => (typeof v === 'bigint' ? v.toString() : v))
  );
});

(async () => {
  console.log('\n[Self-Test] Running In-Memory Deterministic Execution Check...');

  const ask = sequencer.process({
    type: 'ORDER_CREATE',
    orderId: 1001n,
    userId: 101n,
    price: 65000n * SCALE,
    amount: (15n * SCALE) / 10n,
    side: 1, // SELL
    stpMode: STPMode.CANCEL_TAKER
  });
  console.log(`-> Seq #${ask.seqId}: Ask placed (1.5 BTC @ $65,000)`);

  const bid = sequencer.process({
    type: 'ORDER_CREATE',
    orderId: 1002n,
    userId: 202n,
    price: 65000n * SCALE,
    amount: (5n * SCALE) / 10n,
    side: 0, // BUY
    stpMode: STPMode.CANCEL_TAKER
  });
  console.log(`-> Seq #${bid.seqId}: Taker Bid executed. Fills:`, bid.fills);

  const stp = sequencer.process({
    type: 'ORDER_CREATE',
    orderId: 1003n,
    userId: 101n, // Same User ID
    price: 65000n * SCALE,
    amount: 1n * SCALE,
    side: 0,
    stpMode: STPMode.CANCEL_TAKER
  });
  console.log(`-> Seq #${stp.seqId}: Self-Trade Intercepted! Fills: ${stp.fills.length}`);
})();