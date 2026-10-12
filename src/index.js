'use strict';

const { OrderPool } = require('./memory/OrderPool');
const { OrderBook } = require('./engine/OrderBook');
const { MatchingEngine } = require('./engine/MatchingEngine');
const { WriteAheadLog } = require('./consensus/WriteAheadLog');
const { Sequencer } = require('./consensus/Sequencer');
const { EtcdOrderLog } = require('./consensus/EtcdOrderLog');
const { SecurityGate } = require('./network/Security');
const { GrenacheTransport } = require('./network/GrenacheTransport');
const { validateClientCommand } = require('./engine/CommandValidation');

const GRAPE_URL = process.env.GRAPE_URL || 'http://127.0.0.1:30001';
const PORT = parseInt(process.env.PORT, 10) || 1337;
const NODE_ID = process.env.NODE_ID || `engine_node_${PORT}`;
let clientApiKeys;
try {
  clientApiKeys = JSON.parse(process.env.CLIENT_API_KEYS || '');
} catch {
  throw new Error('CONFIG_ERROR: CLIENT_API_KEYS must be a JSON object of API key to secret mappings');
}
if (!clientApiKeys || typeof clientApiKeys !== 'object' || Array.isArray(clientApiKeys) ||
    Object.keys(clientApiKeys).length === 0 ||
    Object.entries(clientApiKeys).some(([apiKey, secret]) =>
      !apiKey || typeof secret !== 'string' || secret.length < 32) ||
    new Set(Object.values(clientApiKeys)).size !== Object.keys(clientApiKeys).length) {
  throw new Error('CONFIG_ERROR: Configure CLIENT_API_KEYS with API secrets of at least 32 characters');
}

const pool = new OrderPool(500000);
const book = new OrderBook(pool);
const engine = new MatchingEngine(book);
const wal = new WriteAheadLog(`./data/${NODE_ID}.wal`);
const sequencer = new Sequencer(engine, wal, 5000, `./data/${NODE_ID}_data`);
const security = new SecurityGate('client-api-key-resolver', 5000, clientApiKeys);

const orderLog = new EtcdOrderLog(sequencer);
const transport = new GrenacheTransport(GRAPE_URL, PORT);

transport.onRequest(async (payload) => {
  const verified = security.verify(payload);
  const data = verified.data;

  const validatedCommand = validateClientCommand(data);
  let command;
  if (validatedCommand.type === 'ORDER_CREATE') {
    command = {
      type: 'ORDER_CREATE',
      orderId: validatedCommand.orderId,
      userId: verified.userId,
      price: validatedCommand.price,
      amount: validatedCommand.amount,
      side: validatedCommand.side,
      stpMode: validatedCommand.stpMode
    };
  } else {
    command = {
      type: 'ORDER_CANCEL',
      orderId: validatedCommand.orderId,
      userId: verified.userId
    };
  }

  const result = await orderLog.submit(command, validatedCommand.requestId);

  return {
    seqId: result.seqId.toString(),
    ...(result.duplicate ? { duplicate: true } : {}),
    fills: (result.fills || []).map((f) => ({
      makerOrderId: f.makerOrderId.toString(),
      takerOrderId: f.takerOrderId.toString(),
      price: f.price.toString(),
      amount: f.amount.toString(),
      makerUserId: f.makerUserId.toString(),
      takerUserId: f.takerUserId.toString()
    }))
  };
});

orderLog.on('sync-error', (error) => {
  console.error(`[P2P Engine] etcd log synchronization failed (${NODE_ID}):`, error);
});

orderLog.start().then(() => {
  transport.start(['rpc_order_engine']);
  console.log(`[P2P Engine] Service initialized (${NODE_ID}) on port ${PORT}; etcd sequence ${sequencer.currentSequence}`);
}).catch((error) => {
  console.error(`[P2P Engine] Startup failed (${NODE_ID}):`, error);
  transport.stop();
  process.exitCode = 1;
});

function shutdown() {
  orderLog.stop();
  transport.stop();
}

process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);