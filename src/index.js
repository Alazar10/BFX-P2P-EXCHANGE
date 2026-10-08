'use strict';

const { OrderPool } = require('./memory/OrderPool');
const { OrderBook } = require('./engine/OrderBook');
const { MatchingEngine } = require('./engine/MatchingEngine');
const { WriteAheadLog } = require('./consensus/WriteAheadLog');
const { Sequencer } = require('./consensus/Sequencer');
const { RaftPeerNode } = require('./consensus/RaftPeerNode');
const { SecurityGate } = require('./network/Security');
const { GrenacheTransport } = require('./network/GrenacheTransport');
const { STPMode } = require('./engine/MatchingEngine');

const GRAPE_URL = process.env.GRAPE_URL || 'http://127.0.0.1:30001';
const PORT = parseInt(process.env.PORT, 10) || 1337;
const NODE_ID = process.env.NODE_ID || `engine_node_${PORT}`;
const CLUSTER_PEERS = (process.env.CLUSTER_PEERS || NODE_ID).split(',').map((id) => id.trim()).filter(Boolean);
const CLUSTER_SECRET = process.env.CLUSTER_SECRET;
if (!CLUSTER_SECRET || CLUSTER_SECRET.length < 32) {
  throw new Error('CONFIG_ERROR: Set CLUSTER_SECRET to a random value of at least 32 characters');
}
let clientApiKeys;
try {
  clientApiKeys = JSON.parse(process.env.CLIENT_API_KEYS || '');
} catch {
  throw new Error('CONFIG_ERROR: CLIENT_API_KEYS must be a JSON object of API key to secret mappings');
}
if (!clientApiKeys || typeof clientApiKeys !== 'object' || Array.isArray(clientApiKeys) ||
    Object.keys(clientApiKeys).length === 0 ||
    Object.entries(clientApiKeys).some(([apiKey, secret]) =>
      !apiKey || typeof secret !== 'string' || secret.length < 32 || secret === CLUSTER_SECRET) ||
    new Set(Object.values(clientApiKeys)).size !== Object.keys(clientApiKeys).length) {
  throw new Error('CONFIG_ERROR: Configure CLIENT_API_KEYS with API secrets of at least 32 characters');
}
if (!CLUSTER_PEERS.includes(NODE_ID)) {
  throw new Error(`CLUSTER_CONFIG_ERROR: CLUSTER_PEERS must include NODE_ID (${NODE_ID})`);
}

const pool = new OrderPool(500000);
const book = new OrderBook(pool);
const engine = new MatchingEngine(book);
const wal = new WriteAheadLog(`./data/${NODE_ID}.wal`);
const sequencer = new Sequencer(engine, wal, 5000, `./data/${NODE_ID}_data`);
const security = new SecurityGate('client-api-key-resolver', 5000, clientApiKeys);
const clusterSecurity = new SecurityGate(CLUSTER_SECRET);

const transport = new GrenacheTransport(GRAPE_URL, PORT);
const raftNode = new RaftPeerNode(NODE_ID, CLUSTER_PEERS, sequencer, transport, clusterSecurity);

function toWireValue(value) {
  return JSON.parse(JSON.stringify(value, (_, nested) =>
    typeof nested === 'bigint' ? nested.toString() : nested
  ));
}

transport.onRequest(async (payload) => {
  if (payload && payload.clusterMessage === true) {
    const verified = clusterSecurity.verifyClusterMessage(payload, CLUSTER_PEERS);
    const message = { ...verified.message, peerId: verified.nodeId };
    if ((message.action === 'RAFT_REQUEST_VOTE' && message.candidateId !== verified.nodeId) ||
        (['RAFT_HEARTBEAT', 'RAFT_APPEND_ENTRIES', 'RAFT_COMMIT'].includes(message.action) &&
         message.leaderId !== verified.nodeId)) {
      throw new Error('CLUSTER_AUTH_REJECT: Message identity does not match signed peer');
    }
    return toWireValue(await raftNode.handleRaftMessage(message));
  }

  // Route 2: Client Traffic via Security Gate
  const verified = security.verify(payload);
  const data = verified.data;

  let command;
  if (data.type === 'ORDER_CREATE') {
    const stpMode = data.stpMode === undefined ? STPMode.CANCEL_TAKER : Number(data.stpMode);
    if (![STPMode.CANCEL_TAKER, STPMode.CANCEL_MAKER, STPMode.CANCEL_BOTH].includes(stpMode)) {
      throw new Error('INVALID_STP_MODE: Self-trade prevention must be enabled');
    }
    command = {
      type: 'ORDER_CREATE',
      orderId: BigInt(data.orderId),
      userId: verified.userId,
      price: BigInt(data.price),
      amount: BigInt(data.amount),
      side: Number(data.side),
      stpMode
    };
  } else if (data.type === 'ORDER_CANCEL') {
    command = {
      type: 'ORDER_CANCEL',
      orderId: BigInt(data.orderId),
      userId: verified.userId
    };
  } else {
    throw new Error(`INVALID_COMMAND_TYPE: ${data.type}`);
  }

  const result = await raftNode.submitTransaction(command);

  return {
    seqId: result.seqId.toString(),
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

// Announce both generic pool and peer-specific endpoint
transport.start(['rpc_order_engine', `rpc_node_${NODE_ID}`]);

if (CLUSTER_PEERS.length === 1 && CLUSTER_PEERS[0] === NODE_ID) {
  raftNode._becomeLeader();
}

console.log(`[P2P Engine] Service initialized cleanly (${NODE_ID}) on port ${PORT}`);