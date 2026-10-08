'use strict';

const { OrderPool } = require('./memory/OrderPool');
const { OrderBook } = require('./engine/OrderBook');
const { MatchingEngine } = require('./engine/MatchingEngine');
const { WriteAheadLog } = require('./consensus/WriteAheadLog');
const { Sequencer } = require('./consensus/Sequencer');
const { RaftPeerNode } = require('./consensus/RaftPeerNode');
const { SecurityGate } = require('./network/Security');
const { GrenacheTransport } = require('./network/GrenacheTransport');

const GRAPE_URL = process.env.GRAPE_URL || 'http://127.0.0.1:30001';
const PORT = parseInt(process.env.PORT, 10) || 1337;
const NODE_ID = process.env.NODE_ID || `engine_node_${PORT}`;
const CLUSTER_PEERS = (process.env.CLUSTER_PEERS || NODE_ID).split(',');
const CLUSTER_SECRET = process.env.CLUSTER_SECRET || 'CLUSTER_SECRET_AUTHENTICATION_KEY_V1';

const pool = new OrderPool(500000);
const book = new OrderBook(pool);
const engine = new MatchingEngine(book);
const wal = new WriteAheadLog(`./data/${NODE_ID}.wal`);
const sequencer = new Sequencer(engine, wal, 5000, `./data/${NODE_ID}_data`);
const security = new SecurityGate(CLUSTER_SECRET);

const transport = new GrenacheTransport(GRAPE_URL, PORT);
const raftNode = new RaftPeerNode(NODE_ID, CLUSTER_PEERS, sequencer, transport);

transport.onRequest(async (payload) => {
  // Route 1: Internal Cluster Consensus Traffic
  if (payload && payload.action && payload.action.startsWith('RAFT_')) {
    return raftNode.handleRaftMessage(payload);
  }
  if (payload && payload.action === 'FORWARD_ORDER_TO_LEADER') {
    return raftNode.handleRaftMessage(payload);
  }

  // Route 2: Client Traffic via Security Gate
  const verified = security.verify(payload);
  const data = verified.data;

  let command;
  if (data.type === 'ORDER_CREATE') {
    command = {
      type: 'ORDER_CREATE',
      orderId: BigInt(data.orderId),
      userId: verified.userId,
      price: BigInt(data.price),
      amount: BigInt(data.amount),
      side: Number(data.side),
      stpMode: Number(data.stpMode || 0)
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