'use strict';

const { SnapshotManager } = require('./SnapshotManager');
const { toUnsignedBigInt } = require('../engine/Integer');
const { STPMode } = require('../engine/MatchingEngine');

class Sequencer {
  constructor(engine, wal, snapshotInterval = 5000, dataDir = './data') {
    this.engine = engine;
    this.wal = wal;
    this.snapshotInterval = snapshotInterval;
    this.dataDir = dataDir;
    this.currentSequence = 0n;
    this.opsSinceLastSnapshot = 0;
    this.isPaused = false;

    this.snapshotManager = new SnapshotManager(engine.book, wal, this.dataDir);
    this._recoverState();
  }

  _recoverState() {
    const { lastAppliedSeqId, loadedOrders } = this.snapshotManager.loadSnapshot();
    this.currentSequence = lastAppliedSeqId;

    let replayed = 0;
    this.wal.replay((entry) => {
      if (entry.type !== 'ORDER_CREATE' && entry.type !== 'ORDER_CANCEL') return;
      const entrySeqId = BigInt(entry.seqId);
      if (entrySeqId > this.currentSequence) {
        if (entrySeqId !== this.currentSequence + 1n) {
          throw new Error(`WAL_SEQUENCE_GAP: Expected ${this.currentSequence + 1n}, received ${entrySeqId}`);
        }
        this.currentSequence = entrySeqId;
        this._applyStateTransition(entry);
        replayed++;
      }
    });
  }

  /**
   * Standalone / Leader execution: assigns monotonic sequence ID and applies state.
   */
  process(command) {
    if (this.isPaused) throw new Error('SEQUENCER_PAUSED_FOR_SYNC');
    return this.applyCommitted({
      ...command,
      seqId: (this.currentSequence + 1n).toString(),
      timestamp: Date.now()
    });
  }

  /**
   * Applies an entry that has reached consensus (or follower commit).
   * Guarantees exact monotonic execution without re-incrementing seqId.
   */
  applyCommitted(entry) {
    const entrySeqId = BigInt(entry.seqId);
    if (entrySeqId <= this.currentSequence) {
      return { seqId: entrySeqId, fills: [], ignored: true };
    }
    if (entrySeqId !== this.currentSequence + 1n) {
      throw new Error(`SEQUENCE_GAP: Expected ${this.currentSequence + 1n}, received ${entrySeqId}`);
    }

    this.validateCommand(entry);
    this.wal.append(entry);
    const fills = this._applyStateTransition(entry);
    this.currentSequence = entrySeqId;

    if (++this.opsSinceLastSnapshot >= this.snapshotInterval) {
      this.checkpoint();
      this.opsSinceLastSnapshot = 0;
    }

    return { seqId: entrySeqId, fills };
  }

  validateCommand(payload) {
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
      throw new Error('INVALID_COMMAND: Expected a command object');
    }
    if (payload.type === 'ORDER_CREATE') {
      const orderId = toUnsignedBigInt(payload.orderId, 'orderId', { positive: true });
      toUnsignedBigInt(payload.userId, 'userId');
      toUnsignedBigInt(payload.price, 'price', { positive: true });
      toUnsignedBigInt(payload.amount, 'amount', { positive: true });
      if (this.engine.book.orderMap.has(orderId)) {
        throw new Error('DUPLICATE_ORDER_ID: Order ID is already resting');
      }
      if (this.engine.pool.freeHead === -1) {
        throw new Error('ORDER_POOL_EXHAUSTED: No order slots are available');
      }
      if (!Number.isInteger(payload.side) || (payload.side !== 0 && payload.side !== 1)) {
        throw new Error('INVALID_ORDER: side must be 0 (BUY) or 1 (SELL)');
      }
      const stpMode = payload.stpMode === undefined ? STPMode.CANCEL_TAKER : payload.stpMode;
      if (!Number.isInteger(stpMode) ||
          ![STPMode.NONE, STPMode.CANCEL_TAKER, STPMode.CANCEL_MAKER, STPMode.CANCEL_BOTH].includes(stpMode)) {
        throw new Error('INVALID_ORDER: Invalid self-trade prevention mode');
      }
      return;
    }
    if (payload.type === 'ORDER_CANCEL') {
      const orderId = toUnsignedBigInt(payload.orderId, 'orderId', { positive: true });
      if (payload.userId !== undefined && payload.userId !== null) {
        const userId = toUnsignedBigInt(payload.userId, 'userId');
        const ptr = this.engine.book.orderMap.get(orderId);
        if (ptr !== undefined && this.engine.pool.userId[ptr] !== userId) {
          throw new Error('UNAUTHORIZED_CANCEL: Order does not belong to user');
        }
      }
      return;
    }
    throw new Error('INVALID_COMMAND_TYPE: Unsupported committed command');
  }

  checkpoint() {
    return this.snapshotManager.createSnapshot(this.currentSequence);
  }

  _applyStateTransition(payload) {
    if (payload.type === 'ORDER_CREATE') {
      const res = this.engine.processOrder(
        toUnsignedBigInt(payload.orderId, 'orderId', { positive: true }),
        toUnsignedBigInt(payload.userId, 'userId'),
        toUnsignedBigInt(payload.price, 'price', { positive: true }),
        toUnsignedBigInt(payload.amount, 'amount', { positive: true }),
        payload.side,
        payload.stpMode
      );
      return res.fills;
    } else if (payload.type === 'ORDER_CANCEL') {
      const reqUserId = payload.userId !== undefined && payload.userId !== null
        ? toUnsignedBigInt(payload.userId, 'userId')
        : null;
      this.engine.book.cancel(
        toUnsignedBigInt(payload.orderId, 'orderId', { positive: true }),
        reqUserId
      );
      return [];
    }
    return [];
  }
}

module.exports = { Sequencer };