'use strict';

const { SnapshotManager } = require('./SnapshotManager');

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

    this.wal.append(entry);
    const fills = this._applyStateTransition(entry);
    this.currentSequence = entrySeqId;

    if (++this.opsSinceLastSnapshot >= this.snapshotInterval) {
      this.checkpoint();
      this.opsSinceLastSnapshot = 0;
    }

    return { seqId: entrySeqId, fills };
  }

  checkpoint() {
    return this.snapshotManager.createSnapshot(this.currentSequence);
  }

  _applyStateTransition(payload) {
    if (payload.type === 'ORDER_CREATE') {
      const res = this.engine.processOrder(
        BigInt(payload.orderId),
        BigInt(payload.userId),
        BigInt(payload.price),
        BigInt(payload.amount),
        Number(payload.side),
        Number(payload.stpMode || 0)
      );
      return res.fills;
    } else if (payload.type === 'ORDER_CANCEL') {
      const reqUserId = payload.userId !== undefined && payload.userId !== null
        ? BigInt(payload.userId)
        : null;
      this.engine.book.cancel(BigInt(payload.orderId), reqUserId);
      return [];
    }
    return [];
  }
}

module.exports = { Sequencer };