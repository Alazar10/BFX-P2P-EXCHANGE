'use strict';

const { SnapshotManager } = require('./SnapshotManager');

class Sequencer {
  /**
   * @param {import('../engine/MatchingEngine').MatchingEngine} engine
   * @param {import('./WriteAheadLog').WriteAheadLog} wal
   * @param {number} snapshotInterval - Snapshot frequency in ticks
   * @param {string} dataDir - Isolated directory for snapshots
   */
  constructor(engine, wal, snapshotInterval = 5000, dataDir = './data') {
    this.engine = engine;
    this.wal = wal;
    this.snapshotInterval = snapshotInterval;
    this.dataDir = dataDir;
    this.currentSequence = 0n;
    this.opsSinceLastSnapshot = 0;

    this.snapshotManager = new SnapshotManager(engine.book, wal, this.dataDir);

    this._recoverState();
  }

  _recoverState() {
    const startTime = process.hrtime.bigint();

    const { lastAppliedSeqId, loadedOrders } = this.snapshotManager.loadSnapshot();
    this.currentSequence = lastAppliedSeqId;

    let replayedDeltas = 0;

    this.wal.replay((entry) => {
      const entrySeqId = BigInt(entry.seqId);
      if (entrySeqId > this.currentSequence) {
        this.currentSequence = entrySeqId;
        this._applyStateTransition(entry);
        replayedDeltas++;
      }
    });

    const elapsedMs = Number(process.hrtime.bigint() - startTime) / 1_000_000;
    if (loadedOrders > 0 || replayedDeltas > 0) {
      console.log(`[State Recovery] Loaded ${loadedOrders} resting orders from snapshot (${this.dataDir}).`);
      console.log(`[State Recovery] Replayed ${replayedDeltas} deltas from WAL.`);
      console.log(`[State Recovery] Engine restored to Seq #${this.currentSequence} in ${elapsedMs.toFixed(3)} ms\n`);
    }
  }

  process(command) {
    const seqId = ++this.currentSequence;
    const timestamp = Date.now();

    const payload = { ...command, seqId, timestamp };

    this.wal.append(payload);

    const fills = this._applyStateTransition(payload);

    if (++this.opsSinceLastSnapshot >= this.snapshotInterval) {
      this.checkpoint();
      this.opsSinceLastSnapshot = 0;
    }

    return { seqId, fills };
  }

  checkpoint() {
    const info = this.snapshotManager.createSnapshot(this.currentSequence);
    console.log(`[Checkpoint] Snapshot created at Seq #${info.lastAppliedSeqId} (${info.restingOrdersCount} active orders in ${this.dataDir})`);
  }

  _applyStateTransition(payload) {
    if (payload.type === 'ORDER_CREATE') {
      const res = this.engine.processOrder(
        BigInt(payload.orderId),
        BigInt(payload.userId),
        BigInt(payload.price),
        BigInt(payload.amount),
        Number(payload.side),
        Number(payload.stpMode)
      );
      return res.fills;
    } else if (payload.type === 'ORDER_CANCEL') {
      this.engine.book.cancel(BigInt(payload.orderId));
      return [];
    }
    return [];
  }
}

module.exports = { Sequencer };