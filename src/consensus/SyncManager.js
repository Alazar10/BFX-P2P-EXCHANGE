'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs');

class SyncManager {
  constructor(sequencer, snapshotManager, transport) {
    this.sequencer = sequencer;
    this.snapshotManager = snapshotManager;
    this.transport = transport;
    this.isSyncing = false;
  }

  handleSyncRequest(request) {
    const followerSeq = BigInt(request.lastAppliedSeqId);
    const currentSeq = this.sequencer.currentSequence;

    if (followerSeq === currentSeq) {
      return { status: 'IN_SYNC', currentSeq: currentSeq.toString() };
    }

    const deltas = [];
    let canDeltaReplay = false;

    if (fs.existsSync(this.sequencer.wal.filePath)) {
      this.sequencer.wal.replay((entry) => {
        if (entry.type !== 'ORDER_CREATE' && entry.type !== 'ORDER_CANCEL') return;
        const entrySeq = BigInt(entry.seqId);
        if (entrySeq > followerSeq) {
          deltas.push(entry);
        }
      });
      let expectedSeq = followerSeq + 1n;
      canDeltaReplay = deltas.length > 0;
      for (const entry of deltas) {
        if (BigInt(entry.seqId) !== expectedSeq++) {
          canDeltaReplay = false;
          break;
        }
      }
      canDeltaReplay = canDeltaReplay && expectedSeq - 1n === currentSeq;
    }

    if (canDeltaReplay) {
      return {
        status: 'DELTA_STREAM',
        currentSeq: currentSeq.toString(),
        deltas
      };
    }

    const snapshotRaw = fs.readFileSync(this.snapshotManager.snapshotPath, 'utf8');
    const checksum = crypto.createHash('sha256').update(snapshotRaw).digest('hex');

    return {
      status: 'SNAPSHOT_TRANSFER',
      currentSeq: currentSeq.toString(),
      snapshotPayload: JSON.parse(snapshotRaw),
      checksum
    };
  }

  async synchronizeWithLeader(leaderNodeId) {
    if (this.isSyncing) return;
    this.isSyncing = true;
    this.sequencer.isPaused = true; // Pause local processing

    try {
      const mySeq = this.sequencer.currentSequence;
      const response = await this.transport.send({
        action: 'SYNC_REQUEST',
        lastAppliedSeqId: mySeq.toString()
      });

      if (response.status === 'IN_SYNC') return;

      if (response.status === 'DELTA_STREAM') {
        for (const entry of response.deltas) {
          // Replay using canonical sequence IDs
          this.sequencer.applyCommitted(entry);
        }
      } else if (response.status === 'SNAPSHOT_TRANSFER') {
        const raw = JSON.stringify(response.snapshotPayload);
        const calcChecksum = crypto.createHash('sha256').update(raw).digest('hex');

        if (calcChecksum !== response.checksum) {
          throw new Error('SYNC_INTEGRITY_VIOLATION: Snapshot checksum mismatch');
        }

        const snapshotSeq = BigInt(response.snapshotPayload.lastAppliedSeqId);
        if (snapshotSeq < mySeq) {
          throw new Error('SYNC_REGRESSION: Leader snapshot is behind follower state');
        }
        this.snapshotManager.installSnapshot(response.snapshotPayload);
        this.sequencer.currentSequence = snapshotSeq;
      }
    } finally {
      this.sequencer.isPaused = false;
      this.isSyncing = false;
    }
  }
}

module.exports = { SyncManager };