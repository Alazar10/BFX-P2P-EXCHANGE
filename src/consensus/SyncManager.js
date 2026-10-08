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
        const entrySeq = BigInt(entry.seqId);
        if (entrySeq > followerSeq) {
          deltas.push(entry);
        }
      });
      if (deltas.length > 0 && BigInt(deltas[0].seqId) === followerSeq + 1n) {
        canDeltaReplay = true;
      }
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

        fs.writeFileSync(this.snapshotManager.snapshotPath, raw);
        this.snapshotManager.loadSnapshot(); // Wipes prior memory before loading
        this.sequencer.currentSequence = BigInt(response.snapshotPayload.lastAppliedSeqId);
      }
    } finally {
      this.sequencer.isPaused = false;
      this.isSyncing = false;
    }
  }
}

module.exports = { SyncManager };