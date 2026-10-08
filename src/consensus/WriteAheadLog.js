'use strict';

const fs = require('node:fs');
const path = require('node:path');

class WriteAheadLog {
  constructor(filePath = './data/engine.wal') {
    this.filePath = filePath;

    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    this.fd = fs.openSync(filePath, 'a+');
  }

  append(payload, { sync = true } = {}) {
    const rawJson = JSON.stringify(payload, (_, value) =>
      typeof value === 'bigint' ? value.toString() : value
    );
    const bodyBuf = Buffer.from(rawJson, 'utf8');
    const lenBuf = Buffer.allocUnsafe(4);
    lenBuf.writeUInt32BE(bodyBuf.length, 0);

    const frame = Buffer.concat([lenBuf, bodyBuf]);
    fs.writeSync(this.fd, frame, 0, frame.length);
    if (sync) fs.fsyncSync(this.fd);
  }

  reset({ preserveTermVote = true } = {}) {
    let latestTermVote = null;
    if (preserveTermVote) {
      this.replay((record) => {
        if (record.type === 'RAFT_TERM_VOTE') latestTermVote = record;
      });
    }
    if (this.fd === null) {
      this.fd = fs.openSync(this.filePath, 'a+');
    }
    fs.ftruncateSync(this.fd, 0);
    fs.fsyncSync(this.fd);
    if (latestTermVote) this.append(latestTermVote);
  }

  /**
   * Replays WAL from disk to restore state on crash recovery.
   * @param {function(object): void} onEvent
   */
  replay(onEvent) {
    if (!fs.existsSync(this.filePath)) return;
    const fileBuf = fs.readFileSync(this.filePath);
    let offset = 0;

    while (offset < fileBuf.length) {
      if (offset + 4 > fileBuf.length) break;
      const len = fileBuf.readUInt32BE(offset);
      offset += 4;
      if (len > fileBuf.length - offset) break;

      if (offset + len > fileBuf.length) break;
      const bodyStr = fileBuf.toString('utf8', offset, offset + len);
      offset += len;

      const parsed = JSON.parse(bodyStr);
      onEvent(parsed);
    }
  }

  close() {
    if (this.fd) {
      fs.closeSync(this.fd);
      this.fd = null;
    }
  }
}

module.exports = { WriteAheadLog };