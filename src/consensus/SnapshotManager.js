'use strict';

const fs = require('node:fs');
const path = require('node:path');

class SnapshotManager {
  /**
   * @param {import('../engine/OrderBook').OrderBook} book
   * @param {import('./WriteAheadLog').WriteAheadLog} wal
   * @param {string} dataDir
   */
  constructor(book, wal, dataDir = './data') {
    this.book = book;
    this.pool = book.pool;
    this.wal = wal;
    this.dataDir = dataDir;

    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }

    this.snapshotPath = path.join(this.dataDir, 'engine.snapshot');
    this.tempSnapshotPath = path.join(this.dataDir, 'snapshot.tmp');
  }

  /**
   * Creates an atomic checkpoint of all resting orders and truncates the WAL
   * @param {bigint} lastAppliedSeqId
   */
  createSnapshot(lastAppliedSeqId) {
    const restingOrders = [];

    for (const [orderId, ptr] of this.book.orderMap.entries()) {
      restingOrders.push({
        orderId: orderId.toString(),
        userId: this.pool.userId[ptr].toString(),
        price: this.pool.price[ptr].toString(),
        amount: this.pool.amount[ptr].toString(),
        side: this.pool.side[ptr]
      });
    }

    const snapshotPayload = {
      version: 1,
      lastAppliedSeqId: lastAppliedSeqId.toString(),
      timestamp: Date.now(),
      orderCount: restingOrders.length,
      orders: restingOrders
    };

    const rawData = JSON.stringify(snapshotPayload);

    const fd = fs.openSync(this.tempSnapshotPath, 'w');
    fs.writeSync(fd, rawData);
    fs.fsyncSync(fd);
    fs.closeSync(fd);

    fs.renameSync(this.tempSnapshotPath, this.snapshotPath);

    this._compactWal();

    return {
      lastAppliedSeqId,
      restingOrdersCount: restingOrders.length,
      snapshotSizeBytes: rawData.length
    };
  }

  /**
   * Loads the snapshot and populates the OrderBook in O(R) time
   * @returns {{ lastAppliedSeqId: bigint, loadedOrders: number }}
   */
  loadSnapshot() {
    if (!fs.existsSync(this.snapshotPath)) {
      return { lastAppliedSeqId: 0n, loadedOrders: 0 };
    }

    const rawData = fs.readFileSync(this.snapshotPath, 'utf8');
    const snapshot = JSON.parse(rawData);
    const lastAppliedSeqId = BigInt(snapshot.lastAppliedSeqId);

    // Clean reset of in-memory structures
    this.book.clear();
    this.pool.reset();

    for (const ord of snapshot.orders) {
      this.book.addRestingOrder(
        BigInt(ord.orderId),
        BigInt(ord.userId),
        BigInt(ord.price),
        BigInt(ord.amount),
        Number(ord.side)
      );
    }

    return {
      lastAppliedSeqId,
      loadedOrders: snapshot.orders.length
    };
  }

  _compactWal() {
    if (this.wal.fd) {
      fs.closeSync(this.wal.fd);
    }
    this.wal.fd = fs.openSync(this.wal.filePath, 'w+');
  }
}

module.exports = { SnapshotManager };