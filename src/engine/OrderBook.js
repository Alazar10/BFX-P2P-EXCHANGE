'use strict';

const { DoublyLinkedList } = require('./DoublyLinkedList');

class OrderBook {
  /**
   * @param {import('../memory/OrderPool').OrderPool} pool
   */
  constructor(pool) {
    this.pool = pool;

    /** @type {Map<bigint, number>} orderId -> pool pointer */
    this.orderMap = new Map();

    /** @type {Map<bigint, {price: bigint, totalVolume: bigint, queue: DoublyLinkedList}>} */
    this.bids = new Map();
    /** @type {Map<bigint, {price: bigint, totalVolume: bigint, queue: DoublyLinkedList}>} */
    this.asks = new Map();

    /** @type {bigint[]} */
    this.sortedBidPrices = [];
    /** @type {bigint[]} */
    this.sortedAskPrices = [];
  }

  addRestingOrder(orderId, userId, price, amount, side) {
    const ptr = this.pool.allocate(orderId, userId, price, amount, side);
    this.orderMap.set(orderId, ptr);

    const levels = side === 0 ? this.bids : this.asks;
    let level = levels.get(price);

    if (!level) {
      level = {
        price,
        totalVolume: 0n,
        queue: new DoublyLinkedList(this.pool)
      };
      levels.set(price, level);
      this._insertSortedPrice(side, price);
    }

    level.totalVolume += amount;
    level.queue.append(ptr);
    return ptr;
  }

  /**
   * O(1) Cancellation by orderId
   * @param {bigint} orderId
   * @returns {boolean}
   */
  cancel(orderId) {
    const ptr = this.orderMap.get(orderId);
    if (ptr === undefined) return false;

    const price = this.pool.price[ptr];
    const side = this.pool.side[ptr];
    const amount = this.pool.amount[ptr];

    const levels = side === 0 ? this.bids : this.asks;
    const level = levels.get(price);

    if (level) {
      level.queue.remove(ptr);
      level.totalVolume -= amount;

      if (level.queue.isEmpty()) {
        levels.delete(price);
        this._removeSortedPrice(side, price);
      }
    }

    this.orderMap.delete(orderId);
    this.pool.free(ptr);
    return true;
  }

  _insertSortedPrice(side, price) {
    const arr = side === 0 ? this.sortedBidPrices : this.sortedAskPrices;
    arr.push(price);

    arr.sort((a, b) => (side === 0 ? (a > b ? -1 : 1) : (a < b ? -1 : 1)));
  }

  _removeSortedPrice(side, price) {
    const arr = side === 0 ? this.sortedBidPrices : this.sortedAskPrices;
    const idx = arr.indexOf(price);
    if (idx !== -1) arr.splice(idx, 1);
  }
}

module.exports = { OrderBook };