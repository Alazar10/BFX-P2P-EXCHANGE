'use strict';

const { DoublyLinkedList } = require('./DoublyLinkedList');
const { toUnsignedBigInt } = require('./Integer');

class OrderBook {
  /**
   * @param {import('../memory/OrderPool').OrderPool} pool
   */
  constructor(pool) {
    this.pool = pool;
    this.bids = new Map();
    this.asks = new Map();
    this.orderMap = new Map();

    // Expose both naming conventions for sorted price levels
    this.sortedBidPrices = [];
    this.sortedAskPrices = [];
  }

  // Compatibility getters/aliases
  get sortedBids() {
    return this.sortedBidPrices;
  }
  get sortedAsks() {
    return this.sortedAskPrices;
  }

  getBestBid() {
    return this.sortedBidPrices.length > 0 ? this.sortedBidPrices[0] : null;
  }

  getBestAsk() {
    return this.sortedAskPrices.length > 0 ? this.sortedAskPrices[0] : null;
  }

  clear() {
    this.bids.clear();
    this.asks.clear();
    this.orderMap.clear();
    this.sortedBidPrices = [];
    this.sortedAskPrices = [];
  }

  addRestingOrder(orderId, userId, price, amount, side) {
    const normalizedOrderId = toUnsignedBigInt(orderId, 'orderId', { positive: true });
    const normalizedPrice = toUnsignedBigInt(price, 'price', { positive: true });
    if (this.orderMap.has(normalizedOrderId)) {
      throw new Error('DUPLICATE_ORDER_ID: Order ID is already resting');
    }
    const ptr = this.pool.alloc(orderId, userId, price, amount, side);
    this.orderMap.set(normalizedOrderId, ptr);

    const tree = side === 0 ? this.bids : this.asks;
    let level = tree.get(normalizedPrice);

    if (!level) {
      level = {
        queue: new DoublyLinkedList(this.pool),
        totalVolume: 0n
      };
      tree.set(normalizedPrice, level);
      this._insertPriceLevel(side, normalizedPrice);
    }

    level.queue.append(ptr);
    level.totalVolume += amount;

    return ptr;
  }

  cancel(orderId, requestingUserId = null) {
    const ptr = this.orderMap.get(orderId);
    if (ptr === undefined) return false;

    if (requestingUserId !== null && requestingUserId !== undefined) {
      const ownerId = this.pool.userId[ptr];
      if (ownerId !== requestingUserId) {
        throw new Error('UNAUTHORIZED_CANCEL: Order does not belong to user');
      }
    }

    const price = this.pool.price[ptr];
    const amount = this.pool.amount[ptr];
    const side = this.pool.side[ptr];
    const tree = side === 0 ? this.bids : this.asks;

    const level = tree.get(price);
    if (level) {
      const queue = level.queue || level;
      if (typeof queue.unlink === 'function') {
        queue.unlink(ptr);
      } else if (typeof queue.remove === 'function') {
        queue.remove(ptr);
      }

      if (level.totalVolume !== undefined) {
        level.totalVolume -= amount;
        if (level.totalVolume < 0n) level.totalVolume = 0n;
      }

      const isEmpty = (queue.length !== undefined && queue.length === 0) ||
                      (queue.isEmpty && queue.isEmpty()) ||
                      (queue.head === -1);

      if (isEmpty) {
        tree.delete(price);
        this._removePriceLevel(side, price);
      }
    }

    this.orderMap.delete(orderId);
    this.pool.free(ptr);
    return true;
  }

  _insertPriceLevel(side, price) {
    const arr = side === 0 ? this.sortedBidPrices : this.sortedAskPrices;
    let low = 0;
    let high = arr.length;

    while (low < high) {
      const mid = (low + high) >>> 1;
      // Bids: descending (highest price first); Asks: ascending (lowest price first)
      const cmp = side === 0 ? arr[mid] < price : arr[mid] > price;
      if (cmp) {
        high = mid;
      } else {
        low = mid + 1;
      }
    }
    arr.splice(low, 0, price);
  }

  _removePriceLevel(side, price) {
    const arr = side === 0 ? this.sortedBidPrices : this.sortedAskPrices;
    const idx = arr.indexOf(price);
    if (idx !== -1) arr.splice(idx, 1);
  }
}

module.exports = { OrderBook };