'use strict';

class OrderPool {
  /**
   * Contiguous TypedArray Arena for orders.
   * Eliminates dynamic V8 heap object creation and GC pauses during trading.
   * @param {number} capacity - Maximum number of concurrent active orders
   */
  constructor(capacity = 500000) {
    this.capacity = capacity;
    this.allocatedCount = 0;
    this.freeHead = 0;

    // Fixed-size columnar arrays (structure of arrays)
    this.orderId = new BigUint64Array(capacity);
    this.userId = new BigUint64Array(capacity);
    this.price = new BigUint64Array(capacity);
    this.amount = new BigUint64Array(capacity);
    this.side = new Uint8Array(capacity); // 0 = BID, 1 = ASK

    // Intrusive doubly linked list pointers for O(1) queue splicing
    this.prev = new Int32Array(capacity);
    this.next = new Int32Array(capacity);

    // Initialize free list chain
    this._initFreeList();
  }

  _initFreeList() {
    this.freeHead = 0;
    this.allocatedCount = 0;
    for (let i = 0; i < this.capacity; i++) {
      this.next[i] = i + 1 < this.capacity ? i + 1 : -1;
      this.prev[i] = -1;
      this.orderId[i] = 0n;
      this.userId[i] = 0n;
      this.price[i] = 0n;
      this.amount[i] = 0n;
      this.side[i] = 0;
    }
  }

  /**
   * Resets the entire arena back to clean initial state.
   * Used during snapshot hydration without reallocating buffers.
   */
  reset() {
    this._initFreeList();
  }

  /**
   * Allocates an order slot in O(1) time.
   * @returns {number} Memory pointer index
   */
  alloc(orderId, userId, price, amount, side) {
    if (this.freeHead === -1) {
      throw new Error(`ORDER_POOL_EXHAUSTED: Capacity of ${this.capacity} reached`);
    }

    const ptr = this.freeHead;
    this.freeHead = this.next[ptr];

    this.orderId[ptr] = BigInt(orderId);
    this.userId[ptr] = BigInt(userId);
    this.price[ptr] = BigInt(price);
    this.amount[ptr] = BigInt(amount);
    this.side[ptr] = Number(side);

    this.prev[ptr] = -1;
    this.next[ptr] = -1;
    this.allocatedCount++;

    return ptr;
  }

  // Alias for backward compatibility
  allocate(orderId, userId, price, amount, side) {
    return this.alloc(orderId, userId, price, amount, side);
  }

  /**
   * Releases an order slot back to the free list in O(1) time.
   * @param {number} ptr - Memory pointer index
   */
  free(ptr) {
    if (ptr < 0 || ptr >= this.capacity) return;

    this.orderId[ptr] = 0n;
    this.userId[ptr] = 0n;
    this.price[ptr] = 0n;
    this.amount[ptr] = 0n;
    this.side[ptr] = 0;
    this.prev[ptr] = -1;

    this.next[ptr] = this.freeHead;
    this.freeHead = ptr;
    this.allocatedCount--;
  }
}

module.exports = { OrderPool };