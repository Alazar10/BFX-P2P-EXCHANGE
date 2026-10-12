'use strict';

const { toUnsignedBigInt } = require('../engine/Integer');

class OrderPool {
  /**
   * Contiguous TypedArray Arena for orders.
   * Eliminates dynamic V8 heap object creation and GC pauses during trading.
   * @param {number} capacity - Maximum number of concurrent active orders
   */
  constructor(capacity = 500000) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0 || capacity > 0x7fffffff) {
      throw new Error('INVALID_ORDER_POOL_CAPACITY: Capacity must be a positive 32-bit integer');
    }
    this.capacity = capacity;
    this.allocatedCount = 0;
    this.freeHead = 0;

    // Fixed-size columns preserve exact BigInt values without uint64 truncation.
    this.orderId = new Array(capacity).fill(0n);
    this.userId = new Array(capacity).fill(0n);
    this.price = new Array(capacity).fill(0n);
    this.amount = new Array(capacity).fill(0n);
    this.side = new Uint8Array(capacity); // 0 = BID, 1 = ASK

    // Intrusive doubly linked list pointers for O(1) queue splicing
    this.prev = new Int32Array(capacity);
    this.next = new Int32Array(capacity);
    this.allocated = new Uint8Array(capacity);
    this.linked = new Uint8Array(capacity);

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
      this.allocated[i] = 0;
      this.linked[i] = 0;
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
    const id = toUnsignedBigInt(orderId, 'orderId', { positive: true });
    const owner = toUnsignedBigInt(userId, 'userId');
    const limitPrice = toUnsignedBigInt(price, 'price', { positive: true });
    const size = toUnsignedBigInt(amount, 'amount', { positive: true });
    if (id <= 0n || owner < 0n || limitPrice <= 0n || size <= 0n ||
        !Number.isInteger(side) || (side !== 0 && side !== 1)) {
      throw new Error('INVALID_ORDER: Invalid order-pool fields');
    }
    if (this.freeHead === -1) {
      throw new Error(`ORDER_POOL_EXHAUSTED: Capacity of ${this.capacity} reached`);
    }

    const ptr = this.freeHead;
    this.freeHead = this.next[ptr];

    this.orderId[ptr] = id;
    this.userId[ptr] = owner;
    this.price[ptr] = limitPrice;
    this.amount[ptr] = size;
    this.side[ptr] = side;
    this.allocated[ptr] = 1;
    this.linked[ptr] = 0;

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
    if (!Number.isInteger(ptr) || ptr < 0 || ptr >= this.capacity || !this.allocated[ptr]) {
      throw new Error('INVALID_ORDER_POOL_FREE: Slot is not allocated');
    }
    if (this.linked[ptr]) {
      throw new Error('INVALID_ORDER_POOL_FREE: Cannot free a slot linked to an order queue');
    }

    this.orderId[ptr] = 0n;
    this.userId[ptr] = 0n;
    this.price[ptr] = 0n;
    this.amount[ptr] = 0n;
    this.side[ptr] = 0;
    this.prev[ptr] = -1;
    this.allocated[ptr] = 0;

    this.next[ptr] = this.freeHead;
    this.freeHead = ptr;
    this.allocatedCount--;
  }
}

module.exports = { OrderPool };