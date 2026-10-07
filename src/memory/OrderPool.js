'use strict';

class OrderPool {
  /**
   * @param {number} capacity - Maximum concurrent resting order slots
   */
  constructor(capacity = 200000) {
    if (typeof capacity !== 'number' || capacity <= 0 || !Number.isInteger(capacity)) {
      throw new TypeError('OrderPool: capacity must be a positive integer');
    }

    this.capacity = capacity;

    this.id = new BigUint64Array(capacity);
    this.userId = new BigUint64Array(capacity);
    this.price = new BigUint64Array(capacity);
    this.amount = new BigUint64Array(capacity);
    this.side = new Uint8Array(capacity); // 0 = BUY, 1 = SELL
    this.prev = new Int32Array(capacity).fill(-1);
    this.next = new Int32Array(capacity).fill(-1);

    this.freeList = new Int32Array(capacity);
    this.freeHead = capacity - 1;

    for (let i = 0; i < capacity; i++) {
      this.freeList[i] = i;
    }
  }

  /**
   * O(1) Memory allocation
   * @returns {number} pointer index
   */
  allocate(id, userId, price, amount, side) {
    if (typeof id !== 'bigint' || typeof userId !== 'bigint' || 
        typeof price !== 'bigint' || typeof amount !== 'bigint') {
      throw new TypeError('OrderPool.allocate: id, userId, price, and amount MUST be BigInt');
    }
    if (typeof side !== 'number' || (side !== 0 && side !== 1)) {
      throw new TypeError('OrderPool.allocate: side must be 0 (BUY) or 1 (SELL)');
    }
    if (this.freeHead < 0) {
      throw new RangeError('CRITICAL: OrderPool exhausted. Increase arena capacity.');
    }

    const ptr = this.freeList[this.freeHead--];
    this.id[ptr] = id;
    this.userId[ptr] = userId;
    this.price[ptr] = price;
    this.amount[ptr] = amount;
    this.side[ptr] = side;
    this.prev[ptr] = -1;
    this.next[ptr] = -1;

    return ptr;
  }

  /**
   * O(1) Memory deallocation
   * @param {number} ptr
   */
  free(ptr) {
    if (typeof ptr !== 'number' || ptr < 0 || ptr >= this.capacity) {
      throw new RangeError('OrderPool.free: Invalid pointer index');
    }
    this.prev[ptr] = -1;
    this.next[ptr] = -1;
    this.freeList[++this.freeHead] = ptr;
  }
}

module.exports = { OrderPool };