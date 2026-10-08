'use strict';

class DoublyLinkedList {
  /**
   * @param {import('../memory/OrderPool').OrderPool} pool
   */
  constructor(pool) {
    this.pool = pool;
    this.head = -1;
    this.tail = -1;
  }

  /**
   * Appends an allocated order pointer to the tail (FIFO enqueue)
   * @param {number} ptr
   */
  append(ptr) {
    if (this.tail === -1) {
      this.head = ptr;
      this.tail = ptr;
    } else {
      this.pool.next[this.tail] = ptr;
      this.pool.prev[ptr] = this.tail;
      this.tail = ptr;
    }
  }

  /**
   * Unlinks an order pointer from anywhere in the list in O(1)
   * @param {number} ptr
   */
  unlink(ptr) {
    const prev = this.pool.prev[ptr];
    const next = this.pool.next[ptr];

    if (prev !== -1) {
      this.pool.next[prev] = next;
    } else {
      this.head = next;
    }

    if (next !== -1) {
      this.pool.prev[next] = prev;
    } else {
      this.tail = prev;
    }

    this.pool.prev[ptr] = -1;
    this.pool.next[ptr] = -1;
    this.length--;
  }

  remove(ptr) {
    return this.unlink(ptr);
  }

  isEmpty() {
    return this.head === -1;
  }
}

module.exports = { DoublyLinkedList };