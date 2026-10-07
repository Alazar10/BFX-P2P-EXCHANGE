'use strict';

const STPMode = Object.freeze({
  CANCEL_TAKER: 0,
  CANCEL_MAKER: 1,
  CANCEL_BOTH: 2
});

class MatchingEngine {
  /**
   * @param {import('./OrderBook').OrderBook} book
   */
  constructor(book) {
    this.book = book;
    this.pool = book.pool;
  }

  processOrder(orderId, userId, price, amount, side, stpMode = STPMode.CANCEL_TAKER) {
    if (typeof orderId !== 'bigint' || typeof userId !== 'bigint' ||
        typeof price !== 'bigint' || typeof amount !== 'bigint') {
      throw new TypeError('MatchingEngine: Numeric parameters must be BigInt');
    }
    if (price <= 0n || amount <= 0n) {
      throw new RangeError('MatchingEngine: Price and amount must be positive non-zero BigInts');
    }

    const fills = [];
    let remaining = amount;

    const isBuy = side === 0;
    const opposingLevels = isBuy ? this.book.asks : this.book.bids;
    const sortedPrices = isBuy ? this.book.sortedAskPrices : this.book.sortedBidPrices;

    while (sortedPrices.length > 0 && remaining > 0n) {
      const bestPrice = sortedPrices[0];

      if (isBuy && price < bestPrice) break;
      if (!isBuy && price > bestPrice) break;

      const level = opposingLevels.get(bestPrice);
      let makerPtr = level.queue.head;

      while (makerPtr !== -1 && remaining > 0n) {
        const nextMakerPtr = this.pool.next[makerPtr];
        const makerUserId = this.pool.userId[makerPtr];
        const makerOrderId = this.pool.id[makerPtr];

        if (makerUserId === userId) {
          if (stpMode === STPMode.CANCEL_TAKER) {
            return { fills, remainingAmount: 0n };
          }
          if (stpMode === STPMode.CANCEL_MAKER || stpMode === STPMode.CANCEL_BOTH) {
            this.book.cancel(makerOrderId);
            if (stpMode === STPMode.CANCEL_BOTH) {
              return { fills, remainingAmount: 0n };
            }
            makerPtr = nextMakerPtr;
            continue;
          }
        }

        const makerAmount = this.pool.amount[makerPtr];
        const fillAmount = remaining < makerAmount ? remaining : makerAmount;

        fills.push({
          makerOrderId,
          takerOrderId: orderId,
          price: bestPrice,
          amount: fillAmount,
          makerUserId,
          takerUserId: userId
        });

        remaining -= fillAmount;
        this.pool.amount[makerPtr] -= fillAmount;
        level.totalVolume -= fillAmount;

        if (this.pool.amount[makerPtr] === 0n) {
          level.queue.remove(makerPtr);
          this.book.orderMap.delete(makerOrderId);
          this.pool.free(makerPtr);
        }

        makerPtr = nextMakerPtr;
      }

      if (level.queue.isEmpty()) {
        opposingLevels.delete(bestPrice);
        sortedPrices.shift();
      }
    }

    if (remaining > 0n) {
      this.book.addRestingOrder(orderId, userId, price, remaining, side);
    }

    return { fills, remainingAmount: remaining };
  }
}

module.exports = { MatchingEngine, STPMode };