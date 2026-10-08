'use strict';

const STPMode = Object.freeze({
  NONE: 0,
  CANCEL_TAKER: 1,
  CANCEL_MAKER: 2,
  CANCEL_BOTH: 3
});

class MatchingEngine {
  /**
   * @param {import('./OrderBook').OrderBook} book
   */
  constructor(book) {
    this.book = book;
    this.pool = book.pool;
  }

  /**
   * Deterministic matching cycle
   * @returns {{ fills: Array<object>, remainingAmount: bigint }}
   */
  processOrder(orderId, userId, price, amount, side, stpMode = STPMode.NONE) {
    let remainingAmount = amount;
    const fills = [];

    // Taker is BUY (side 0) -> matches against resting ASKS (ascending)
    // Taker is SELL (side 1) -> matches against resting BIDS (descending)
    const isBuy = side === 0;

    while (remainingAmount > 0n) {
      const priceLevels = isBuy ? this.book.sortedAskPrices : this.book.sortedBidPrices;
      if (!priceLevels || priceLevels.length === 0) break;

      const bestPrice = priceLevels[0];

      // Price limit check
      if (isBuy && bestPrice > price) break;
      if (!isBuy && bestPrice < price) break;

      const tree = isBuy ? this.book.asks : this.book.bids;
      const level = tree.get(bestPrice);
      if (!level || !level.queue || level.queue.head === -1) {
        // Empty level guard
        priceLevels.shift();
        tree.delete(bestPrice);
        continue;
      }

      const makerPtr = level.queue.head;
      const makerUserId = this.pool.userId[makerPtr];
      const makerOrderId = this.pool.orderId[makerPtr];
      const makerAmount = this.pool.amount[makerPtr];

      // 1. Native Self-Trade Prevention (STP) Intercept
      if (makerUserId === userId && stpMode !== STPMode.NONE) {
        if (stpMode === STPMode.CANCEL_TAKER) {
          remainingAmount = 0n;
          break; // Stop matching; taker cancelled
        } else if (stpMode === STPMode.CANCEL_MAKER) {
          this.book.cancel(makerOrderId);
          continue; // Maker cancelled; continue matching against next order
        } else if (stpMode === STPMode.CANCEL_BOTH) {
          this.book.cancel(makerOrderId);
          remainingAmount = 0n;
          break;
        }
      }

      // 2. Compute fill execution
      const fillAmount = remainingAmount < makerAmount ? remainingAmount : makerAmount;

      fills.push({
        makerOrderId,
        takerOrderId: orderId,
        price: bestPrice,
        amount: fillAmount,
        makerUserId,
        takerUserId: userId
      });

      remainingAmount -= fillAmount;
      const updatedMakerAmount = makerAmount - fillAmount;

      if (updatedMakerAmount === 0n) {
        // Fully filled maker order: remove from book and pool
        this.book.cancel(makerOrderId);
      } else {
        // Partial fill: update resting order amount and level total volume
        this.pool.amount[makerPtr] = updatedMakerAmount;
        if (level.totalVolume !== undefined) {
          level.totalVolume -= fillAmount;
        }
      }
    }

    // 3. Any unfilled balance rests on the book
    if (remainingAmount > 0n) {
      this.book.addRestingOrder(orderId, userId, price, remainingAmount, side);
    }

    return { fills, remainingAmount };
  }
}

module.exports = { MatchingEngine, STPMode };