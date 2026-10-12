'use strict';

const { STPMode } = require('./MatchingEngine');
const { MAX_INTEGER_DIGITS, toUnsignedBigInt } = require('./Integer');

const CREATE_FIELDS = new Set([
  'type',
  'orderId',
  'price',
  'amount',
  'side',
  'stpMode',
  'requestId'
]);
const CANCEL_FIELDS = new Set(['type', 'orderId', 'requestId']);
const REQUEST_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function assertPlainObject(value, field = 'command') {
  if (!value || typeof value !== 'object' || Array.isArray(value) ||
      Object.getPrototypeOf(value) !== Object.prototype) {
    throw new Error(`INVALID_SCHEMA: ${field} must be a plain object`);
  }
}

function assertAllowedFields(value, fields) {
  for (const field of Object.keys(value)) {
    if (!fields.has(field)) {
      throw new Error(`INVALID_SCHEMA: Unexpected field "${field}"`);
    }
  }
}

function validateClientCommand(command) {
  assertPlainObject(command);
  if (typeof command.requestId !== 'string' || !REQUEST_ID_PATTERN.test(command.requestId)) {
    throw new Error('INVALID_SCHEMA: requestId must be a UUID v4 for safe retries');
  }

  if (command.type === 'ORDER_CREATE') {
    assertAllowedFields(command, CREATE_FIELDS);
    const stpMode = command.stpMode === undefined
      ? STPMode.CANCEL_TAKER
      : command.stpMode;
    if (!Number.isInteger(command.side) || (command.side !== 0 && command.side !== 1)) {
      throw new Error('INVALID_SCHEMA: side must be 0 (BUY) or 1 (SELL)');
    }
    if (!Number.isInteger(stpMode) ||
        ![STPMode.CANCEL_TAKER, STPMode.CANCEL_MAKER, STPMode.CANCEL_BOTH].includes(stpMode)) {
      throw new Error('INVALID_SCHEMA: stpMode must enable self-trade prevention');
    }

    return {
      type: 'ORDER_CREATE',
      orderId: toUnsignedBigInt(command.orderId, 'orderId', { positive: true }),
      price: toUnsignedBigInt(command.price, 'price', { positive: true }),
      amount: toUnsignedBigInt(command.amount, 'amount', { positive: true }),
      side: command.side,
      stpMode,
      requestId: command.requestId
    };
  }

  if (command.type === 'ORDER_CANCEL') {
    assertAllowedFields(command, CANCEL_FIELDS);
    return {
      type: 'ORDER_CANCEL',
      orderId: toUnsignedBigInt(command.orderId, 'orderId', { positive: true }),
      requestId: command.requestId
    };
  }

  throw new Error('INVALID_COMMAND_TYPE: Unsupported order command');
}

module.exports = {
  MAX_INTEGER_DIGITS,
  validateClientCommand
};
