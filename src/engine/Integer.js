'use strict';

const MAX_INTEGER_DIGITS = 256;

function toUnsignedBigInt(value, field, { positive = false } = {}) {
  let text;
  if (typeof value === 'bigint') {
    text = value.toString();
  } else if (typeof value === 'string') {
    text = value;
  } else {
    throw new Error(`INVALID_INTEGER: ${field} must be a decimal string or BigInt`);
  }

  if (text.length > MAX_INTEGER_DIGITS ||
      !/^(0|[1-9][0-9]*)$/.test(text)) {
    throw new Error(`INVALID_INTEGER: ${field} must be a canonical unsigned decimal integer`);
  }

  const parsed = BigInt(text);
  if (positive && parsed === 0n) {
    throw new Error(`INVALID_INTEGER: ${field} must be greater than zero`);
  }
  return parsed;
}

module.exports = { MAX_INTEGER_DIGITS, toUnsignedBigInt };
