'use strict';

/**
 * The gRPC boundary of SeatHoldQueryService.
 */

const { toServiceError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/seat-hold-query');

module.exports = {
  browseSeatHolds(call, callback) {
    operations.browseSeatHolds(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  countSeatHolds: unary(operations.countSeatHolds),
  exportSeatHolds: serverStream(operations.exportSeatHolds),
  findSeatHolds(call, callback) {
    operations.findSeatHolds(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  lookupSeatHold: unary(operations.lookupSeatHold),
  peekSeatHold: unary(operations.peekSeatHold),
  resolveSeatHolds(call, callback) {
    operations.resolveSeatHolds(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  suggestSeatHolds: unary(operations.suggestSeatHolds),
  summarizeSeatHolds: unary(operations.summarizeSeatHolds),
};
