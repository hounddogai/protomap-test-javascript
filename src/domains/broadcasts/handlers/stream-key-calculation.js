'use strict';

/**
 * The gRPC boundary of StreamKeyCalculationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/stream-key-calculation');

module.exports = {
  calculateStreamKey(call, callback) {
    operations.calculateStreamKey(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  compareStreamKeyOptions: unary(operations.compareStreamKeyOptions),
  estimateStreamKey: unary(operations.estimateStreamKey),
  explainStreamKeyCalculation(call, callback) {
    operations.explainStreamKeyCalculation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getStreamKeyRates: unary(operations.getStreamKeyRates),
  quoteStreamKey: unary(operations.quoteStreamKey),
  recalculateStreamKeys(call, callback) {
    operations.recalculateStreamKeys(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  simulateStreamKey: unary(operations.simulateStreamKey),
};
