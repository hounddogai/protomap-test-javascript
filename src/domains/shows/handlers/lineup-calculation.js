'use strict';

/**
 * The gRPC boundary of LineupCalculationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/lineup-calculation');

module.exports = {
  calculateLineup(call, callback) {
    operations.calculateLineup(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  compareLineupOptions: unary(operations.compareLineupOptions),
  estimateLineup: unary(operations.estimateLineup),
  explainLineupCalculation(call, callback) {
    operations.explainLineupCalculation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getLineupRates: unary(operations.getLineupRates),
  quoteLineup: unary(operations.quoteLineup),
  recalculateLineups(call, callback) {
    operations.recalculateLineups(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  simulateLineup: unary(operations.simulateLineup),
};
