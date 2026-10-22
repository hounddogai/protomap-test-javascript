'use strict';

/**
 * The gRPC boundary of ConcessionStandCalculationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/concession-stand-calculation');

module.exports = {
  calculateConcessionStand(call, callback) {
    operations.calculateConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  compareConcessionStandOptions: unary(operations.compareConcessionStandOptions),
  estimateConcessionStand: unary(operations.estimateConcessionStand),
  explainConcessionStandCalculation(call, callback) {
    operations.explainConcessionStandCalculation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getConcessionStandRates: unary(operations.getConcessionStandRates),
  quoteConcessionStand: unary(operations.quoteConcessionStand),
  recalculateConcessionStands(call, callback) {
    operations.recalculateConcessionStands(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  simulateConcessionStand: unary(operations.simulateConcessionStand),
};
