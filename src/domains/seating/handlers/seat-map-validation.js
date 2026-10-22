'use strict';

/**
 * The gRPC boundary of SeatMapValidationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/seat-map-validation');

module.exports = {
  checkSeatMapEligibility(call, callback) {
    operations.checkSeatMapEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  explainSeatMapDecision: unary(operations.explainSeatMapDecision),
  getSeatMapRules: unary(operations.getSeatMapRules),
  lintSeatMaps(call, callback) {
    operations.lintSeatMaps(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  normalizeSeatMap: unary(operations.normalizeSeatMap),
  previewSeatMap: unary(operations.previewSeatMap),
  scoreSeatMap(call, callback) {
    operations.scoreSeatMap(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  validateSeatMap: unary(operations.validateSeatMap),
};
