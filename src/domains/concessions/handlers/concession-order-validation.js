'use strict';

/**
 * The gRPC boundary of ConcessionOrderValidationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/concession-order-validation');

module.exports = {
  explainConcessionOrderDecision(call, callback) {
    operations.explainConcessionOrderDecision(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getConcessionOrderRules: unary(operations.getConcessionOrderRules),
  normalizeConcessionOrder: unary(operations.normalizeConcessionOrder),
  previewConcessionOrder(call, callback) {
    operations.previewConcessionOrder(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  scoreConcessionOrder: unary(operations.scoreConcessionOrder),
  validateConcessionOrder: unary(operations.validateConcessionOrder),
};
