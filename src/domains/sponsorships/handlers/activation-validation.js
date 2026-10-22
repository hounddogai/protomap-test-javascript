'use strict';

/**
 * The gRPC boundary of ActivationValidationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/activation-validation');

module.exports = {
  checkActivationEligibility(call, callback) {
    operations.checkActivationEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  explainActivationDecision: unary(operations.explainActivationDecision),
  getActivationRules: unary(operations.getActivationRules),
  lintActivations(call, callback) {
    operations.lintActivations(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  normalizeActivation: unary(operations.normalizeActivation),
  previewActivation: unary(operations.previewActivation),
  scoreActivation(call, callback) {
    operations.scoreActivation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  validateActivation: unary(operations.validateActivation),
};
