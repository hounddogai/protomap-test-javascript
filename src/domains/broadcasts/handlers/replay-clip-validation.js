'use strict';

/**
 * The gRPC boundary of ReplayClipValidationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/replay-clip-validation');

module.exports = {
  checkReplayClipEligibility(call, callback) {
    operations.checkReplayClipEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  explainReplayClipDecision: unary(operations.explainReplayClipDecision),
  getReplayClipRules: unary(operations.getReplayClipRules),
  normalizeReplayClip(call, callback) {
    operations.normalizeReplayClip(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  previewReplayClip: unary(operations.previewReplayClip),
  validateReplayClip: unary(operations.validateReplayClip),
};
