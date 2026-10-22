'use strict';

/**
 * The gRPC boundary of SponsorValidationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/sponsor-validation');

module.exports = {
  checkSponsorEligibility(call, callback) {
    operations.checkSponsorEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  explainSponsorDecision: unary(operations.explainSponsorDecision),
  getSponsorRules: unary(operations.getSponsorRules),
  normalizeSponsor(call, callback) {
    operations.normalizeSponsor(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  previewSponsor: unary(operations.previewSponsor),
  scoreSponsor: unary(operations.scoreSponsor),
  validateSponsor(call, callback) {
    operations.validateSponsor(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
};
