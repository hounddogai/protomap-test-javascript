'use strict';

/**
 * The gRPC boundary of VenueSectionValidationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/venue-section-validation');

module.exports = {
  checkVenueSectionEligibility(call, callback) {
    operations.checkVenueSectionEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  explainVenueSectionDecision: unary(operations.explainVenueSectionDecision),
  getVenueSectionRules: unary(operations.getVenueSectionRules),
  lintVenueSections(call, callback) {
    operations.lintVenueSections(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  normalizeVenueSection: unary(operations.normalizeVenueSection),
  previewVenueSection: unary(operations.previewVenueSection),
  scoreVenueSection(call, callback) {
    operations.scoreVenueSection(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  validateVenueSection: unary(operations.validateVenueSection),
};
