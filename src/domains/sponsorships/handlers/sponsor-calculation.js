'use strict';

/**
 * The gRPC boundary of SponsorCalculationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/sponsor-calculation');

module.exports = {
  calculateSponsor(call, callback) {
    operations.calculateSponsor(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  compareSponsorOptions: unary(operations.compareSponsorOptions),
  estimateSponsor: unary(operations.estimateSponsor),
  explainSponsorCalculation(call, callback) {
    operations.explainSponsorCalculation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getSponsorRates: unary(operations.getSponsorRates),
  quoteSponsor: unary(operations.quoteSponsor),
  recalculateSponsors(call, callback) {
    operations.recalculateSponsors(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  simulateSponsor: unary(operations.simulateSponsor),
};
