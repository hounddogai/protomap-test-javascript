'use strict';

/**
 * The gRPC boundary of SponsorshipDealCalculationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/sponsorship-deal-calculation');

module.exports = {
  calculateSponsorshipDeal(call, callback) {
    operations.calculateSponsorshipDeal(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  compareSponsorshipDealOptions: unary(operations.compareSponsorshipDealOptions),
  estimateSponsorshipDeal: unary(operations.estimateSponsorshipDeal),
  getSponsorshipDealRates(call, callback) {
    operations.getSponsorshipDealRates(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  quoteSponsorshipDeal: unary(operations.quoteSponsorshipDeal),
  recalculateSponsorshipDeals: unary(operations.recalculateSponsorshipDeals),
};
