'use strict';

/**
 * The gRPC boundary of SponsorshipDealWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/sponsorship-deal-workflow');

module.exports = {
  advanceSponsorshipDealWorkflow(call, callback) {
    operations.advanceSponsorshipDealWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelSponsorshipDealWorkflow: unary(operations.cancelSponsorshipDealWorkflow),
  inspectSponsorshipDealWorkflow: unary(operations.inspectSponsorshipDealWorkflow),
  listSponsorshipDealWorkflows(call, callback) {
    operations.listSponsorshipDealWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  pauseSponsorshipDealWorkflow: unary(operations.pauseSponsorshipDealWorkflow),
  resumeSponsorshipDealWorkflow: unary(operations.resumeSponsorshipDealWorkflow),
  retrySponsorshipDealWorkflow(call, callback) {
    operations.retrySponsorshipDealWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  startSponsorshipDealWorkflow: unary(operations.startSponsorshipDealWorkflow),
};
