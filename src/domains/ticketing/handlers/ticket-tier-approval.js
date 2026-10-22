'use strict';

/**
 * The gRPC boundary of TicketTierApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/ticket-tier-approval');

module.exports = {
  approveTicketTier(call, callback) {
    operations.approveTicketTier(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  commentTicketTierApproval: unary(operations.commentTicketTierApproval),
  delegateTicketTierApproval: unary(operations.delegateTicketTierApproval),
  getTicketTierApprovalStatus(call, callback) {
    operations.getTicketTierApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  recallTicketTier: unary(operations.recallTicketTier),
  rejectTicketTier: unary(operations.rejectTicketTier),
  submitTicketTier(call, callback) {
    operations.submitTicketTier(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
};
