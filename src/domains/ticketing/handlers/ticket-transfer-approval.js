'use strict';

/**
 * The gRPC boundary of TicketTransferApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/ticket-transfer-approval');

module.exports = {
  approveTicketTransfer(call, callback) {
    operations.approveTicketTransfer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  commentTicketTransferApproval: unary(operations.commentTicketTransferApproval),
  delegateTicketTransferApproval: unary(operations.delegateTicketTransferApproval),
  escalateTicketTransfer(call, callback) {
    operations.escalateTicketTransfer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getTicketTransferApprovalStatus: unary(operations.getTicketTransferApprovalStatus),
  listPendingTicketTransfers: unary(operations.listPendingTicketTransfers),
  recallTicketTransfer(call, callback) {
    operations.recallTicketTransfer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  rejectTicketTransfer: unary(operations.rejectTicketTransfer),
  submitTicketTransfer: unary(operations.submitTicketTransfer),
};
