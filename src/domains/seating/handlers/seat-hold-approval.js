'use strict';

/**
 * The gRPC boundary of SeatHoldApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/seat-hold-approval');

module.exports = {
  approveSeatHold(call, callback) {
    operations.approveSeatHold(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  commentSeatHoldApproval: unary(operations.commentSeatHoldApproval),
  delegateSeatHoldApproval: unary(operations.delegateSeatHoldApproval),
  escalateSeatHold(call, callback) {
    operations.escalateSeatHold(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getSeatHoldApprovalStatus: unary(operations.getSeatHoldApprovalStatus),
  listPendingSeatHolds: unary(operations.listPendingSeatHolds),
  recallSeatHold(call, callback) {
    operations.recallSeatHold(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  rejectSeatHold: unary(operations.rejectSeatHold),
  submitSeatHold: unary(operations.submitSeatHold),
};
