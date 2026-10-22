'use strict';

/**
 * The gRPC boundary of SeatAssignmentApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/seat-assignment-approval');

module.exports = {
  approveSeatAssignment(call, callback) {
    operations.approveSeatAssignment(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  escalateSeatAssignment: unary(operations.escalateSeatAssignment),
  getSeatAssignmentApprovalStatus: unary(operations.getSeatAssignmentApprovalStatus),
  listPendingSeatAssignments(call, callback) {
    operations.listPendingSeatAssignments(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  recallSeatAssignment: unary(operations.recallSeatAssignment),
  rejectSeatAssignment: unary(operations.rejectSeatAssignment),
  submitSeatAssignment(call, callback) {
    operations.submitSeatAssignment(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
};
