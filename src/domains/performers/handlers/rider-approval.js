'use strict';

/**
 * The gRPC boundary of RiderApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/rider-approval');

module.exports = {
  approveRider(call, callback) {
    operations.approveRider(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  commentRiderApproval: unary(operations.commentRiderApproval),
  delegateRiderApproval: unary(operations.delegateRiderApproval),
  escalateRider(call, callback) {
    operations.escalateRider(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getRiderApprovalStatus: unary(operations.getRiderApprovalStatus),
  listPendingRiders: unary(operations.listPendingRiders),
  recallRider(call, callback) {
    operations.recallRider(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  submitRider: unary(operations.submitRider),
};
