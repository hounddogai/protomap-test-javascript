'use strict';

/**
 * The gRPC boundary of ShowApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/show-approval');

module.exports = {
  approveShow(call, callback) {
    operations.approveShow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  delegateShowApproval: unary(operations.delegateShowApproval),
  escalateShow: unary(operations.escalateShow),
  getShowApprovalStatus(call, callback) {
    operations.getShowApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  listPendingShows: unary(operations.listPendingShows),
  recallShow: unary(operations.recallShow),
  rejectShow(call, callback) {
    operations.rejectShow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  submitShow: unary(operations.submitShow),
};
