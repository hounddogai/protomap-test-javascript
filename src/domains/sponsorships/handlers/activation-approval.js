'use strict';

/**
 * The gRPC boundary of ActivationApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/activation-approval');

module.exports = {
  approveActivation(call, callback) {
    operations.approveActivation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  commentActivationApproval: unary(operations.commentActivationApproval),
  delegateActivationApproval: unary(operations.delegateActivationApproval),
  escalateActivation(call, callback) {
    operations.escalateActivation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getActivationApprovalStatus: unary(operations.getActivationApprovalStatus),
  listPendingActivations: unary(operations.listPendingActivations),
  recallActivation(call, callback) {
    operations.recallActivation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  rejectActivation: unary(operations.rejectActivation),
  submitActivation: unary(operations.submitActivation),
};
