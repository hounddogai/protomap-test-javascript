'use strict';

/**
 * The gRPC boundary of PerformerContractApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/performer-contract-approval');

module.exports = {
  approvePerformerContract(call, callback) {
    operations.approvePerformerContract(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  commentPerformerContractApproval: unary(operations.commentPerformerContractApproval),
  escalatePerformerContract: unary(operations.escalatePerformerContract),
  listPendingPerformerContracts(call, callback) {
    operations.listPendingPerformerContracts(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  recallPerformerContract: unary(operations.recallPerformerContract),
  rejectPerformerContract: unary(operations.rejectPerformerContract),
  submitPerformerContract(call, callback) {
    operations.submitPerformerContract(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
};
