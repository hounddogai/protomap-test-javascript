'use strict';

/**
 * The gRPC boundary of AdmissionApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/admission-approval');

module.exports = {
  approveAdmission(call, callback) {
    operations.approveAdmission(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  commentAdmissionApproval: unary(operations.commentAdmissionApproval),
  delegateAdmissionApproval: unary(operations.delegateAdmissionApproval),
  escalateAdmission(call, callback) {
    operations.escalateAdmission(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getAdmissionApprovalStatus: unary(operations.getAdmissionApprovalStatus),
  listPendingAdmissions: unary(operations.listPendingAdmissions),
  recallAdmission(call, callback) {
    operations.recallAdmission(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  rejectAdmission: unary(operations.rejectAdmission),
  submitAdmission: unary(operations.submitAdmission),
};
