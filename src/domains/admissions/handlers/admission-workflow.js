'use strict';

/**
 * The gRPC boundary of AdmissionWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/admission-workflow');

module.exports = {
  advanceAdmissionWorkflow(call, callback) {
    operations.advanceAdmissionWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelAdmissionWorkflow: unary(operations.cancelAdmissionWorkflow),
  inspectAdmissionWorkflow: unary(operations.inspectAdmissionWorkflow),
  listAdmissionWorkflows(call, callback) {
    operations.listAdmissionWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  pauseAdmissionWorkflow: unary(operations.pauseAdmissionWorkflow),
  resumeAdmissionWorkflow: unary(operations.resumeAdmissionWorkflow),
  retryAdmissionWorkflow(call, callback) {
    operations.retryAdmissionWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  signalAdmissionWorkflow: unary(operations.signalAdmissionWorkflow),
  startAdmissionWorkflow: unary(operations.startAdmissionWorkflow),
};
