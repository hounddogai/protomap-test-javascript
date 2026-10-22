'use strict';

/**
 * The gRPC boundary of BoxOfficeSaleWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/box-office-sale-workflow');

module.exports = {
  advanceBoxOfficeSaleWorkflow(call, callback) {
    operations.advanceBoxOfficeSaleWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelBoxOfficeSaleWorkflow: unary(operations.cancelBoxOfficeSaleWorkflow),
  inspectBoxOfficeSaleWorkflow: unary(operations.inspectBoxOfficeSaleWorkflow),
  listBoxOfficeSaleWorkflows(call, callback) {
    operations.listBoxOfficeSaleWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  pauseBoxOfficeSaleWorkflow: unary(operations.pauseBoxOfficeSaleWorkflow),
  resumeBoxOfficeSaleWorkflow: unary(operations.resumeBoxOfficeSaleWorkflow),
  retryBoxOfficeSaleWorkflow(call, callback) {
    operations.retryBoxOfficeSaleWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  signalBoxOfficeSaleWorkflow: unary(operations.signalBoxOfficeSaleWorkflow),
  startBoxOfficeSaleWorkflow: unary(operations.startBoxOfficeSaleWorkflow),
};
