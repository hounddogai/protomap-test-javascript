'use strict';

/**
 * The gRPC boundary of GateScanWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/gate-scan-workflow');

module.exports = {
  advanceGateScanWorkflow(call, callback) {
    operations.advanceGateScanWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  inspectGateScanWorkflow: unary(operations.inspectGateScanWorkflow),
  listGateScanWorkflows: unary(operations.listGateScanWorkflows),
  pauseGateScanWorkflow(call, callback) {
    operations.pauseGateScanWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  resumeGateScanWorkflow: unary(operations.resumeGateScanWorkflow),
  retryGateScanWorkflow: unary(operations.retryGateScanWorkflow),
  signalGateScanWorkflow(call, callback) {
    operations.signalGateScanWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  startGateScanWorkflow: unary(operations.startGateScanWorkflow),
};
