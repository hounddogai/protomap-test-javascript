'use strict';

/**
 * The gRPC boundary of StreamKeyWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/stream-key-workflow');

module.exports = {
  advanceStreamKeyWorkflow(call, callback) {
    operations.advanceStreamKeyWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelStreamKeyWorkflow: unary(operations.cancelStreamKeyWorkflow),
  inspectStreamKeyWorkflow: unary(operations.inspectStreamKeyWorkflow),
  listStreamKeyWorkflows(call, callback) {
    operations.listStreamKeyWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  pauseStreamKeyWorkflow: unary(operations.pauseStreamKeyWorkflow),
  resumeStreamKeyWorkflow: unary(operations.resumeStreamKeyWorkflow),
  retryStreamKeyWorkflow(call, callback) {
    operations.retryStreamKeyWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  signalStreamKeyWorkflow: unary(operations.signalStreamKeyWorkflow),
  startStreamKeyWorkflow: unary(operations.startStreamKeyWorkflow),
};
