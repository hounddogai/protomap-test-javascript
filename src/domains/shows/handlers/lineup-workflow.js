'use strict';

/**
 * The gRPC boundary of LineupWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/lineup-workflow');

module.exports = {
  advanceLineupWorkflow(call, callback) {
    operations.advanceLineupWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelLineupWorkflow: unary(operations.cancelLineupWorkflow),
  inspectLineupWorkflow: unary(operations.inspectLineupWorkflow),
  listLineupWorkflows(call, callback) {
    operations.listLineupWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  pauseLineupWorkflow: unary(operations.pauseLineupWorkflow),
  resumeLineupWorkflow: unary(operations.resumeLineupWorkflow),
  retryLineupWorkflow(call, callback) {
    operations.retryLineupWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  signalLineupWorkflow: unary(operations.signalLineupWorkflow),
  startLineupWorkflow: unary(operations.startLineupWorkflow),
};
