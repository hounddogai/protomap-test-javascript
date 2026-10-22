'use strict';

/**
 * The gRPC boundary of WristbandWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/wristband-workflow');

module.exports = {
  advanceWristbandWorkflow(call, callback) {
    operations.advanceWristbandWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelWristbandWorkflow: unary(operations.cancelWristbandWorkflow),
  inspectWristbandWorkflow: unary(operations.inspectWristbandWorkflow),
  listWristbandWorkflows(call, callback) {
    operations.listWristbandWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  pauseWristbandWorkflow: unary(operations.pauseWristbandWorkflow),
  resumeWristbandWorkflow: unary(operations.resumeWristbandWorkflow),
  retryWristbandWorkflow(call, callback) {
    operations.retryWristbandWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  signalWristbandWorkflow: unary(operations.signalWristbandWorkflow),
  startWristbandWorkflow: unary(operations.startWristbandWorkflow),
};
