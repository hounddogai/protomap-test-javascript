'use strict';

/**
 * The gRPC boundary of TourWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/tour-workflow');

module.exports = {
  advanceTourWorkflow(call, callback) {
    operations.advanceTourWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelTourWorkflow: unary(operations.cancelTourWorkflow),
  inspectTourWorkflow: unary(operations.inspectTourWorkflow),
  pauseTourWorkflow(call, callback) {
    operations.pauseTourWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  resumeTourWorkflow: unary(operations.resumeTourWorkflow),
  signalTourWorkflow: unary(operations.signalTourWorkflow),
  startTourWorkflow(call, callback) {
    operations.startTourWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
};
