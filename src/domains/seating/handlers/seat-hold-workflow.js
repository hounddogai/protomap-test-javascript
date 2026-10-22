'use strict';

/**
 * The gRPC boundary of SeatHoldWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/seat-hold-workflow');

module.exports = {
  advanceSeatHoldWorkflow(call, callback) {
    operations.advanceSeatHoldWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelSeatHoldWorkflow: unary(operations.cancelSeatHoldWorkflow),
  listSeatHoldWorkflows: unary(operations.listSeatHoldWorkflows),
  resumeSeatHoldWorkflow(call, callback) {
    operations.resumeSeatHoldWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  signalSeatHoldWorkflow: unary(operations.signalSeatHoldWorkflow),
  startSeatHoldWorkflow: unary(operations.startSeatHoldWorkflow),
};
