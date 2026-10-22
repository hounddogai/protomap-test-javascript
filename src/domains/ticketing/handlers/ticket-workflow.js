'use strict';

/**
 * The gRPC boundary of TicketWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/ticket-workflow');

module.exports = {
  advanceTicketWorkflow(call, callback) {
    operations.advanceTicketWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelTicketWorkflow: unary(operations.cancelTicketWorkflow),
  inspectTicketWorkflow: unary(operations.inspectTicketWorkflow),
  listTicketWorkflows(call, callback) {
    operations.listTicketWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  pauseTicketWorkflow: unary(operations.pauseTicketWorkflow),
  resumeTicketWorkflow: unary(operations.resumeTicketWorkflow),
  retryTicketWorkflow(call, callback) {
    operations.retryTicketWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  signalTicketWorkflow: unary(operations.signalTicketWorkflow),
  startTicketWorkflow: unary(operations.startTicketWorkflow),
};
