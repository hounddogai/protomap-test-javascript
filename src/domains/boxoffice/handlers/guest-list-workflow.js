'use strict';

/**
 * The gRPC boundary of GuestListWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/guest-list-workflow');

module.exports = {
  advanceGuestListWorkflow(call, callback) {
    operations.advanceGuestListWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelGuestListWorkflow: unary(operations.cancelGuestListWorkflow),
  inspectGuestListWorkflow: unary(operations.inspectGuestListWorkflow),
  pauseGuestListWorkflow(call, callback) {
    operations.pauseGuestListWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  retryGuestListWorkflow: unary(operations.retryGuestListWorkflow),
  startGuestListWorkflow: unary(operations.startGuestListWorkflow),
};
