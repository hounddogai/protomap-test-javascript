'use strict';

/**
 * The gRPC boundary of CashDrawerWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/cash-drawer-workflow');

module.exports = {
  advanceCashDrawerWorkflow(call, callback) {
    operations.advanceCashDrawerWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelCashDrawerWorkflow: unary(operations.cancelCashDrawerWorkflow),
  inspectCashDrawerWorkflow: unary(operations.inspectCashDrawerWorkflow),
  listCashDrawerWorkflows(call, callback) {
    operations.listCashDrawerWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  pauseCashDrawerWorkflow: unary(operations.pauseCashDrawerWorkflow),
  resumeCashDrawerWorkflow: unary(operations.resumeCashDrawerWorkflow),
  retryCashDrawerWorkflow(call, callback) {
    operations.retryCashDrawerWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  signalCashDrawerWorkflow: unary(operations.signalCashDrawerWorkflow),
  startCashDrawerWorkflow: unary(operations.startCashDrawerWorkflow),
};
