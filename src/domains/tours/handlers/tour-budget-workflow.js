'use strict';

/**
 * The gRPC boundary of TourBudgetWorkflowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/tour-budget-workflow');

module.exports = {
  advanceTourBudgetWorkflow(call, callback) {
    operations.advanceTourBudgetWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cancelTourBudgetWorkflow: unary(operations.cancelTourBudgetWorkflow),
  inspectTourBudgetWorkflow: unary(operations.inspectTourBudgetWorkflow),
  listTourBudgetWorkflows(call, callback) {
    operations.listTourBudgetWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  pauseTourBudgetWorkflow: unary(operations.pauseTourBudgetWorkflow),
  resumeTourBudgetWorkflow: unary(operations.resumeTourBudgetWorkflow),
  retryTourBudgetWorkflow(call, callback) {
    operations.retryTourBudgetWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  signalTourBudgetWorkflow: unary(operations.signalTourBudgetWorkflow),
  startTourBudgetWorkflow: unary(operations.startTourBudgetWorkflow),
};
