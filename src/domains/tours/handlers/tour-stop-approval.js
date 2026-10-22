'use strict';

/**
 * The gRPC boundary of TourStopApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/tour-stop-approval');

module.exports = {
  approveTourStop(call, callback) {
    operations.approveTourStop(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  delegateTourStopApproval: unary(operations.delegateTourStopApproval),
  escalateTourStop: unary(operations.escalateTourStop),
  getTourStopApprovalStatus(call, callback) {
    operations.getTourStopApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  listPendingTourStops: unary(operations.listPendingTourStops),
  recallTourStop: unary(operations.recallTourStop),
  rejectTourStop(call, callback) {
    operations.rejectTourStop(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  submitTourStop: unary(operations.submitTourStop),
};
