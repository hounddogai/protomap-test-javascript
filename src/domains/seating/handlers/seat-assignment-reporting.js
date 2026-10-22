'use strict';

/**
 * The gRPC boundary of SeatAssignmentReportingService.
 */

const { toServiceError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/seat-assignment-reporting');

module.exports = {
  countSeatAssignmentTotals(call, callback) {
    operations.countSeatAssignmentTotals(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  deleteSeatAssignmentReport: unary(operations.deleteSeatAssignmentReport),
  fetchSeatAssignmentReport: unary(operations.fetchSeatAssignmentReport),
  generateSeatAssignmentReport(call, callback) {
    operations.generateSeatAssignmentReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  listSeatAssignmentReports: unary(operations.listSeatAssignmentReports),
  scheduleSeatAssignmentReport: unary(operations.scheduleSeatAssignmentReport),
  shareSeatAssignmentReport(call, callback) {
    operations.shareSeatAssignmentReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  streamSeatAssignmentMetrics: serverStream(operations.streamSeatAssignmentMetrics),
};
