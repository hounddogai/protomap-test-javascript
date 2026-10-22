'use strict';

/**
 * The gRPC boundary of SeatMapLifecycleService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/seat-map-lifecycle');

module.exports = {
  activateSeatMap(call, callback) {
    operations.activateSeatMap(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  expireSeatMap: unary(operations.expireSeatMap),
  getSeatMapLifecycle: unary(operations.getSeatMapLifecycle),
  reinstateSeatMap(call, callback) {
    operations.reinstateSeatMap(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  renewSeatMap: unary(operations.renewSeatMap),
  scheduleSeatMapExpiry: unary(operations.scheduleSeatMapExpiry),
  suspendSeatMap(call, callback) {
    operations.suspendSeatMap(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  terminateSeatMap: unary(operations.terminateSeatMap),
};
