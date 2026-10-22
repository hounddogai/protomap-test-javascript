'use strict';

/**
 * The gRPC boundary of GateScanLifecycleService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/gate-scan-lifecycle');

module.exports = {
  activateGateScan(call, callback) {
    operations.activateGateScan(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  expireGateScan: unary(operations.expireGateScan),
  getGateScanLifecycle: unary(operations.getGateScanLifecycle),
  reinstateGateScan(call, callback) {
    operations.reinstateGateScan(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  renewGateScan: unary(operations.renewGateScan),
  scheduleGateScanExpiry: unary(operations.scheduleGateScanExpiry),
  suspendGateScan(call, callback) {
    operations.suspendGateScan(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
};
