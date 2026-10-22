'use strict';

/**
 * The gRPC boundary of WristbandLifecycleService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/wristband-lifecycle');

module.exports = {
  activateWristband(call, callback) {
    operations.activateWristband(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  expireWristband: unary(operations.expireWristband),
  getWristbandLifecycle: unary(operations.getWristbandLifecycle),
  reinstateWristband(call, callback) {
    operations.reinstateWristband(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  renewWristband: unary(operations.renewWristband),
  scheduleWristbandExpiry: unary(operations.scheduleWristbandExpiry),
  suspendWristband(call, callback) {
    operations.suspendWristband(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  terminateWristband: unary(operations.terminateWristband),
};
