'use strict';

/**
 * The gRPC boundary of GuestListLifecycleService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/guest-list-lifecycle');

module.exports = {
  activateGuestList(call, callback) {
    operations.activateGuestList(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  expireGuestList: unary(operations.expireGuestList),
  getGuestListLifecycle: unary(operations.getGuestListLifecycle),
  reinstateGuestList(call, callback) {
    operations.reinstateGuestList(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  renewGuestList: unary(operations.renewGuestList),
  scheduleGuestListExpiry: unary(operations.scheduleGuestListExpiry),
  suspendGuestList(call, callback) {
    operations.suspendGuestList(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  terminateGuestList: unary(operations.terminateGuestList),
};
