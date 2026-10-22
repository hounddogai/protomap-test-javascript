'use strict';

/**
 * The gRPC boundary of CashDrawerLifecycleService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/cash-drawer-lifecycle');

module.exports = {
  activateCashDrawer(call, callback) {
    operations.activateCashDrawer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  expireCashDrawer: unary(operations.expireCashDrawer),
  getCashDrawerLifecycle: unary(operations.getCashDrawerLifecycle),
  reinstateCashDrawer(call, callback) {
    operations.reinstateCashDrawer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  renewCashDrawer: unary(operations.renewCashDrawer),
  scheduleCashDrawerExpiry: unary(operations.scheduleCashDrawerExpiry),
  suspendCashDrawer(call, callback) {
    operations.suspendCashDrawer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  terminateCashDrawer: unary(operations.terminateCashDrawer),
};
