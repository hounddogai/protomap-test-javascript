'use strict';

/**
 * The gRPC boundary of MenuItemReportingService.
 */

const { toServiceError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/menu-item-reporting');

module.exports = {
  countMenuItemTotals(call, callback) {
    operations.countMenuItemTotals(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  fetchMenuItemReport: unary(operations.fetchMenuItemReport),
  generateMenuItemReport: unary(operations.generateMenuItemReport),
  listMenuItemReports(call, callback) {
    operations.listMenuItemReports(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  scheduleMenuItemReport: unary(operations.scheduleMenuItemReport),
  shareMenuItemReport: unary(operations.shareMenuItemReport),
  streamMenuItemMetrics: serverStream(operations.streamMenuItemMetrics),
};
