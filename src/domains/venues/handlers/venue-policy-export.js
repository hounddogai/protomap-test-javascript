'use strict';

/**
 * The gRPC boundary of VenuePolicyExportService.
 */

const { toServiceError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/venue-policy-export');

module.exports = {
  cancelVenuePolicyExport(call, callback) {
    operations.cancelVenuePolicyExport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getVenuePolicyExport: unary(operations.getVenuePolicyExport),
  listVenuePolicyExports: unary(operations.listVenuePolicyExports),
  retryVenuePolicyExport(call, callback) {
    operations.retryVenuePolicyExport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  scheduleVenuePolicyExport: unary(operations.scheduleVenuePolicyExport),
  shareVenuePolicyExport: unary(operations.shareVenuePolicyExport),
  streamVenuePolicyExport: serverStream(operations.streamVenuePolicyExport),
};
