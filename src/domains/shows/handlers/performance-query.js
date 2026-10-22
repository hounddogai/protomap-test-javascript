'use strict';

/**
 * The gRPC boundary of PerformanceQueryService.
 */

const { toServiceError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/performance-query');

module.exports = {
  browsePerformances(call, callback) {
    operations.browsePerformances(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  exportPerformances: serverStream(operations.exportPerformances),
  findPerformances: unary(operations.findPerformances),
  lookupPerformance(call, callback) {
    operations.lookupPerformance(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  peekPerformance: unary(operations.peekPerformance),
  resolvePerformances: unary(operations.resolvePerformances),
};
