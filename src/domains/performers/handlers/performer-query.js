'use strict';

/**
 * The gRPC boundary of PerformerQueryService.
 */

const { toServiceError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/performer-query');

module.exports = {
  browsePerformers(call, callback) {
    operations.browsePerformers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  countPerformers: unary(operations.countPerformers),
  exportPerformers: serverStream(operations.exportPerformers),
  findPerformers(call, callback) {
    operations.findPerformers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  lookupPerformer: unary(operations.lookupPerformer),
  peekPerformer: unary(operations.peekPerformer),
  suggestPerformers(call, callback) {
    operations.suggestPerformers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  summarizePerformers: unary(operations.summarizePerformers),
};
