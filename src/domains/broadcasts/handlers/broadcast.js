'use strict';

/**
 * The gRPC boundary of BroadcastService.
 */

const { toServiceError } = require('../../../platform/errors');
const { clientStream, serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/broadcast');

module.exports = {
  batchCreateBroadcasts: clientStream(operations.batchCreateBroadcasts, () => ({})),
  batchGetBroadcasts: unary(operations.batchGetBroadcasts),
  cloneBroadcast: unary(operations.cloneBroadcast),
  createBroadcast(call, callback) {
    operations.createBroadcast(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  deleteBroadcast: unary(operations.deleteBroadcast),
  getBroadcast: unary(operations.getBroadcast),
  listBroadcasts(call, callback) {
    operations.listBroadcasts(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  searchBroadcasts: unary(operations.searchBroadcasts),
  updateBroadcast: unary(operations.updateBroadcast),
  watchBroadcasts: serverStream(operations.watchBroadcasts),
};
