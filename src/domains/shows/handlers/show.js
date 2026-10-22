'use strict';

/**
 * The gRPC boundary of ShowService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/show');

module.exports = {
  batchGetShows(call, callback) {
    operations.batchGetShows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  cloneShow: unary(operations.cloneShow),
  createShow: unary(operations.createShow),
  deleteShow(call, callback) {
    operations.deleteShow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getShow: unary(operations.getShow),
  listShows: unary(operations.listShows),
  searchShows(call, callback) {
    operations.searchShows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
};
