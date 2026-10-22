'use strict';

/**
 * The gRPC boundary of VenueHistoryService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/venue-history');

module.exports = {
  annotateVenueVersion(call, callback) {
    operations.annotateVenueVersion(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  compareVenueVersions: unary(operations.compareVenueVersions),
  getVenueVersion: unary(operations.getVenueVersion),
  listVenueVersions(call, callback) {
    operations.listVenueVersions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  recordVenueVersion: unary(operations.recordVenueVersion),
  revertVenue: unary(operations.revertVenue),
};
