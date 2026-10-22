'use strict';

/**
 * The gRPC boundary of VenueSectionAdminService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/venue-section-admin');

module.exports = {
  archiveVenueSection(call, callback) {
    operations.archiveVenueSection(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  lockVenueSection: unary(operations.lockVenueSection),
  purgeVenueSection: unary(operations.purgeVenueSection),
  reassignVenueSection(call, callback) {
    operations.reassignVenueSection(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  restoreVenueSection: unary(operations.restoreVenueSection),
  transferVenueSection: unary(operations.transferVenueSection),
};
