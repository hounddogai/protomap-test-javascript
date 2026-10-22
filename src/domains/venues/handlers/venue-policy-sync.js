'use strict';

/**
 * The gRPC boundary of VenuePolicySyncService.
 */

const { toServiceError } = require('../../../platform/errors');
const { duplex, unary } = require('../../../platform/handlers');
const operations = require('../operations/venue-policy-sync');

module.exports = {
  listVenuePolicyConflicts(call, callback) {
    operations.listVenuePolicyConflicts(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  pushVenuePolicy: unary(operations.pushVenuePolicy),
  reconcileVenuePolicies: unary(operations.reconcileVenuePolicies),
  resolveVenuePolicyConflict(call, callback) {
    operations.resolveVenuePolicyConflict(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  streamVenuePolicyChanges: duplex(operations.streamVenuePolicyChanges),
  syncVenuePolicies: unary(operations.syncVenuePolicies),
};
