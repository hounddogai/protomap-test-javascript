'use strict';

/**
 * The gRPC boundary of ConcessionStandService.
 */

const { toServiceError } = require('../../../platform/errors');
const { clientStream, serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/concession-stand');

module.exports = {
  batchCreateConcessionStands: clientStream(operations.batchCreateConcessionStands, () => ({})),
  batchGetConcessionStands: unary(operations.batchGetConcessionStands),
  cloneConcessionStand: unary(operations.cloneConcessionStand),
  createConcessionStand(call, callback) {
    operations.createConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getConcessionStand: unary(operations.getConcessionStand),
  searchConcessionStands: unary(operations.searchConcessionStands),
  updateConcessionStand(call, callback) {
    operations.updateConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  watchConcessionStands: serverStream(operations.watchConcessionStands),
};
