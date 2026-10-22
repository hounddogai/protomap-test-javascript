'use strict';

/**
 * The gRPC boundary of ConcessionOrderLifecycleService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/concession-order-lifecycle');

module.exports = {
  activateConcessionOrder(call, callback) {
    operations.activateConcessionOrder(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  expireConcessionOrder: unary(operations.expireConcessionOrder),
  getConcessionOrderLifecycle: unary(operations.getConcessionOrderLifecycle),
  reinstateConcessionOrder(call, callback) {
    operations.reinstateConcessionOrder(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  renewConcessionOrder: unary(operations.renewConcessionOrder),
  scheduleConcessionOrderExpiry: unary(operations.scheduleConcessionOrderExpiry),
  suspendConcessionOrder(call, callback) {
    operations.suspendConcessionOrder(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  terminateConcessionOrder: unary(operations.terminateConcessionOrder),
};
