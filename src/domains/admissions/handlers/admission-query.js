'use strict';

/**
 * The gRPC boundary of AdmissionQueryService.
 */

const { toServiceError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/admission-query');

module.exports = {
  browseAdmissions(call, callback) {
    operations.browseAdmissions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  countAdmissions: unary(operations.countAdmissions),
  exportAdmissions: serverStream(operations.exportAdmissions),
  lookupAdmission(call, callback) {
    operations.lookupAdmission(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  peekAdmission: unary(operations.peekAdmission),
  resolveAdmissions: unary(operations.resolveAdmissions),
  suggestAdmissions(call, callback) {
    operations.suggestAdmissions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  summarizeAdmissions: unary(operations.summarizeAdmissions),
};
