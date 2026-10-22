'use strict';

/**
 * The gRPC boundary of BoxOfficeSaleQueryService.
 */

const { toServiceError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/box-office-sale-query');

module.exports = {
  browseBoxOfficeSales(call, callback) {
    operations.browseBoxOfficeSales(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  countBoxOfficeSales: unary(operations.countBoxOfficeSales),
  exportBoxOfficeSales: serverStream(operations.exportBoxOfficeSales),
  findBoxOfficeSales(call, callback) {
    operations.findBoxOfficeSales(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  lookupBoxOfficeSale: unary(operations.lookupBoxOfficeSale),
  peekBoxOfficeSale: unary(operations.peekBoxOfficeSale),
  resolveBoxOfficeSales(call, callback) {
    operations.resolveBoxOfficeSales(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  suggestBoxOfficeSales: unary(operations.suggestBoxOfficeSales),
  summarizeBoxOfficeSales: unary(operations.summarizeBoxOfficeSales),
};
