'use strict';

/**
 * The gRPC boundary of FanSessionGatewayService.
 */

const { toServiceError } = require('../../../platform/errors');
const { clientStream, serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/fan-session-gateway');

module.exports = {
  composeFanSessionPage(call, callback) {
    operations.composeFanSessionPage(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  fetchFanSessionView: unary(operations.fetchFanSessionView),
  getFanSessionSummary: unary(operations.getFanSessionSummary),
  refreshFanSession(call, callback) {
    operations.refreshFanSession(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  searchFanSessionCatalog: unary(operations.searchFanSessionCatalog),
  startFanSessionCheckout: unary(operations.startFanSessionCheckout),
  streamFanSessionUpdates: serverStream(operations.streamFanSessionUpdates),
  submitFanSessionForm: unary(operations.submitFanSessionForm),
  trackFanSession: unary(operations.trackFanSession),
  uploadFanSessionAttachments: clientStream(operations.uploadFanSessionAttachments, () => ({})),
};
