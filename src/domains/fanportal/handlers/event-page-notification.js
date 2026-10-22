'use strict';

/**
 * The gRPC boundary of EventPageNotificationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/event-page-notification');

module.exports = {
  getEventPagePreferences(call, callback) {
    operations.getEventPagePreferences(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  listEventPageSubscriptions: unary(operations.listEventPageSubscriptions),
  notifyEventPageWatchers: unary(operations.notifyEventPageWatchers),
  sendEventPageTestNotice(call, callback) {
    operations.sendEventPageTestNotice(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  streamEventPageNotices: serverStream(operations.streamEventPageNotices),
  subscribeEventPageUpdates: unary(operations.subscribeEventPageUpdates),
  unsubscribeEventPageUpdates(call, callback) {
    operations.unsubscribeEventPageUpdates(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
};
