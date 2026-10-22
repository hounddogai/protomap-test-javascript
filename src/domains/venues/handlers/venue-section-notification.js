'use strict';

/**
 * The gRPC boundary of VenueSectionNotificationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/venue-section-notification');

module.exports = {
  getVenueSectionPreferences(call, callback) {
    operations.getVenueSectionPreferences(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  listVenueSectionSubscriptions: unary(operations.listVenueSectionSubscriptions),
  notifyVenueSectionWatchers: unary(operations.notifyVenueSectionWatchers),
  sendVenueSectionTestNotice(call, callback) {
    operations.sendVenueSectionTestNotice(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  streamVenueSectionNotices: serverStream(operations.streamVenueSectionNotices),
  subscribeVenueSectionUpdates: unary(operations.subscribeVenueSectionUpdates),
  unsubscribeVenueSectionUpdates(call, callback) {
    operations.unsubscribeVenueSectionUpdates(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
};
