'use strict';

/**
 * The gRPC boundary of WishlistNotificationService.
 */

const { toServiceError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const operations = require('../operations/wishlist-notification');

module.exports = {
  getWishlistPreferences(call, callback) {
    operations.getWishlistPreferences(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  listWishlistSubscriptions: unary(operations.listWishlistSubscriptions),
  notifyWishlistWatchers: unary(operations.notifyWishlistWatchers),
  sendWishlistTestNotice(call, callback) {
    operations.sendWishlistTestNotice(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  streamWishlistNotices: serverStream(operations.streamWishlistNotices),
  subscribeWishlistUpdates: unary(operations.subscribeWishlistUpdates),
  unsubscribeWishlistUpdates(call, callback) {
    operations.unsubscribeWishlistUpdates(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
};
