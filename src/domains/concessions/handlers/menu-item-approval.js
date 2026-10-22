'use strict';

/**
 * The gRPC boundary of MenuItemApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/menu-item-approval');

module.exports = {
  approveMenuItem(call, callback) {
    operations.approveMenuItem(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  commentMenuItemApproval: unary(operations.commentMenuItemApproval),
  delegateMenuItemApproval: unary(operations.delegateMenuItemApproval),
  escalateMenuItem(call, callback) {
    operations.escalateMenuItem(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  getMenuItemApprovalStatus: unary(operations.getMenuItemApprovalStatus),
  listPendingMenuItems: unary(operations.listPendingMenuItems),
  recallMenuItem(call, callback) {
    operations.recallMenuItem(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  rejectMenuItem: unary(operations.rejectMenuItem),
  submitMenuItem: unary(operations.submitMenuItem),
};
