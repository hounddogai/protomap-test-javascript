'use strict';

/**
 * The gRPC boundary of ReplayClipApprovalService.
 */

const { toServiceError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const operations = require('../operations/replay-clip-approval');

module.exports = {
  approveReplayClip(call, callback) {
    operations.approveReplayClip(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  commentReplayClipApproval: unary(operations.commentReplayClipApproval),
  delegateReplayClipApproval: unary(operations.delegateReplayClipApproval),
  escalateReplayClip(call, callback) {
    operations.escalateReplayClip(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  },
  rejectReplayClip: unary(operations.rejectReplayClip),
  submitReplayClip: unary(operations.submitReplayClip),
};
