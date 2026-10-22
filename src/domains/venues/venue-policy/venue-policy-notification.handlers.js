'use strict';

/**
 * Tells the people who follow a venue policy what changed.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const { assemble } = require('../../../platform/rpc');

/**
 * The handlers of VenuePolicyNotificationService, over the dependencies the venues team is given.
 */
function createVenuePolicyNotificationHandlers({ store, clients, bus, logger }) {
  /**
   * Returns a venue policy.
   */
  async function getVenuePolicyPreferences(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await store.record(request.context.tenant_id, 'GetVenuePolicyPreferences', request);
    logger.info('get_venue_policy_preferences', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { venue_policy: { venue_policy_id: (request?.venue_policy_id ?? '') } };
  }

  /**
   * Notifys a venue policy.
   */
  async function notifyVenuePolicyWatchers(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'NotifyVenuePolicyWatchers', request);
    logger.info('notify_venue_policy_watchers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.details?.variables ?? {}) },
        owner_id: (request?.details?.template_id ?? ''),
      },
    };
    const reply1 = await bus.request('acme.venues.v1.VenuePolicyNotificationService/NotifyVenuePolicyWatchers:0', request1);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        venue_policy: {
          owner: { email: (request?.details?.recipient?.email ?? '') },
          venue_policy_id: (request?.venue_policy_id ?? ''),
        },
      },
    );
  }

  /**
   * Sends a venue policy.
   */
  async function sendVenuePolicyTestNotice(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'SendVenuePolicyTestNotice', request);
    logger.info('send_venue_policy_test_notice', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        owner: { email: (request?.details?.recipient?.email ?? '') },
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }

  /**
   * Streams the changes of venue policies as they happen.
   */
  async function streamVenuePolicyNotices(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'StreamVenuePolicyNotices', request);
    logger.info('stream_venue_policy_notices', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
    return {
      venue_policy: { owner: { email: (request?.subscriber?.email ?? '') } },
    };
  }

  /**
   * Subscribes a venue policy.
   */
  async function subscribeVenuePolicyUpdates(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'SubscribeVenuePolicyUpdates', request);
    logger.info('subscribe_venue_policy_updates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        owner: { email: (request?.details?.recipient?.email ?? '') },
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }

  /**
   * Unsubscribes a venue policy.
   */
  async function unsubscribeVenuePolicyUpdates(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'UnsubscribeVenuePolicyUpdates', request);
    logger.info('unsubscribe_venue_policy_updates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        owner: { email: (request?.details?.recipient?.email ?? '') },
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }

  return {
    getVenuePolicyPreferences: unary(getVenuePolicyPreferences),
    notifyVenuePolicyWatchers: unary(notifyVenuePolicyWatchers),
    sendVenuePolicyTestNotice: unary(sendVenuePolicyTestNotice),
    streamVenuePolicyNotices: serverStream(streamVenuePolicyNotices),
    subscribeVenuePolicyUpdates: unary(subscribeVenuePolicyUpdates),
    unsubscribeVenuePolicyUpdates: unary(unsubscribeVenuePolicyUpdates),
  };
}

module.exports = { createVenuePolicyNotificationHandlers };
