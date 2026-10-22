'use strict';

/**
 * Tells the people who follow a wishlist what changed. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsGateScanWorkflowClient,
  analyticsAnalyticsEventQueryClient,
  fanportalEventPageNotificationClient,
  fanportalFanSessionQueryClient,
  featureflagsFeatureFlagQueryClient,
  ratelimitingRateLimitQueryClient,
  searchSearchIndexValidationClient,
  toursTourBudgetApprovalClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Returns a wishlist.
 */
async function getWishlistPreferences(request) {
  validateContext(request.context);
  if (!request.wishlist_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wishlist_id is required');
  }
  await store.record(request.context.tenant_id, 'GetWishlistPreferences', request);
  logger.info('get_wishlist_preferences', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { wishlist: { wishlist_id: (request?.wishlist_id ?? '') } };
}

/**
 * Lists wishlists a page at a time.
 */
async function listWishlistSubscriptions(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListWishlistSubscriptions', request);
  logger.info('list_wishlist_subscriptions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { notify: { email: (request?.context?.actor?.email ?? '') } },
    gate_scan: {
      measurements: [{ observed_at: request?.filter?.created_after }],
      source: (request?.filter?.owner_id ?? ''),
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => admissionsGateScanWorkflowClient.advanceGateScanWorkflow(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
    wishlists: [{ lines: [{ attributes: (reply1?.gate_scan?.attributes ?? {}) }] }],
  };
}

/**
 * Notifys a wishlist.
 */
async function notifyWishlistWatchers(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'NotifyWishlistWatchers', request);
  logger.info('notify_wishlist_watchers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      labels: { ...(request?.details?.variables ?? {}) },
      owner_id: (request?.details?.template_id ?? ''),
    },
  };
  const messages1 = await collect(featureflagsFeatureFlagQueryClient.exportFeatureFlags(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    context: input2?.context,
    details: {
      recipient: {
        name: input2?.details?.recipient?.name,
        phone: (input2?.details?.recipient?.phone ?? ''),
      },
    },
  };
  const reply2 = await call((done) => fanportalEventPageNotificationClient.sendEventPageTestNotice(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      wishlist: {
        audit: { created_by: (reply2?.event_page?.audit?.created_by ?? '') },
        billing_address: { recipient: (reply2?.event_page?.billing_address?.recipient ?? '') },
      },
    },
    { wishlist: { wishlist_id: (request?.wishlist_id ?? '') } },
  );
}

/**
 * Sends a wishlist.
 */
async function sendWishlistTestNotice(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'SendWishlistTestNotice', request);
  logger.info('send_wishlist_test_notice', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    subscriber: {
      phone: (request?.details?.recipient?.phone ?? ''),
      preferred_channel: (request?.details?.recipient?.preferred_channel ?? ''),
    },
  };
  const messages1 = await collect(fanportalEventPageNotificationClient.streamEventPageNotices(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      owner_id: (request?.wishlist_id ?? ''),
    },
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => fanportalFanSessionQueryClient.countFanSessions(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (reply2?.result?.correlation_id ?? ''),
    },
  };
  const copied3 = structuredClone(request3);
  const reply3 = await call((done) => toursTourBudgetApprovalClient.listPendingTourBudgets(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { status: (reply2?.result?.status ?? '') },
      result: { status: (reply2?.result?.status ?? '') },
    },
    { result: { reference_ids: (reply3?.result?.reference_ids ?? []) } },
    { wishlist: { wishlist_id: (request?.wishlist_id ?? '') } },
  );
}

/**
 * Streams the changes of wishlists as they happen.
 */
async function streamWishlistNotices(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'StreamWishlistNotices', request);
  logger.info('stream_wishlist_notices', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.subscriber?.email ?? ''),
        name: request?.subscriber?.name,
        phone: (request?.subscriber?.phone ?? ''),
        preferred_channel: (request?.subscriber?.preferred_channel ?? ''),
      },
    },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => fanportalEventPageNotificationClient.unsubscribeEventPageUpdates(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      labels: { ...(request?.filter?.labels ?? {}) },
      region: (reply1?.event_page?.billing_address?.region ?? ''),
    },
  };
  const messages2 = await collect(ratelimitingRateLimitQueryClient.exportRateLimits(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  const request3 = {
    context: request?.context,
    search_index: {
      address: { postal_code: (reply1?.event_page?.billing_address?.postal_code ?? '') },
    },
  };
  const reply3 = await withRetry(() => call((done) => searchSearchIndexValidationClient.checkSearchIndexEligibility(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  const request4 = {
    context: request?.context,
    details: { recipient: { email: (reply3?.search_index?.owner?.email ?? '') } },
  };
  const task4 = deferred(() => call((done) => fanportalEventPageNotificationClient.notifyEventPageWatchers(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  const reply4 = await task4();
  logger.info('local_audit', { value: (request?.filter?.owner_id ?? '') });
  return assemble(
    {
      wishlist: { audit: { created_by: (reply1?.event_page?.audit?.created_by ?? '') } },
    },
    { result: { status: (reply3?.result?.status ?? '') } },
    { wishlist: { metadata: (reply4?.event_page?.metadata ?? {}) } },
    {
      wishlist: { billing_address: { region: (request?.filter?.region ?? '') } },
    },
  );
}

/**
 * Subscribes a wishlist.
 */
async function subscribeWishlistUpdates(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'SubscribeWishlistUpdates', request);
  logger.info('subscribe_wishlist_updates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      recipient: {
        name: request?.details?.recipient?.name,
        preferred_channel: (request?.details?.recipient?.preferred_channel ?? ''),
      },
    },
  };
  const reply1 = await bus.request('acme.fanportal.v1.WishlistNotificationService/SubscribeWishlistUpdates:0', request1);
  const request2 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    reason: (request?.reason ?? ''),
  };
  const reply2 = await runInContext(request.context, () => call((done) => analyticsAnalyticsEventQueryClient.summarizeAnalyticsEvents(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const request3 = {
    context: request?.context,
    rider: {
      audit: {
        created_at: reply1?.event_page?.audit?.created_at,
        etag: (reply1?.event_page?.audit?.etag ?? ''),
      },
      owner: { email: (request?.details?.recipient?.email ?? '') },
    },
  };
  const reply3 = await call((done) => clients.performersRiderApprovalClient.approveRider(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.channel ?? '') });
  return assemble(
    {
      outcome: { status: (reply1?.result?.status ?? '') },
      wishlist: { billing_address: reply1?.event_page?.billing_address },
    },
    { outcome: { message: (reply2?.result?.message ?? '') } },
    {
      wishlist: {
        lines: [{
          tax: {
            amount: (reply3?.rider?.action === 'discount' ? reply3?.rider.discount : undefined)?.amount,
          },
        }],
      },
    },
    { wishlist: { wishlist_id: (request?.wishlist_id ?? '') } },
  );
}

/**
 * Unsubscribes a wishlist.
 */
async function unsubscribeWishlistUpdates(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'UnsubscribeWishlistUpdates', request);
  logger.info('unsubscribe_wishlist_updates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      labels: { ...(request?.details?.variables ?? {}) },
      owner_id: (request?.details?.template_id ?? ''),
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    fanportalEventPageNotificationClient.listEventPageSubscriptions(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.details?.recipient?.email ?? ''),
        preferred_channel: (request?.details?.recipient?.preferred_channel ?? ''),
      },
    },
  };
  const unsubscribeEventPageUpdates2 = promisify(fanportalEventPageNotificationClient.unsubscribeEventPageUpdates.bind(fanportalEventPageNotificationClient));
  const reply2 = await unsubscribeEventPageUpdates2(request2, metadataFor(request2.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        message: (reply1?.result?.message ?? ''),
      },
    },
    {
      result: { message: (reply2?.result?.message ?? '') },
      wishlist: { billing_address: reply2?.event_page?.billing_address },
    },
    { wishlist: { wishlist_id: (request?.wishlist_id ?? '') } },
  );
}

module.exports = { getWishlistPreferences, listWishlistSubscriptions, notifyWishlistWatchers, sendWishlistTestNotice, streamWishlistNotices, subscribeWishlistUpdates, unsubscribeWishlistUpdates };
