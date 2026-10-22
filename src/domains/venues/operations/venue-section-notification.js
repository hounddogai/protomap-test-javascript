'use strict';

/**
 * Tells the people who follow a venue section what changed. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const {
  venuesVenueNotificationClient,
  venuesVenuePolicyExportClient,
  venuesVenueSectionSyncClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Returns a venue section.
 */
async function getVenueSectionPreferences(request) {
  validateContext(request.context);
  if (!request.venue_section_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
  }
  await store.record(request.context.tenant_id, 'GetVenueSectionPreferences', request);
  logger.info('get_venue_section_preferences', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return {
    venue_section: { venue_section_id: (request?.venue_section_id ?? '') },
  };
}

/**
 * Lists venue sections a page at a time.
 */
async function listVenueSectionSubscriptions(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListVenueSectionSubscriptions', request);
  logger.info('list_venue_section_subscriptions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Notifys a venue section.
 */
async function notifyVenueSectionWatchers(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'NotifyVenueSectionWatchers', request);
  logger.info('notify_venue_section_watchers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_section: {
      manager: { email: (request?.details?.recipient?.email ?? '') },
      venue_section_id: (request?.venue_section_id ?? ''),
    },
  };
}

/**
 * Sends a venue section.
 */
async function sendVenueSectionTestNotice(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'SendVenueSectionTestNotice', request);
  logger.info('send_venue_section_test_notice', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      recipient: { phone: (request?.details?.recipient?.phone ?? '') },
      template_id: (request?.details?.template_id ?? ''),
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => venuesVenuePolicyExportClient.shareVenuePolicyExport(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      venue_section: {
        children: [{
          children: [{
            audit: { etag: (reply1?.venue_policy?.audit?.etag ?? '') },
            manager: { user_id: (reply1?.venue_policy?.owner?.user_id ?? '') },
          }],
        }],
      },
    },
    {
      venue_section: {
        manager: { email: (request?.details?.recipient?.email ?? '') },
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    },
  );
}

/**
 * Streams the changes of venue sections as they happen.
 */
async function streamVenueSectionNotices(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'StreamVenueSectionNotices', request);
  logger.info('stream_venue_section_notices', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
  return {
    venue_section: {
      location: { region: (request?.filter?.region ?? '') },
      manager: { email: (request?.subscriber?.email ?? '') },
    },
  };
}

/**
 * Subscribes a venue section.
 */
async function subscribeVenueSectionUpdates(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'SubscribeVenueSectionUpdates', request);
  logger.info('subscribe_venue_section_updates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    venue_section: { manager: { email: (input1?.details?.recipient?.email ?? '') } },
  };
  const reply1 = await call((done) => venuesVenueSectionSyncClient.resolveVenueSectionConflict(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.outcome?.status ?? '') } },
    {
      venue_section: {
        manager: { email: (request?.details?.recipient?.email ?? '') },
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    },
  );
}

/**
 * Unsubscribes a venue section.
 */
async function unsubscribeVenueSectionUpdates(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'UnsubscribeVenueSectionUpdates', request);
  logger.info('unsubscribe_venue_section_updates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      recipient: {
        name: request?.details?.recipient?.name,
        phone: (request?.details?.recipient?.phone ?? ''),
      },
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => venuesVenueNotificationClient.notifyVenueWatchers(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      venue_section: {
        children: [{
          children: [{ audit: { updated_by: (reply1?.venue?.audit?.updated_by ?? '') } }],
        }],
        location: {
          postal_code: ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.postal_code ?? ''),
        },
      },
    },
    {
      venue_section: {
        manager: { email: (request?.details?.recipient?.email ?? '') },
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    },
  );
}

module.exports = { getVenueSectionPreferences, listVenueSectionSubscriptions, notifyVenueSectionWatchers, sendVenueSectionTestNotice, streamVenueSectionNotices, subscribeVenueSectionUpdates, unsubscribeVenueSectionUpdates };
