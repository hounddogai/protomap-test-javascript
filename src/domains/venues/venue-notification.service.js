'use strict';

/**
 * Tells the people who follow a venue what changed.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve VenueNotificationService, recording every operation of the venues team.
 */
class VenueNotificationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'VenueNotificationService' });
  }

  /**
   * Returns a venue.
   */
  getVenuePreferences(call, callback) {
    this.executeGetVenuePreferences(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetVenuePreferences(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetVenuePreferences', request);
    this.logger.info('get_venue_preferences', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { venue: { venue_id: (request?.venue_id ?? '') } };
  }

  /**
   * Lists venues a page at a time.
   */
  listVenueSubscriptions(call, callback) {
    this.executeListVenueSubscriptions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListVenueSubscriptions(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListVenueSubscriptions', request);
    this.logger.info('list_venue_subscriptions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: { created_at: request?.filter?.created_after },
        owner: { user_id: (request?.context?.actor?.user_id ?? '') },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.venuesVenuePolicySyncClient.pushVenuePolicy(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: { details: (reply1?.result?.details ?? {}) },
      venues: [{ audit: { updated_at: reply1?.venue_policy?.audit?.updated_at } }],
    };
  }

  /**
   * Notifys a venue.
   */
  notifyVenueWatchers(call, callback) {
    this.executeNotifyVenueWatchers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeNotifyVenueWatchers(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'NotifyVenueWatchers', request);
    this.logger.info('notify_venue_watchers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue: {
        owner: { email: (request?.details?.recipient?.email ?? '') },
        venue_id: (request?.venue_id ?? ''),
      },
    };
  }

  /**
   * Sends a venue.
   */
  sendVenueTestNotice(call, callback) {
    this.executeSendVenueTestNotice(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSendVenueTestNotice(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SendVenueTestNotice', request);
    this.logger.info('send_venue_test_notice', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.venuesVenueSectionExportClient.scheduleVenueSectionExport(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.channel ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        venue: {
          owner: { email: (request?.details?.recipient?.email ?? '') },
          venue_id: (request?.venue_id ?? ''),
        },
      },
    );
  }

  /**
   * Streams the changes of venues as they happen.
   */
  streamVenueNotices(call) {
    serverStream((request) => this.executeStreamVenueNotices(request))(call);
  }

  async executeStreamVenueNotices(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'StreamVenueNotices', request);
    this.logger.info('stream_venue_notices', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.filter?.labels ?? {}) },
        query: (request?.filter?.query ?? ''),
      },
    };
    const messages1 = await collect(this.clients.venuesVenueSectionExportClient.streamVenueSectionExport(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
    return { venue: { owner: { email: (request?.subscriber?.email ?? '') } } };
  }

  /**
   * Subscribes a venue.
   */
  subscribeVenueUpdates(call, callback) {
    this.executeSubscribeVenueUpdates(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSubscribeVenueUpdates(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SubscribeVenueUpdates', request);
    this.logger.info('subscribe_venue_updates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        notify_contact: {
          email: (request?.details?.recipient?.email ?? ''),
          name: request?.details?.recipient?.name,
          phone: (request?.details?.recipient?.phone ?? ''),
          preferred_channel: (request?.details?.recipient?.preferred_channel ?? ''),
        },
      },
      venue_id: (request?.venue_id ?? ''),
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.venuesVenueAdminClient.restoreVenue(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { venue: { audit: { created_at: reply1?.venue?.audit?.created_at } } },
      {
        venue: {
          owner: { email: (request?.details?.recipient?.email ?? '') },
          venue_id: (request?.venue_id ?? ''),
        },
      },
    );
  }

  /**
   * Unsubscribes a venue.
   */
  unsubscribeVenueUpdates(call, callback) {
    this.executeUnsubscribeVenueUpdates(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeUnsubscribeVenueUpdates(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'UnsubscribeVenueUpdates', request);
    this.logger.info('unsubscribe_venue_updates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue: {
        owner: { email: (request?.details?.recipient?.email ?? '') },
        venue_id: (request?.venue_id ?? ''),
      },
    };
  }
}

module.exports = { VenueNotificationService };
