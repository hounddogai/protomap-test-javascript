'use strict';

/**
 * Manages fan sessions: creates, reads, changes, and lists them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { clientStream, serverStream, unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect, exchange, upload } = require('../../../platform/streams');

/**
 * The handlers of FanSessionService, over the dependencies the fanportal team is given.
 */
function createFanSessionHandlers({ store, clients, bus, logger }) {
  /**
   * Takes a stream of fan sessions and answers once at the end.
   */
  async function batchCreateFanSessions(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'BatchCreateFanSessions', request);
    logger.info('batch_create_fan_sessions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      details: {
        recipient: {
          preferred_channel: ((input1?.chunk === 'header' ? input1.header : undefined)?.submitted_by?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await call((done) => clients.fanportalWishlistNotificationClient.subscribeWishlistUpdates(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    let selected2_1;
    switch (request?.chunk) {
      case 'record':
        selected2_1 = ((request?.chunk === 'record' ? request.record : undefined)?.requester?.email ?? '');
        break;
      default:
        selected2_1 = '';
    }
    let selected2_2;
    switch (request?.chunk) {
      case 'record':
        selected2_2 = ((request?.chunk === 'record' ? request.record : undefined)?.requester?.phone ?? '');
        break;
      default:
        selected2_2 = '';
    }
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          email: selected2_1,
          phone: selected2_2,
        },
      },
    };
    const reply2 = await call((done) => clients.auditAuditEventValidationClient.previewAuditEvent(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const results3 = await Promise.all(((request?.chunk === 'record' ? request.record : undefined)?.comments ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        filter: {
          created_after: item?.created_at,
          region: (reply1?.wishlist?.billing_address?.region ?? ''),
        },
      };
      const reply3 = await call((done) => clients.telematicsTelemetryReadingQueryClient.findTelemetryReadings(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    const request4 = {
      context: request?.context,
      subscriber: {
        email: ((request?.chunk === 'header' ? request.header : undefined)?.submitted_by?.email ?? ''),
        name: (request?.chunk === 'header' ? request.header : undefined)?.submitted_by?.name,
        phone: ((request?.chunk === 'record' ? request.record : undefined)?.requester?.phone ?? ''),
      },
    };
    const messages4 = await collect(clients.documentsDocumentClient.watchDocuments(request4, metadataFor(request4.context), { deadline: deadline() }));
    const reply4 = messages4.at(-1) ?? {};
    logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.outcome?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
    );
  }

  /**
   * Creates a fan session.
   */
  async function createFanSession(request) {
    validateContext(request.context);
    if (request.fan_session) {
      await store.save(store.toFanSessionRecord(request.context.tenant_id, request.fan_session));
    }
    await store.record(request.context.tenant_id, 'CreateFanSession', request);
    logger.info('create_fan_session', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      subscriber: {
        email: (request?.fan_session?.assignee?.email ?? ''),
        name: request?.fan_session?.requester?.name,
        phone: (request?.fan_session?.requester?.phone ?? ''),
        preferred_channel: (request?.fan_session?.requester?.preferred_channel ?? ''),
      },
    };
    const messages1 = await collect(clients.fanportalWishlistNotificationClient.streamWishlistNotices(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.fan_session?.audit?.updated_at,
        labels: { ...(request?.fan_session?.custom_fields ?? {}) },
        owner_id: (request?.fan_session?.assignee?.user_id ?? ''),
      },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.venuesVenueExportClient.listVenueExports(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      stream_key: {
        contact: { phone: (request?.fan_session?.requester?.phone ?? '') },
        holder: {
          display_name: ((request?.fan_session?.origin === 'customer' ? request?.fan_session.customer : undefined)?.display_name ?? ''),
        },
      },
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => clients.broadcastsStreamKeyWorkflowClient.resumeStreamKeyWorkflow(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.result?.status ?? ''),
        },
      },
      { result: { message: (reply3?.outcome?.message ?? '') } },
      {
        fan_session: {
          priority: (request?.fan_session?.priority ?? 0),
          subject: (request?.fan_session?.subject ?? ''),
        },
      },
    );
  }

  /**
   * Deletes a fan session.
   */
  async function deleteFanSession(request) {
    validateContext(request.context);
    if (!request.fan_session_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'fan_session_id is required');
    }
    await store.record(request.context.tenant_id, 'DeleteFanSession', request);
    logger.info('delete_fan_session', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
      reason: (request?.reason ?? ''),
    };
    const reply1 = await withRetry(() => call((done) => clients.fanportalWishlistNotificationClient.subscribeWishlistUpdates(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            location: {
              lines: [...(reply1?.wishlist?.billing_address?.lines ?? [])],
              postal_code: (reply1?.wishlist?.billing_address?.postal_code ?? ''),
            },
          }],
        }],
      },
    };
    const task2 = deferred(() => call((done) => clients.seatingSeatMapApprovalClient.approveSeatMap(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    const request3 = {
      context: request?.context,
      ledger_account: { audit: { updated_at: reply1?.wishlist?.audit?.updated_at } },
    };
    const reply3 = await bus.request('acme.fanportal.v1.FanSessionService/DeleteFanSession:2', request3);
    const request4 = {
      context: request?.context,
      customer: {
        primary_address: { postal_code: (reply1?.wishlist?.billing_address?.postal_code ?? '') },
      },
    };
    const reply4 = await runInContext(request.context, () => call((done) => clients.customersCustomerCalculationClient.quoteCustomer(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.etag ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          status: (reply2?.outcome?.status ?? ''),
        },
      },
      { result: { details: (reply3?.result?.details ?? {}) } },
      { result: { status: (reply4?.result?.status ?? '') } },
    );
  }

  /**
   * Returns a fan session.
   */
  async function getFanSession(request) {
    validateContext(request.context);
    if (!request.fan_session_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'fan_session_id is required');
    }
    await store.record(request.context.tenant_id, 'GetFanSession', request);
    logger.info('get_fan_session', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { fan_session: { fan_session_id: (request?.fan_session_id ?? '') } };
  }

  /**
   * Lists fan sessions a page at a time.
   */
  async function listFanSessions(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListFanSessions', request);
    logger.info('list_fan_sessions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      user_id: (request?.context?.actor?.user_id ?? ''),
    };
    const reply1 = await call((done) => clients.identityUserClient.getUser(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      fan_sessions: [{ audit: { updated_by: (reply1?.user?.audit?.updated_by ?? '') } }],
    };
  }

  /**
   * Lists fan sessions a page at a time.
   */
  async function searchFanSessions(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'SearchFanSessions', request);
    logger.info('search_fan_sessions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        labels: { ...(request?.filter?.labels ?? {}) },
        owner_id: (request?.filter?.owner_id ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.metricsMetricSeriesQueryClient.browseMetricSerieses(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      change: {
        children: [{
          children: [{ parent_id: (request?.context?.actor?.user_id ?? '') }],
          parent_id: (request?.context?.actor?.user_id ?? ''),
        }],
        parent_id: (request?.context?.actor?.user_id ?? ''),
      },
      context: request?.context,
    };
    const messages2 = await exchange(clients.venuesVenueSectionSyncClient.streamVenueSectionChanges(metadataFor(request2.context), { deadline: deadline() }), [request2]);
    const reply2 = messages2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        message: (reply1?.result?.message ?? ''),
      },
    };
  }

  /**
   * Changes a fan session.
   */
  async function updateFanSession(request) {
    validateContext(request.context);
    if (request.fan_session) {
      await store.save(store.toFanSessionRecord(request.context.tenant_id, request.fan_session));
    }
    await store.record(request.context.tenant_id, 'UpdateFanSession', request);
    logger.info('update_fan_session', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        recipient: {
          email: (request?.fan_session?.assignee?.email ?? ''),
          phone: (request?.fan_session?.requester?.phone ?? ''),
          preferred_channel: (request?.fan_session?.requester?.preferred_channel ?? ''),
        },
      },
    };
    const subscribeWishlistUpdates1 = promisify(clients.fanportalWishlistNotificationClient.subscribeWishlistUpdates.bind(clients.fanportalWishlistNotificationClient));
    const reply1 = await subscribeWishlistUpdates1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.wishlist?.audit?.created_at,
        owner_id: (reply1?.wishlist?.wishlist_id ?? ''),
        region: (reply1?.wishlist?.billing_address?.region ?? ''),
      },
    };
    const reply2 = await call((done) => clients.seatingSeatMapApprovalClient.listPendingSeatMaps(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const settled3 = await Promise.allSettled((request?.fan_session?.comments ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        details: {
          channel: ((request?.fan_session?.origin === 'channel' ? request?.fan_session.channel : undefined)?.channel ?? ''),
          recipient: {
            email: (item?.author_email ?? ''),
            phone: (request?.fan_session?.requester?.phone ?? ''),
          },
        },
      };
      const reply3 = await call((done) => clients.fanportalWishlistNotificationClient.sendWishlistTestNotice(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const results3 = settled3.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply3 = results3.at(-1) ?? {};
    const request4 = {
      context: request?.context,
      ticket_tier: { audit: { created_by: (reply1?.wishlist?.audit?.created_by ?? '') } },
    };
    const method4 = 'pauseTicketTierWorkflow';
    const reply4 = await call((done) => clients.ticketingTicketTierWorkflowClient[method4](request4, metadataFor(request4.context), { deadline: deadline() }, done));
    const request5 = {
      context: request?.context,
      header: {
        source: (request?.context?.actor?.user_id ?? ''),
        submitted_by: request?.fan_session?.requester,
      },
    };
    const reply5 = await upload(
      (done) => clients.admissionsAdmissionClient.batchCreateAdmissions(metadataFor(request5.context), { deadline: deadline() }, done),
      [request5],
    );
    logger.info('local_audit', { value: (request?.fan_session?.audit?.etag ?? '') });
    return assemble(
      { result: { status: (reply1?.outcome?.status ?? '') } },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          details: (reply2?.result?.details ?? {}),
        },
      },
      {
        fan_session: { audit: { created_at: reply4?.ticket_tier?.audit?.created_at } },
      },
      {
        fan_session: {
          assignee: request?.fan_session?.assignee,
          requester: request?.fan_session?.requester,
        },
      },
    );
  }

  /**
   * Streams the changes of fan sessions as they happen.
   */
  async function watchFanSessions(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'WatchFanSessions', request);
    logger.info('watch_fan_sessions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
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
    const client1 = clients.registry.get('acme.fanportal.v1.WishlistNotificationService');
    const reply1 = await call((done) => client1.unsubscribeWishlistUpdates(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { filters: { ...(reply1?.wishlist?.metadata ?? {}) } },
    };
    const send2 = clients.featureflagsFeatureFlagQueryClient.summarizeFeatureFlags.bind(clients.featureflagsFeatureFlagQueryClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.filter?.region ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        fan_session: {
          requester: {
            email: (request?.subscriber?.email ?? ''),
            preferred_channel: (request?.subscriber?.preferred_channel ?? ''),
          },
        },
      },
    );
  }

  return {
    batchCreateFanSessions: clientStream(batchCreateFanSessions, () => ({})),
    createFanSession: unary(createFanSession),
    deleteFanSession: unary(deleteFanSession),
    getFanSession: unary(getFanSession),
    listFanSessions: unary(listFanSessions),
    searchFanSessions: unary(searchFanSessions),
    updateFanSession: unary(updateFanSession),
    watchFanSessions: serverStream(watchFanSessions),
  };
}

module.exports = { createFanSessionHandlers };
