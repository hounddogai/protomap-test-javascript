'use strict';

/**
 * Answers read-only questions about broadcasts, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of BroadcastQueryService, over the dependencies the broadcasts team is given.
 */
function createBroadcastQueryHandlers({ store, clients, bus, logger }) {
  /**
   * Lists broadcasts a page at a time.
   */
  async function browseBroadcasts(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'BrowseBroadcasts', request);
    logger.info('browse_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
      replay_clip: {
        audit: { created_at: request?.filter?.created_after },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const client1 = clients.registry.get('acme.broadcasts.v1.ReplayClipCalculationService');
    const reply1 = await call((done) => client1.simulateReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } };
  }

  /**
   * Counts broadcasts by status.
   */
  async function countBroadcasts(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'CountBroadcasts', request);
    logger.info('count_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: { labels: { ...(request?.filter?.labels ?? {}) } },
    };
    const send1 = clients.broadcastsStreamKeyValidationClient.lintStreamKeys.bind(clients.broadcastsStreamKeyValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { message: (reply1?.result?.message ?? '') } };
  }

  /**
   * Streams every matching broadcast.
   */
  async function exportBroadcasts(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'ExportBroadcasts', request);
    logger.info('export_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: { labels: { ...(request?.filter?.labels ?? {}) } },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.broadcastsStreamKeyCalculationClient.quoteStreamKey(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      details: { recipient: { phone: (reply1?.stream_key?.contact?.phone ?? '') } },
    };
    const reply2 = await call((done) => clients.venuesVenueNotificationClient.sendVenueTestNotice(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    let selected3_3;
    switch (reply1?.stream_key?.funding) {
      case 'bank':
        selected3_3 = ((reply1?.stream_key?.funding === 'bank' ? reply1?.stream_key.bank : undefined)?.account_holder ?? '');
        break;
      default:
        selected3_3 = '';
    }
    const request3 = {
      context: request?.context,
      stream_key: {
        audit: { updated_at: reply1?.stream_key?.audit?.updated_at },
        bank: { account_holder: selected3_3 },
        contact: { email: (reply1?.stream_key?.contact?.email ?? '') },
        holder: { show_id: (reply1?.stream_key?.holder?.show_id ?? '') },
      },
    };
    const reply3 = await call((done) => clients.broadcastsStreamKeyValidationClient.normalizeStreamKey(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      {
        broadcast: { audit: { created_at: reply1?.stream_key?.audit?.created_at } },
      },
      {
        broadcast: {
          participants: [{ contact: { email: (reply2?.venue?.owner?.email ?? '') } }],
        },
      },
      {
        broadcast: { participants: [{ contact: reply3?.stream_key?.contact }] },
        result: { status: (reply3?.outcome?.status ?? '') },
      },
      { broadcast: { location: { region: (request?.filter?.region ?? '') } } },
    );
  }

  /**
   * Returns a broadcast.
   */
  async function lookupBroadcast(request) {
    validateContext(request.context);
    if (!request.broadcast_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'broadcast_id is required');
    }
    await store.record(request.context.tenant_id, 'LookupBroadcast', request);
    logger.info('lookup_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { broadcast: { broadcast_id: (request?.broadcast_id ?? '') } };
  }

  /**
   * Returns a broadcast.
   */
  async function peekBroadcast(request) {
    validateContext(request.context);
    if (!request.broadcast_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'broadcast_id is required');
    }
    await store.record(request.context.tenant_id, 'PeekBroadcast', request);
    logger.info('peek_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        owner_id: (request?.broadcast_id ?? ''),
      },
    };
    const messages1 = await collect(clients.identityUserQueryClient.exportUsers(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      replay_clip: { owner: { user_id: (request?.context?.actor?.user_id ?? '') } },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.broadcastsReplayClipCalculationClient.compareReplayClipOptions(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    return assemble(
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      { broadcast: { broadcast_id: (request?.broadcast_id ?? '') } },
    );
  }

  /**
   * Returns several broadcasts at once.
   */
  async function resolveBroadcasts(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'ResolveBroadcasts', request);
    logger.info('resolve_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: { contact: { email: (request?.context?.actor?.email ?? '') } },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.broadcastsStreamKeyCalculationClient.compareStreamKeyOptions(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    return { result: { status: (reply1?.result?.status ?? '') } };
  }

  /**
   * Lists broadcasts a page at a time.
   */
  async function suggestBroadcasts(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'SuggestBroadcasts', request);
    logger.info('suggest_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Summarizes a broadcast.
   */
  async function summarizeBroadcasts(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'SummarizeBroadcasts', request);
    logger.info('summarize_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: {
        recipient: { email: (request?.context?.actor?.email ?? '') },
        variables: { ...(request?.details?.filters ?? {}) },
      },
      reason: (request?.reason ?? ''),
    };
    const reply1 = await withRetry(() => call((done) => clients.venuesVenuePolicyNotificationClient.sendVenuePolicyTestNotice(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      stream_key: {
        audit: {
          created_by: (reply1?.venue_policy?.audit?.created_by ?? ''),
          updated_at: reply1?.venue_policy?.audit?.updated_at,
        },
        limits: [{ name: (reply1?.venue_policy?.name ?? '') }],
      },
    };
    const task2 = deferred(() => call((done) => clients.broadcastsStreamKeyCalculationClient.estimateStreamKey(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      { period: request?.details?.period },
    );
  }

  return {
    browseBroadcasts: unary(browseBroadcasts),
    countBroadcasts: unary(countBroadcasts),
    exportBroadcasts: serverStream(exportBroadcasts),
    lookupBroadcast: unary(lookupBroadcast),
    peekBroadcast: unary(peekBroadcast),
    resolveBroadcasts: unary(resolveBroadcasts),
    suggestBroadcasts: unary(suggestBroadcasts),
    summarizeBroadcasts: unary(summarizeBroadcasts),
  };
}

module.exports = { createBroadcastQueryHandlers };
