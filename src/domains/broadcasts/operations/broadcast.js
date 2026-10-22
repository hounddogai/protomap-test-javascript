'use strict';

/**
 * Manages broadcasts: creates, reads, changes, and lists them. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  broadcastsBroadcastQueryClient,
  broadcastsBroadcastValidationClient,
  broadcastsReplayClipCalculationClient,
  broadcastsReplayClipValidationClient,
  broadcastsStreamKeyQueryClient,
  broadcastsStreamKeyValidationClient,
  metricsMetricSeriesQueryClient,
  venuesVenueExportClient,
  venuesVenueSectionQueryClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Takes a stream of broadcasts and answers once at the end.
 */
async function batchCreateBroadcasts(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'BatchCreateBroadcasts', request);
  logger.info('batch_create_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue: {
      address: {
        lines: [...((request?.chunk === 'record' ? request.record : undefined)?.location?.lines ?? [])],
      },
    },
  };
  const reply1 = await bus.request('acme.broadcasts.v1.BroadcastService/BatchCreateBroadcasts:0', request1);
  let selected2_1;
  switch (reply1?.venue?.locator) {
    case 'address':
      selected2_1 = ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.locality ?? '');
      break;
    default:
      selected2_1 = '';
  }
  const request2 = {
    context: request?.context,
    replay_clip: {
      address: { locality: selected2_1 },
      owner: { display_name: (reply1?.venue?.owner?.display_name ?? '') },
    },
  };
  const reply2 = await call((done) => broadcastsReplayClipValidationClient.checkReplayClipEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        message: (reply1?.outcome?.message ?? ''),
      },
    },
    { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
  );
}

/**
 * Returns several broadcasts at once.
 */
async function batchGetBroadcasts(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'BatchGetBroadcasts', request);
  logger.info('batch_get_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { owner_id: (request?.context?.actor?.user_id ?? '') },
  };
  const reply1 = await call((done) => clients.broadcastsBroadcastQueryClient.countBroadcasts(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    annotations: { ...(request?.context?.baggage ?? {}) },
    context: request?.context,
    details: { filters: { ...(reply1?.result?.details ?? {}) } },
  };
  const reply2 = await new Promise((resolve, reject) => {
    broadcastsBroadcastQueryClient.summarizeBroadcasts(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
  );
}

/**
 * Creates a broadcast.
 */
async function cloneBroadcast(request) {
  validateContext(request.context);
  if (request.broadcast) {
    await store.save(store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
  }
  await store.record(request.context.tenant_id, 'CloneBroadcast', request);
  logger.info('clone_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const resolveVenueSections1 = promisify(venuesVenueSectionQueryClient.resolveVenueSections.bind(venuesVenueSectionQueryClient));
  const reply1 = await resolveVenueSections1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    context: request?.context,
    replay_clip: {
      address: {
        lines: [...(request?.broadcast?.location?.lines ?? [])],
        locality: (request?.broadcast?.location?.locality ?? ''),
        region: (request?.broadcast?.location?.region ?? ''),
      },
      owner: { display_name: (request?.broadcast?.organizer?.display_name ?? '') },
    },
  };
  const reply2 = await call((done) => broadcastsReplayClipValidationClient.normalizeReplayClip(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const results3 = [];
  for (const item of (request?.broadcast?.participants ?? [])) {
    const request3 = {
      broadcast: {
        organizer: { email: (item?.contact?.email ?? '') },
        participants: [{ role: (item?.role ?? '') }],
      },
      context: request?.context,
      details: { contact: { name: item?.contact?.name } },
    };
    const reply3 = await call((done) => broadcastsBroadcastValidationClient.explainBroadcastDecision(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    results3.push(reply3);
  }
  const reply3 = results3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.request_id ?? '') });
  return assemble(
    {
      result: {
        message: (reply1?.result?.message ?? ''),
        status: (reply1?.result?.status ?? ''),
      },
    },
    {
      broadcast: { audit: { updated_at: reply2?.replay_clip?.audit?.updated_at } },
    },
    {
      broadcast: {
        status: (request?.broadcast?.status ?? ''),
        window: request?.broadcast?.window,
      },
    },
  );
}

/**
 * Creates a broadcast.
 */
async function createBroadcast(request) {
  validateContext(request.context);
  if (request.broadcast) {
    await store.save(store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
  }
  await store.record(request.context.tenant_id, 'CreateBroadcast', request);
  logger.info('create_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    replay_clip: {
      address: { postal_code: (request?.broadcast?.location?.postal_code ?? '') },
      audit: { updated_by: (request?.broadcast?.audit?.updated_by ?? '') },
      owner: { user_id: (request?.broadcast?.organizer?.user_id ?? '') },
    },
  };
  const method1 = 'previewReplayClip';
  const reply1 = await call((done) => broadcastsReplayClipValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  let selected2_1;
  switch (reply1?.replay_clip?.locator) {
    case 'address':
      selected2_1 = ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? '');
      break;
    default:
      selected2_1 = '';
  }
  const request2 = {
    context: request?.context,
    filter: { region: selected2_1 },
  };
  const reply2 = await call((done) => broadcastsStreamKeyQueryClient.suggestStreamKeys(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.request_id ?? '') });
  return assemble(
    {
      broadcast: {
        organizer: { email: (reply1?.replay_clip?.owner?.email ?? '') },
        participants: [{ contact: { email: (reply1?.replay_clip?.owner?.email ?? '') } }],
      },
    },
    {
      result: {
        details: (reply2?.result?.details ?? {}),
        status: (reply2?.result?.status ?? ''),
      },
    },
    {
      broadcast: {
        status: (request?.broadcast?.status ?? ''),
        window: request?.broadcast?.window,
      },
    },
  );
}

/**
 * Deletes a broadcast.
 */
async function deleteBroadcast(request) {
  validateContext(request.context);
  if (!request.broadcast_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'broadcast_id is required');
  }
  await store.record(request.context.tenant_id, 'DeleteBroadcast', request);
  logger.info('delete_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: { email: (request?.context?.actor?.email ?? '') },
      locale: (request?.context?.locale ?? ''),
    },
    reason: (request?.reason ?? ''),
    stream_key: { audit: { etag: (request?.etag ?? '') } },
  };
  const send1 = broadcastsStreamKeyValidationClient.previewStreamKey.bind(broadcastsStreamKeyValidationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    replay_clip: {
      audit: {
        created_at: reply1?.stream_key?.audit?.created_at,
        updated_at: reply1?.stream_key?.audit?.updated_at,
      },
    },
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => broadcastsReplayClipCalculationClient.recalculateReplayClips(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.broadcast_id ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    {
      result: {
        message: (reply2?.result?.message ?? ''),
        reference_ids: (reply2?.result?.reference_ids ?? []),
      },
    },
  );
}

/**
 * Returns a broadcast.
 */
async function getBroadcast(request) {
  validateContext(request.context);
  if (!request.broadcast_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'broadcast_id is required');
  }
  await store.record(request.context.tenant_id, 'GetBroadcast', request);
  logger.info('get_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    details: {
      contact: { email: (input1?.context?.actor?.email ?? '') },
      locale: (input1?.context?.locale ?? ''),
    },
  };
  const reply1 = await call((done) => broadcastsStreamKeyValidationClient.checkStreamKeyEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      contact: {
        name: reply1?.stream_key?.contact?.name,
        phone: (reply1?.stream_key?.contact?.phone ?? ''),
      },
    },
    replay_clip: {
      audit: { created_by: (reply1?.stream_key?.audit?.created_by ?? '') },
      owner: { display_name: (reply1?.stream_key?.holder?.display_name ?? '') },
    },
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => broadcastsReplayClipValidationClient.previewReplayClip(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  return assemble(
    {
      broadcast: {
        participants: [{ contact: { email: (reply1?.stream_key?.contact?.email ?? '') } }],
      },
      result: { details: (reply1?.result?.details ?? {}) },
    },
    {
      broadcast: {
        audit: { created_at: reply2?.replay_clip?.audit?.created_at },
        location: {
          region: ((reply2?.replay_clip?.locator === 'address' ? reply2?.replay_clip.address : undefined)?.region ?? ''),
        },
      },
    },
    { broadcast: { broadcast_id: (request?.broadcast_id ?? '') } },
  );
}

/**
 * Lists broadcasts a page at a time.
 */
async function listBroadcasts(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListBroadcasts', request);
  logger.info('list_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.filter?.created_after,
      region: (request?.filter?.region ?? ''),
    },
    order_by: (request?.order_by ?? ''),
    page: { page_size: (request?.page?.page_size ?? 0) },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => metricsMetricSeriesQueryClient.browseMetricSerieses(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    replay_clip: {
      address: { region: (request?.filter?.region ?? '') },
      owner: { user_id: (request?.context?.actor?.user_id ?? '') },
    },
  };
  const merged2 = Object.assign({}, request2, { context: request2.context });
  const reply2 = await call((done) => broadcastsReplayClipValidationClient.validateReplayClip(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.filter?.owner_id ?? '') });
  return assemble(
    {
      page_info: { next_page_token: (reply1?.page_info?.next_page_token ?? '') },
      result: { status: (reply1?.result?.status ?? '') },
    },
    {
      broadcasts: [{
        audit: { updated_by: (reply2?.replay_clip?.audit?.updated_by ?? '') },
        organizer: { email: (reply2?.replay_clip?.owner?.email ?? '') },
      }],
    },
  );
}

/**
 * Lists broadcasts a page at a time.
 */
async function searchBroadcasts(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'SearchBroadcasts', request);
  logger.info('search_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    replay_clip: {
      address: { region: (request?.filter?.region ?? '') },
      owner: { user_id: (request?.context?.actor?.user_id ?? '') },
    },
  };
  const reply1 = await withRetry(() => call((done) => broadcastsReplayClipValidationClient.checkReplayClipEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } };
}

/**
 * Changes a broadcast.
 */
async function updateBroadcast(request) {
  validateContext(request.context);
  if (request.broadcast) {
    await store.save(store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
  }
  await store.record(request.context.tenant_id, 'UpdateBroadcast', request);
  logger.info('update_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.broadcast?.audit?.created_at,
      region: (request?.broadcast?.location?.region ?? ''),
    },
  };
  const messages1 = await collect(broadcastsBroadcastQueryClient.exportBroadcasts(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    details: { period: request?.broadcast?.window },
  };
  const task2 = deferred(() => call((done) => broadcastsStreamKeyQueryClient.summarizeStreamKeys(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  logger.info('local_audit', { value: (request?.broadcast?.audit?.etag ?? '') });
  return assemble(
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    {
      broadcast: {
        metadata: (request?.broadcast?.metadata ?? {}),
        status: (request?.broadcast?.status ?? ''),
      },
    },
  );
}

/**
 * Streams the changes of broadcasts as they happen.
 */
async function watchBroadcasts(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'WatchBroadcasts', request);
  logger.info('watch_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: {
        phone: (request?.subscriber?.phone ?? ''),
        preferred_channel: (request?.subscriber?.preferred_channel ?? ''),
      },
    },
  };
  const reply1 = await bus.request('acme.broadcasts.v1.BroadcastService/WatchBroadcasts:0', request1);
  logger.info('local_audit', { value: (request?.filter?.owner_id ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    {
      broadcast: {
        location: { region: (request?.filter?.region ?? '') },
        organizer: { email: (request?.subscriber?.email ?? '') },
      },
    },
  );
}

module.exports = { batchCreateBroadcasts, batchGetBroadcasts, cloneBroadcast, createBroadcast, deleteBroadcast, getBroadcast, listBroadcasts, searchBroadcasts, updateBroadcast, watchBroadcasts };
