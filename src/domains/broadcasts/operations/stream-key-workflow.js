'use strict';

/**
 * Runs the long-lived workflow that takes a stream key from start to finish. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const clients = require('../../../platform/clients');
const {
  broadcastsBroadcastApprovalClient,
  broadcastsReplayClipApprovalClient,
  broadcastsStreamKeyApprovalClient,
  broadcastsStreamKeyCalculationClient,
  notificationsNotificationValidationClient,
  performersRiderValidationClient,
  showsPerformanceCalculationClient,
  showsPerformanceQueryClient,
  showsShowClient,
  showsShowQueryClient,
  showsShowValidationClient,
  venuesVenueExportClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Advances a stream key.
 */
async function advanceStreamKeyWorkflow(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'AdvanceStreamKeyWorkflow', request);
  logger.info('advance_stream_key_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    stream_key: {
      audit: {
        created_by: (request?.stream_key?.audit?.created_by ?? ''),
        updated_by: (request?.stream_key?.audit?.updated_by ?? ''),
      },
      contact: {
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => broadcastsStreamKeyApprovalClient.commentStreamKeyApproval(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    broadcast: { audit: { created_by: (reply1?.stream_key?.audit?.created_by ?? '') } },
    context: request?.context,
  };
  const reply2 = await withRetry(() => call((done) => broadcastsBroadcastApprovalClient.approveBroadcast(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      stream_key: { holder: { starts_at: reply1?.stream_key?.holder?.starts_at } },
    },
    {
      result: {
        details: (reply2?.result?.details ?? {}),
        status: (reply2?.result?.status ?? ''),
      },
    },
    {
      stream_key: {
        currency_code: (request?.stream_key?.currency_code ?? ''),
        stream_key_id: (request?.stream_key_id ?? ''),
      },
    },
  );
}

/**
 * Cancels a stream key.
 */
async function cancelStreamKeyWorkflow(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'CancelStreamKeyWorkflow', request);
  logger.info('cancel_stream_key_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = [];
  for (const item of (request?.stream_key?.limits ?? [])) {
    const request1 = {
      context: request?.context,
      replay_clip: {
        audit: { updated_at: request?.stream_key?.audit?.updated_at },
        name: (item?.name ?? ''),
        owner: {
          display_name: (request?.stream_key?.holder?.display_name ?? ''),
          email: (request?.stream_key?.contact?.email ?? ''),
        },
      },
    };
    const reply1 = await call((done) => broadcastsReplayClipApprovalClient.escalateReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    results1.push(reply1);
  }
  const reply1 = results1.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    stream_key: {
      currency_code: (request?.stream_key?.currency_code ?? ''),
      stream_key_id: (request?.stream_key_id ?? ''),
    },
  };
}

/**
 * Returns a stream key.
 */
async function inspectStreamKeyWorkflow(request) {
  validateContext(request.context);
  if (!request.stream_key_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'stream_key_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectStreamKeyWorkflow', request);
  logger.info('inspect_stream_key_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    stream_key: {
      audit: { updated_at: request?.context?.deadline },
      contact: { email: (request?.context?.actor?.email ?? '') },
    },
    stream_key_id: (request?.stream_key_id ?? ''),
  };
  const task1 = deferred(() => call((done) => broadcastsStreamKeyCalculationClient.compareStreamKeyOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  let selected2_1;
  switch (reply1?.stream_key?.funding) {
    case 'card':
      selected2_1 = ((reply1?.stream_key?.funding === 'card' ? reply1?.stream_key.card : undefined)?.last4 ?? '');
      break;
    default:
      selected2_1 = '';
  }
  const request2 = {
    context: request?.context,
    stream_key: {
      card: { last4: selected2_1 },
      contact: { email: (reply1?.stream_key?.contact?.email ?? '') },
    },
  };
  const reply2 = await call((done) => broadcastsStreamKeyCalculationClient.simulateStreamKey(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  return assemble(
    {
      stream_key: {
        audit: { created_by: (reply1?.stream_key?.audit?.created_by ?? '') },
        holder: reply1?.stream_key?.holder,
      },
    },
    {
      stream_key: {
        balance: reply2?.stream_key?.balance,
        stream_key_id: (reply2?.stream_key?.stream_key_id ?? ''),
      },
    },
  );
}

/**
 * Lists stream keys a page at a time.
 */
async function listStreamKeyWorkflows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListStreamKeyWorkflows', request);
  logger.info('list_stream_key_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.filter?.created_after,
      labels: { ...(request?.filter?.labels ?? {}) },
    },
  };
  const messages1 = await collect(venuesVenueExportClient.streamVenueExport(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Pauses a stream key.
 */
async function pauseStreamKeyWorkflow(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'PauseStreamKeyWorkflow', request);
  logger.info('pause_stream_key_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    rider: {
      audit: {
        created_by: (request?.stream_key?.audit?.created_by ?? ''),
        etag: (request?.stream_key?.audit?.etag ?? ''),
      },
      owner: {
        display_name: (request?.stream_key?.holder?.display_name ?? ''),
        email: (request?.details?.notify?.email ?? ''),
      },
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => performersRiderValidationClient.scoreRider(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    performance: {
      audit: { created_at: reply1?.rider?.audit?.created_at },
      participants: [{
        contact: {
          email: (request?.details?.notify?.email ?? ''),
          name: request?.stream_key?.contact?.name,
        },
      }],
    },
  };
  const reply2 = await call((done) => clients.showsPerformanceValidationClient.validatePerformance(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      stream_key: { currency_code: (reply1?.breakdown?.currency_code ?? '') },
    },
    { outcome: { message: (reply2?.result?.message ?? '') } },
    {
      stream_key: {
        balance: request?.stream_key?.balance,
        stream_key_id: (request?.stream_key_id ?? ''),
      },
    },
  );
}

/**
 * Resumes a stream key.
 */
async function resumeStreamKeyWorkflow(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'ResumeStreamKeyWorkflow', request);
  logger.info('resume_stream_key_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    performance: {
      participants: [{ contact: { phone: (request?.details?.notify?.phone ?? '') } }],
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    showsPerformanceCalculationClient.comparePerformanceOptions(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    context: request?.context,
    filter: { region: (reply1?.performance?.location?.region ?? '') },
  };
  const messages2 = await collect(showsShowQueryClient.exportShows(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      stream_key: { audit: { updated_by: (reply1?.performance?.audit?.updated_by ?? '') } },
    },
    {
      stream_key: {
        holder: request?.stream_key?.holder,
        stream_key_id: (request?.stream_key_id ?? ''),
      },
    },
  );
}

/**
 * Retrys a stream key.
 */
async function retryStreamKeyWorkflow(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'RetryStreamKeyWorkflow', request);
  logger.info('retry_stream_key_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.stream_key?.holder?.starts_at,
      labels: { ...(request?.stream_key?.labels ?? {}) },
      owner_id: (request?.stream_key?.holder?.show_id ?? ''),
    },
  };
  const suggestShows1 = promisify(showsShowQueryClient.suggestShows.bind(showsShowQueryClient));
  const reply1 = await suggestShows1(request1, metadataFor(request1.context), { deadline: deadline() });
  const results2 = await Promise.all((request?.stream_key?.limits ?? []).map(async (item) => {
    const request2 = {
      context: request?.context,
      details: { contact: { phone: (request?.stream_key?.contact?.phone ?? '') } },
      rider: {
        discount: { amount: item?.amount },
        name: (item?.name ?? ''),
      },
    };
    const reply2 = await call((done) => performersRiderValidationClient.validateRider(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return reply2;
  }));
  const reply2 = results2.at(-1) ?? {};
  const request3 = {
    context: request?.context,
    performance: {
      audit: { created_by: (reply2?.rider?.audit?.created_by ?? '') },
      participants: [{
        contact: {
          phone: (request?.details?.notify?.phone ?? ''),
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      }],
    },
  };
  const reply3 = await call((done) => showsPerformanceCalculationClient.calculatePerformance(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        status: (reply1?.result?.status ?? ''),
      },
    },
    {
      stream_key: {
        audit: {
          created_by: (reply3?.performance?.audit?.created_by ?? ''),
          updated_by: (reply3?.performance?.audit?.updated_by ?? ''),
        },
      },
    },
    {
      stream_key: {
        contact: request?.stream_key?.contact,
        stream_key_id: (request?.stream_key_id ?? ''),
      },
    },
  );
}

/**
 * Signals a stream key.
 */
async function signalStreamKeyWorkflow(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'SignalStreamKeyWorkflow', request);
  logger.info('signal_stream_key_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    etag: (request?.stream_key?.audit?.etag ?? ''),
    reason: (request?.reason ?? ''),
    show_id: (request?.stream_key?.holder?.show_id ?? ''),
  };
  const method1 = 'deleteShow';
  const reply1 = await call((done) => showsShowClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    notification: { audit: { updated_by: (request?.stream_key?.audit?.updated_by ?? '') } },
  };
  const client2 = clients.registry.get('acme.notifications.v1.NotificationValidationService');
  const reply2 = await call((done) => client2.normalizeNotification(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
  return assemble(
    { outcome: { status: (reply1?.result?.status ?? '') } },
    {
      result: { details: (reply2?.result?.details ?? {}) },
      stream_key: {
        contact: {
          preferred_channel: (reply2?.notification?.requester?.preferred_channel ?? ''),
        },
      },
    },
    {
      stream_key: {
        balance: request?.stream_key?.balance,
        stream_key_id: (request?.stream_key_id ?? ''),
      },
    },
  );
}

/**
 * Starts a stream key.
 */
async function startStreamKeyWorkflow(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'StartStreamKeyWorkflow', request);
  logger.info('start_stream_key_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    show_id: (request?.stream_key?.holder?.show_id ?? ''),
  };
  const send1 = showsShowValidationClient.getShowRules.bind(showsShowValidationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = { context: request?.context };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => showsPerformanceQueryClient.resolvePerformances(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { status: (reply1?.result?.status ?? '') },
      stream_key: { audit: { created_by: (reply1?.show?.audit?.created_by ?? '') } },
    },
    { result: { details: (reply2?.result?.details ?? {}) } },
    {
      stream_key: {
        balance: request?.stream_key?.balance,
        stream_key_id: (request?.stream_key_id ?? ''),
      },
    },
  );
}

module.exports = { advanceStreamKeyWorkflow, cancelStreamKeyWorkflow, inspectStreamKeyWorkflow, listStreamKeyWorkflows, pauseStreamKeyWorkflow, resumeStreamKeyWorkflow, retryStreamKeyWorkflow, signalStreamKeyWorkflow, startStreamKeyWorkflow };
