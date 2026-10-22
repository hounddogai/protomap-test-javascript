'use strict';

/**
 * Manages shows: creates, reads, changes, and lists them. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionQueryClient,
  broadcastsReplayClipCalculationClient,
  concessionsMenuItemCalculationClient,
  searchSearchIndexValidationClient,
  showsLineupCalculationClient,
  showsPerformanceValidationClient,
  showsShowCalculationClient,
  showsShowValidationClient,
  venuesVenuePolicyValidationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Returns several shows at once.
 */
async function batchGetShows(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'BatchGetShows', request);
  logger.info('batch_get_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    replay_clip: {
      audit: { created_at: request?.context?.deadline },
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const reply1 = await bus.request('acme.shows.v1.ShowService/BatchGetShows:0', request1);
  return {
    shows: [{
      location: {
        postal_code: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.postal_code ?? ''),
      },
    }],
  };
}

/**
 * Creates a show.
 */
async function cloneShow(request) {
  validateContext(request.context);
  if (request.show) {
    await store.save(store.toShowRecord(request.context.tenant_id, request.show));
  }
  await store.record(request.context.tenant_id, 'CloneShow', request);
  logger.info('clone_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    search_index: { owner: { email: (request?.show?.organizer?.email ?? '') } },
  };
  const reply1 = await runInContext(request.context, () => call((done) => searchSearchIndexValidationClient.validateSearchIndex(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  let selected2_2;
  switch (request?.show?.recurrence) {
    case 'weekly':
      selected2_2 = ((request?.show?.recurrence === 'weekly' ? request?.show.weekly : undefined)?.interval ?? 0);
      break;
    default:
      selected2_2 = 0;
  }
  const request2 = {
    context: request?.context,
    show: {
      audit: { etag: (reply1?.search_index?.audit?.etag ?? '') },
      location: { locality: (request?.show?.location?.locality ?? '') },
      participants: [{ contact: { email: (request?.show?.organizer?.email ?? '') } }],
      weekly: { interval: selected2_2 },
    },
  };
  const reply2 = await call((done) => showsShowCalculationClient.explainShowCalculation(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const settled3 = await Promise.allSettled((request?.show?.participants ?? []).map(async (item) => {
    const request3 = {
      context: request?.context,
      details: {
        contact: {
          email: (item?.contact?.email ?? ''),
          preferred_channel: (item?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply3 = await call((done) => showsPerformanceValidationClient.lintPerformances(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    return reply3;
  }));
  const results3 = settled3.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply3 = results3.at(-1) ?? {};
  const request4 = {
    context: request?.context,
    show_id: (request?.show?.show_id ?? ''),
  };
  const reply4 = await new Promise((resolve, reject) => {
    showsShowValidationClient.getShowRules(request4, metadataFor(request4.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.request_id ?? '') });
  return assemble(
    {
      result: {
        message: (reply1?.outcome?.message ?? ''),
        status: (reply1?.outcome?.status ?? ''),
      },
    },
    { show: { window: reply2?.show?.window } },
    { result: { message: (reply4?.result?.message ?? '') } },
    {
      show: {
        show_id: (request?.show?.show_id ?? ''),
        status: (request?.show?.status ?? ''),
      },
    },
  );
}

/**
 * Creates a show.
 */
async function createShow(request) {
  validateContext(request.context);
  if (request.show) {
    await store.save(store.toShowRecord(request.context.tenant_id, request.show));
  }
  await store.record(request.context.tenant_id, 'CreateShow', request);
  logger.info('create_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = [];
  for (const item of (request?.show?.participants ?? [])) {
    const request1 = {
      context: request?.context,
      performance: {
        participants: [{
          contact: {
            email: (item?.contact?.email ?? ''),
            phone: (item?.contact?.phone ?? ''),
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
          participant_id: (item?.participant_id ?? ''),
        }],
      },
    };
    const reply1 = await call((done) => showsPerformanceValidationClient.scorePerformance(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    results1.push(reply1);
  }
  const reply1 = results1.at(-1) ?? {};
  const results2 = await Promise.all((request?.show?.participants ?? []).map(async (item) => {
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          email: (item?.contact?.email ?? ''),
          name: item?.contact?.name,
        },
      },
    };
    const reply2 = await call((done) => showsLineupCalculationClient.explainLineupCalculation(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return reply2;
  }));
  const reply2 = results2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.request_id ?? '') });
  return {
    show: {
      metadata: (request?.show?.metadata ?? {}),
      title: (request?.show?.title ?? ''),
    },
  };
}

/**
 * Deletes a show.
 */
async function deleteShow(request) {
  validateContext(request.context);
  if (!request.show_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'show_id is required');
  }
  await store.record(request.context.tenant_id, 'DeleteShow', request);
  logger.info('delete_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    show: { audit: { etag: (request?.etag ?? '') } },
  };
  const recalculateShows1 = promisify(showsShowCalculationClient.recalculateShows.bind(showsShowCalculationClient));
  const reply1 = await recalculateShows1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    context: request?.context,
    filter: {
      created_after: reply1?.show?.audit?.updated_at,
      region: (reply1?.show?.location?.region ?? ''),
    },
  };
  const reply2 = await call((done) => admissionsAdmissionQueryClient.suggestAdmissions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
  );
}

/**
 * Returns a show.
 */
async function getShow(request) {
  validateContext(request.context);
  if (!request.show_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'show_id is required');
  }
  await store.record(request.context.tenant_id, 'GetShow', request);
  logger.info('get_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: { email: (request?.context?.actor?.email ?? '') },
      locale: (request?.context?.locale ?? ''),
    },
    venue_policy: { owner: { user_id: (request?.context?.actor?.user_id ?? '') } },
  };
  const method1 = 'previewVenuePolicy';
  const reply1 = await call((done) => venuesVenuePolicyValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    { show: { show_id: (request?.show_id ?? '') } },
  );
}

/**
 * Lists shows a page at a time.
 */
async function listShows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListShows', request);
  logger.info('list_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { destination: { region: (request?.filter?.region ?? '') } },
    replay_clip: {
      audit: { created_at: request?.filter?.created_after },
      owner: { email: (request?.context?.actor?.email ?? '') },
    },
  };
  const client1 = clients.registry.get('acme.broadcasts.v1.ReplayClipCalculationService');
  const reply1 = await call((done) => client1.recalculateReplayClips(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    shows: [{
      location: {
        region: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? ''),
      },
      organizer: { email: (reply1?.replay_clip?.owner?.email ?? '') },
    }],
  };
}

/**
 * Lists shows a page at a time.
 */
async function searchShows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'SearchShows', request);
  logger.info('search_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    menu_item: {
      address: { region: (request?.filter?.region ?? '') },
      owner: { user_id: (request?.context?.actor?.user_id ?? '') },
    },
  };
  const send1 = concessionsMenuItemCalculationClient.estimateMenuItem.bind(concessionsMenuItemCalculationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    shows: [{
      location: {
        region: ((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.region ?? ''),
      },
    }],
  };
}

module.exports = { batchGetShows, cloneShow, createShow, deleteShow, getShow, listShows, searchShows };
