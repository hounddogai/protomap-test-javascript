'use strict';

/**
 * Manages concession stands: creates, reads, changes, and lists them. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  broadcastsReplayClipCalculationClient,
  broadcastsStreamKeyQueryClient,
  concessionsConcessionOrderCalculationClient,
  concessionsConcessionOrderQueryClient,
  concessionsConcessionOrderValidationClient,
  concessionsConcessionStandCalculationClient,
  concessionsMenuItemValidationClient,
  documentsDocumentClient,
  searchSearchIndexValidationClient,
  sponsorshipsSponsorQueryClient,
  sponsorshipsSponsorshipDealQueryClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Takes a stream of concession stands and answers once at the end.
 */
async function batchCreateConcessionStands(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'BatchCreateConcessionStands', request);
  logger.info('batch_create_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    concession_order: {
      audit: {
        created_by: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.created_by ?? ''),
      },
      billing_address: {
        region: (((request?.chunk === 'record' ? request.record : undefined)?.locator === 'address' ? (request?.chunk === 'record' ? request.record : undefined).address : undefined)?.region ?? ''),
      },
    },
    context: request?.context,
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => concessionsConcessionOrderValidationClient.scoreConcessionOrder(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  let selected2_1;
  switch (request?.chunk) {
    case 'record':
      selected2_1 = ((request?.chunk === 'record' ? request.record : undefined)?.dimensions?.weight ?? 0);
      break;
    default:
      selected2_1 = 0;
  }
  const request2 = {
    context: request?.context,
    search_index: { dimensions: { weight: selected2_1 } },
  };
  const reply2 = await call((done) => searchSearchIndexValidationClient.validateSearchIndex(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        message: (reply1?.result?.message ?? ''),
      },
    },
    { result: { message: (reply2?.result?.message ?? '') } },
  );
}

/**
 * Returns several concession stands at once.
 */
async function batchGetConcessionStands(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'BatchGetConcessionStands', request);
  logger.info('batch_get_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    concession_order: { audit: { updated_at: request?.context?.deadline } },
    context: request?.context,
  };
  const task1 = deferred(() => call((done) => concessionsConcessionOrderCalculationClient.quoteConcessionOrder(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  return {
    concession_stands: [{ audit: { updated_at: reply1?.concession_order?.audit?.updated_at } }],
    result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
  };
}

/**
 * Creates a concession stand.
 */
async function cloneConcessionStand(request) {
  validateContext(request.context);
  if (request.concession_stand) {
    await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
  }
  await store.record(request.context.tenant_id, 'CloneConcessionStand', request);
  logger.info('clone_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      region: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.region ?? ''),
    },
  };
  const messages1 = await collect(sponsorshipsSponsorshipDealQueryClient.exportSponsorshipDeals(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    concession_order: {
      audit: { created_at: request?.concession_stand?.audit?.created_at },
      billing_address: {
        locality: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.locality ?? ''),
        postal_code: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.postal_code ?? ''),
      },
    },
    context: request?.context,
  };
  const reply2 = await bus.request('acme.concessions.v1.ConcessionStandService/CloneConcessionStand:1', request2);
  let selected3_1;
  switch (request?.concession_stand?.locator) {
    case 'address':
      selected3_1 = ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.locality ?? '');
      break;
    default:
      selected3_1 = '';
  }
  const request3 = {
    context: request?.context,
    details: { destination: { locality: selected3_1 } },
  };
  const reply3 = await call((done) => concessionsConcessionStandCalculationClient.recalculateConcessionStands(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.request_id ?? '') });
  return assemble(
    {
      concession_stand: { audit: { updated_at: reply2?.concession_order?.audit?.updated_at } },
      result: { status: (reply2?.result?.status ?? '') },
    },
    {
      concession_stand: { status: (reply3?.concession_stand?.status ?? '') },
    },
    {
      concession_stand: {
        capacity: (request?.concession_stand?.capacity ?? '0'),
        owner: request?.concession_stand?.owner,
      },
    },
  );
}

/**
 * Creates a concession stand.
 */
async function createConcessionStand(request) {
  validateContext(request.context);
  if (request.concession_stand) {
    await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
  }
  await store.record(request.context.tenant_id, 'CreateConcessionStand', request);
  logger.info('create_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    concession_order: {
      audit: { etag: (request?.concession_stand?.audit?.etag ?? '') },
      billing_address: {
        country_code: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.country_code ?? ''),
        locality: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.locality ?? ''),
      },
    },
    context: request?.context,
  };
  const reply1 = await call((done) => clients.concessionsConcessionOrderCalculationClient.simulateConcessionOrder(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.request_id ?? '') });
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    {
      concession_stand: {
        specs: (request?.concession_stand?.specs ?? {}),
        status: (request?.concession_stand?.status ?? ''),
      },
    },
  );
}

/**
 * Returns a concession stand.
 */
async function getConcessionStand(request) {
  validateContext(request.context);
  if (!request.concession_stand_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
  }
  await store.record(request.context.tenant_id, 'GetConcessionStand', request);
  logger.info('get_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    menu_item: { owner: { email: (request?.context?.actor?.email ?? '') } },
  };
  const reply1 = await new Promise((resolve, reject) => {
    concessionsMenuItemValidationClient.explainMenuItemDecision(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  return assemble(
    { result: { message: (reply1?.outcome?.message ?? '') } },
    {
      concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
    },
  );
}

/**
 * Lists concession stands a page at a time.
 */
async function searchConcessionStands(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'SearchConcessionStands', request);
  logger.info('search_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { destination: { region: (request?.filter?.region ?? '') } },
  };
  const simulateReplayClip1 = promisify(broadcastsReplayClipCalculationClient.simulateReplayClip.bind(broadcastsReplayClipCalculationClient));
  const reply1 = await simulateReplayClip1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    concession_order: { audit: { created_at: reply1?.replay_clip?.audit?.created_at } },
    context: request?.context,
  };
  const reply2 = await call((done) => concessionsConcessionOrderValidationClient.normalizeConcessionOrder(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return assemble(
    {
      concession_stands: [{
        category_path: (reply1?.replay_clip?.category_path ?? []),
        name: (reply1?.replay_clip?.name ?? ''),
      }],
    },
    {
      concession_stands: [{ audit: { etag: (reply2?.concession_order?.audit?.etag ?? '') } }],
    },
  );
}

/**
 * Changes a concession stand.
 */
async function updateConcessionStand(request) {
  validateContext(request.context);
  if (request.concession_stand) {
    await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
  }
  await store.record(request.context.tenant_id, 'UpdateConcessionStand', request);
  logger.info('update_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.concession_stand?.specs ?? {}) },
    context: request?.context,
    details: { filters: { ...(request?.context?.baggage ?? {}) } },
  };
  const method1 = 'summarizeConcessionOrders';
  const reply1 = await call((done) => concessionsConcessionOrderQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = { context: request?.context };
  const client2 = clients.registry.get('acme.broadcasts.v1.StreamKeyQueryService');
  const reply2 = await call((done) => client2.resolveStreamKeys(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    menu_item: {
      audit: { updated_by: (request?.concession_stand?.audit?.updated_by ?? '') },
    },
  };
  const send3 = concessionsMenuItemValidationClient.validateMenuItem.bind(concessionsMenuItemValidationClient);
  const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    filter: {
      created_after: reply3?.menu_item?.audit?.created_at,
      owner_id: (request?.concession_stand?.owner?.user_id ?? ''),
      region: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.region ?? ''),
    },
  };
  const spread4 = {
    ...request4,
    context: request4.context ? { ...request4.context, baggage: { ...request4.context.baggage } } : null,
  };
  const reply4 = await call((done) => concessionsConcessionOrderQueryClient.suggestConcessionOrders(spread4, metadataFor(spread4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.concession_stand?.audit?.etag ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    {
      result: {
        message: (reply2?.result?.message ?? ''),
        reference_ids: (reply2?.result?.reference_ids ?? []),
      },
    },
    {
      concession_stand: { audit: { created_at: reply3?.menu_item?.audit?.created_at } },
    },
    {
      result: {
        message: (reply4?.result?.message ?? ''),
        reference_ids: (reply4?.result?.reference_ids ?? []),
      },
    },
    {
      concession_stand: {
        code: (request?.concession_stand?.code ?? ''),
        name: (request?.concession_stand?.name ?? ''),
      },
    },
  );
}

/**
 * Streams the changes of concession stands as they happen.
 */
async function watchConcessionStands(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'WatchConcessionStands', request);
  logger.info('watch_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    annotations: { ...(input1?.context?.baggage ?? {}) },
    context: input1?.context,
    details: { filters: { ...(input1?.filter?.labels ?? {}) } },
  };
  const reply1 = await call((done) => sponsorshipsSponsorQueryClient.summarizeSponsors(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = { context: request?.context };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => documentsDocumentClient.batchGetDocuments(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
    {
      concession_stand: { owner: { email: (request?.subscriber?.email ?? '') } },
    },
  );
}

module.exports = { batchCreateConcessionStands, batchGetConcessionStands, cloneConcessionStand, createConcessionStand, getConcessionStand, searchConcessionStands, updateConcessionStand, watchConcessionStands };
