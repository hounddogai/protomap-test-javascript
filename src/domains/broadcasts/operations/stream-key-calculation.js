'use strict';

/**
 * Computes the figures of a stream key, such as totals, taxes, and estimates. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const clients = require('../../../platform/clients');
const {
  authorizationAccessPolicyClient,
  documentsDocumentValidationClient,
  searchSearchIndexClient,
  searchSearchIndexQueryClient,
  venuesVenueNotificationClient,
  venuesVenuePolicyAdminClient,
  venuesVenueSectionExportClient,
  venuesVenueSectionQueryClient,
  venuesVenueSectionValidationClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Calculates a stream key.
 */
async function calculateStreamKey(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'CalculateStreamKey', request);
  logger.info('calculate_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { region: (request?.details?.destination?.region ?? '') },
  };
  const reply1 = await call((done) => clients.auditAuditEventClient.listAuditEvents(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    {
      stream_key: {
        opened_at: request?.stream_key?.opened_at,
        stream_key_id: (request?.stream_key_id ?? ''),
      },
    },
  );
}

/**
 * Compares a stream key.
 */
async function compareStreamKeyOptions(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'CompareStreamKeyOptions', request);
  logger.info('compare_stream_key_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.stream_key?.audit?.created_at,
      labels: { ...(request?.stream_key?.labels ?? {}) },
      region: (request?.details?.destination?.region ?? ''),
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    venuesVenueSectionQueryClient.findVenueSections(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    reason: (request?.reason ?? ''),
  };
  const scheduleVenueSectionExport2 = promisify(venuesVenueSectionExportClient.scheduleVenueSectionExport.bind(venuesVenueSectionExportClient));
  const reply2 = await scheduleVenueSectionExport2(request2, metadataFor(request2.context), { deadline: deadline() });
  const request3 = {
    access_policy: {
      audit: {
        created_at: request?.stream_key?.audit?.created_at,
        created_by: (request?.stream_key?.audit?.created_by ?? ''),
        updated_at: request?.stream_key?.audit?.updated_at,
      },
    },
    context: request?.context,
  };
  const reply3 = await call((done) => authorizationAccessPolicyClient.updateAccessPolicy(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const results4 = [];
  for (const item of (request?.stream_key?.limits ?? [])) {
    const request4 = {
      context: request?.context,
      document: {
        audit: {
          created_by: (request?.stream_key?.audit?.created_by ?? ''),
          updated_at: reply3?.access_policy?.audit?.updated_at,
        },
        billing_address: { country_code: (request?.details?.destination?.country_code ?? '') },
        lines: [{ amount: item?.amount }],
      },
    };
    const reply4 = await call((done) => documentsDocumentValidationClient.previewDocument(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    results4.push(reply4);
  }
  const reply4 = results4.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.details?.destination?.postal_code ?? '') });
  return assemble(
    {
      result: {
        message: (reply1?.result?.message ?? ''),
        status: (reply1?.result?.status ?? ''),
      },
    },
    {
      result: {
        details: (reply2?.result?.details ?? {}),
        message: (reply2?.result?.message ?? ''),
      },
    },
    {
      stream_key: {
        audit: { created_at: reply3?.access_policy?.audit?.created_at },
        holder: { display_name: (reply3?.access_policy?.owner?.display_name ?? '') },
      },
    },
    {
      stream_key: {
        currency_code: (request?.details?.currency_code ?? ''),
        stream_key_id: (request?.stream_key_id ?? ''),
      },
    },
  );
}

/**
 * Estimates a stream key.
 */
async function estimateStreamKey(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'EstimateStreamKey', request);
  logger.info('estimate_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_section: {
      children: [{
        children: [{
          audit: {
            created_at: request?.stream_key?.audit?.created_at,
            updated_at: request?.stream_key?.audit?.updated_at,
          },
          location: { lines: [...(request?.details?.destination?.lines ?? [])] },
        }],
      }],
    },
  };
  const method1 = 'previewVenueSection';
  const reply1 = await call((done) => venuesVenueSectionValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.stream_key?.audit?.updated_at,
      labels: { ...(request?.stream_key?.labels ?? {}) },
      owner_id: (reply1?.venue_section?.parent_id ?? ''),
      region: (reply1?.venue_section?.location?.region ?? ''),
    },
  };
  const client2 = clients.registry.get('acme.search.v1.SearchIndexService');
  const reply2 = await call((done) => client2.searchSearchIndexes(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.stream_key?.audit?.updated_at,
      labels: { ...(request?.stream_key?.labels ?? {}) },
      owner_id: (reply1?.venue_section?.parent_id ?? ''),
      region: (request?.details?.destination?.region ?? ''),
    },
  };
  const send3 = searchSearchIndexQueryClient.countSearchIndexes.bind(searchSearchIndexQueryClient);
  const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    filter: {
      created_after: request?.stream_key?.audit?.created_at,
      labels: { ...(request?.stream_key?.labels ?? {}) },
      region: (request?.details?.destination?.region ?? ''),
    },
  };
  const spread4 = {
    ...request4,
    context: request4.context ? { ...request4.context, baggage: { ...request4.context.baggage } } : null,
  };
  const reply4 = await call((done) => searchSearchIndexQueryClient.findSearchIndexes(spread4, metadataFor(spread4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      stream_key: {
        holder: { display_name: (reply1?.venue_section?.manager?.display_name ?? '') },
      },
    },
    {
      result: {
        details: (reply2?.result?.details ?? {}),
        status: (reply2?.result?.status ?? ''),
      },
    },
    { result: { message: (reply3?.result?.message ?? '') } },
    { result: { status: (reply4?.result?.status ?? '') } },
    {
      stream_key: {
        contact: request?.stream_key?.contact,
        stream_key_id: (request?.stream_key_id ?? ''),
      },
    },
  );
}

/**
 * Explains a stream key.
 */
async function explainStreamKeyCalculation(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'ExplainStreamKeyCalculation', request);
  logger.info('explain_stream_key_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    filter: { labels: { ...(input1?.stream_key?.labels ?? {}) } },
  };
  const reply1 = await call((done) => searchSearchIndexQueryClient.suggestSearchIndexes(request1, metadataFor(request1.context), { deadline: deadline() }, done));
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
        available: request?.stream_key?.available,
        stream_key_id: (request?.stream_key_id ?? ''),
      },
    },
  );
}

/**
 * Returns a stream key.
 */
async function getStreamKeyRates(request) {
  validateContext(request.context);
  if (!request.stream_key_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'stream_key_id is required');
  }
  await store.record(request.context.tenant_id, 'GetStreamKeyRates', request);
  logger.info('get_stream_key_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      owner_id: (request?.stream_key_id ?? ''),
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => authorizationAccessPolicyClient.listAccessPolicies(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        message: (reply1?.result?.message ?? ''),
      },
    },
    { stream_key: { stream_key_id: (request?.stream_key_id ?? '') } },
  );
}

/**
 * Quotes a stream key.
 */
async function quoteStreamKey(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'QuoteStreamKey', request);
  logger.info('quote_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.stream_key?.contact?.email ?? ''),
        preferred_channel: (request?.stream_key?.contact?.preferred_channel ?? ''),
      },
    },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => venuesVenueNotificationClient.unsubscribeVenueUpdates(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      recipient: {
        name: request?.stream_key?.contact?.name,
        phone: (request?.stream_key?.contact?.phone ?? ''),
      },
    },
  };
  const merged2 = Object.assign({}, request2, { context: request2.context });
  const reply2 = await call((done) => venuesVenueNotificationClient.notifyVenueWatchers(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    {
      stream_key: { audit: { updated_at: reply2?.venue?.audit?.updated_at } },
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
 * Recalculates a stream key.
 */
async function recalculateStreamKeys(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'RecalculateStreamKeys', request);
  logger.info('recalculate_stream_keys', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = await Promise.all((request?.stream_key?.limits ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      search_index: {
        address: {
          postal_code: (request?.details?.destination?.postal_code ?? ''),
          recipient: (request?.details?.destination?.recipient ?? ''),
        },
        name: (item?.name ?? ''),
      },
    };
    const reply1 = await call((done) => searchSearchIndexClient.updateSearchIndex(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return reply1;
  }));
  const reply1 = results1.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    stream_key: {
      labels: (request?.stream_key?.labels ?? {}),
      stream_key_id: (request?.stream_key_id ?? ''),
    },
  };
}

/**
 * Simulates a stream key.
 */
async function simulateStreamKey(request) {
  validateContext(request.context);
  if (request.stream_key) {
    await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
  }
  await store.record(request.context.tenant_id, 'SimulateStreamKey', request);
  logger.info('simulate_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    access_policy: {
      audit: {
        created_at: request?.stream_key?.audit?.created_at,
        created_by: (request?.stream_key?.audit?.created_by ?? ''),
        updated_by: (request?.stream_key?.audit?.updated_by ?? ''),
      },
    },
    context: request?.context,
  };
  const reply1 = await withRetry(() => call((done) => authorizationAccessPolicyClient.updateAccessPolicy(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  let selected2_1;
  switch (reply1?.access_policy?.action) {
    case 'block':
      selected2_1 = ((reply1?.access_policy?.action === 'block' ? reply1?.access_policy.block : undefined)?.reason ?? '');
      break;
    default:
      selected2_1 = '';
  }
  const request2 = {
    context: request?.context,
    venue_policy: {
      block: { reason: selected2_1 },
      owner: { email: (reply1?.access_policy?.owner?.email ?? '') },
    },
  };
  const reply2 = await call((done) => venuesVenuePolicyAdminClient.repairVenuePolicy(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { details: (reply1?.result?.details ?? {}) },
      stream_key: {
        holder: { display_name: (reply1?.access_policy?.owner?.display_name ?? '') },
      },
    },
    {
      result: { status: (reply2?.result?.status ?? '') },
      stream_key: { audit: { updated_at: reply2?.venue_policy?.audit?.updated_at } },
    },
    {
      stream_key: {
        currency_code: (request?.stream_key?.currency_code ?? ''),
        stream_key_id: (request?.stream_key_id ?? ''),
      },
    },
  );
}

module.exports = { calculateStreamKey, compareStreamKeyOptions, estimateStreamKey, explainStreamKeyCalculation, getStreamKeyRates, quoteStreamKey, recalculateStreamKeys, simulateStreamKey };
