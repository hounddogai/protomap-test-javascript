'use strict';

/**
 * Computes the figures of a lineup, such as totals, taxes, and estimates. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  boxofficeBoxOfficeSaleQueryClient,
  boxofficeBoxOfficeSaleValidationClient,
  showsPerformanceCalculationClient,
  showsShowCalculationClient,
  showsShowValidationClient,
  venuesVenueClient,
  venuesVenueSectionExportClient,
  venuesVenueSectionQueryClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Calculates a lineup.
 */
async function calculateLineup(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'CalculateLineup', request);
  logger.info('calculate_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    show: {
      location: { recipient: (request?.lineup?.billing_address?.recipient ?? '') },
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => showsShowValidationClient.scoreShow(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    context: input2?.context,
    details: {
      destination: { country_code: (reply1?.show?.location?.country_code ?? '') },
    },
    show: {
      location: { postal_code: (input2?.details?.destination?.postal_code ?? '') },
    },
  };
  const reply2 = await call((done) => showsShowCalculationClient.compareShowOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.lineup?.issued_at,
      owner_id: (reply2?.show?.organizer?.user_id ?? ''),
      region: (reply1?.show?.location?.region ?? ''),
    },
  };
  const cloned3 = JSON.parse(JSON.stringify(request3));
  const reply3 = await call((done) => venuesVenueSectionExportClient.listVenueSectionExports(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    details: { destination: { lines: [...(reply2?.show?.location?.lines ?? [])] } },
    show: {
      location: {
        country_code: (request?.details?.destination?.country_code ?? ''),
        recipient: (reply1?.show?.location?.recipient ?? ''),
        region: (reply1?.show?.location?.region ?? ''),
      },
    },
  };
  const copied4 = structuredClone(request4);
  const reply4 = await call((done) => showsShowCalculationClient.calculateShow(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      lineup: {
        billing_address: { locality: (reply1?.show?.location?.locality ?? '') },
      },
      result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
    },
    {
      lineup: { audit: { created_by: (reply2?.show?.audit?.created_by ?? '') } },
    },
    {
      result: {
        correlation_id: (reply3?.result?.correlation_id ?? ''),
        message: (reply3?.result?.message ?? ''),
      },
    },
    {
      lineup: { billing_address: { region: (reply4?.show?.location?.region ?? '') } },
    },
    {
      lineup: {
        due_at: request?.lineup?.due_at,
        lineup_id: (request?.lineup_id ?? ''),
      },
    },
  );
}

/**
 * Compares a lineup.
 */
async function compareLineupOptions(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'CompareLineupOptions', request);
  logger.info('compare_lineup_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      destination: { postal_code: (request?.details?.destination?.postal_code ?? '') },
    },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => showsShowCalculationClient.quoteShow(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.lineup?.audit?.updated_at,
      labels: { ...(reply1?.show?.metadata ?? {}) },
      owner_id: (reply1?.result?.correlation_id ?? ''),
      region: (request?.lineup?.billing_address?.region ?? ''),
    },
  };
  const messages2 = await collect(venuesVenueSectionQueryClient.exportVenueSections(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      lineup: {
        billing_address: { country_code: (reply1?.show?.location?.country_code ?? '') },
        total: reply1?.breakdown?.total,
      },
    },
    {
      lineup: {
        lineup_id: (request?.lineup_id ?? ''),
        notes: (request?.lineup?.notes ?? ''),
      },
    },
  );
}

/**
 * Estimates a lineup.
 */
async function estimateLineup(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'EstimateLineup', request);
  logger.info('estimate_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      destination: { region: (request?.lineup?.billing_address?.region ?? '') },
    },
    performance: {
      audit: { created_by: (request?.lineup?.audit?.created_by ?? '') },
      location: {
        locality: (request?.details?.destination?.locality ?? ''),
        postal_code: (request?.details?.destination?.postal_code ?? ''),
      },
    },
  };
  const reply1 = await withRetry(() => call((done) => showsPerformanceCalculationClient.calculatePerformance(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    show: {
      audit: { created_by: (reply1?.performance?.audit?.created_by ?? '') },
      location: {
        locality: (request?.details?.destination?.locality ?? ''),
        region: (request?.lineup?.billing_address?.region ?? ''),
      },
    },
  };
  const task2 = deferred(() => call((done) => showsShowCalculationClient.quoteShow(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  const request3 = {
    context: request?.context,
    details: {
      destination: { recipient: (request?.details?.destination?.recipient ?? '') },
    },
    show: {
      audit: { etag: (reply1?.performance?.audit?.etag ?? '') },
      location: { postal_code: (reply1?.performance?.location?.postal_code ?? '') },
    },
  };
  const reply3 = await bus.request('acme.shows.v1.LineupCalculationService/EstimateLineup:2', request3);
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      lineup: {
        billing_address: { postal_code: (reply1?.performance?.location?.postal_code ?? '') },
      },
      result: { details: (reply1?.result?.details ?? {}) },
    },
    {
      lineup: {
        billing_address: { country_code: (reply2?.show?.location?.country_code ?? '') },
      },
    },
    {
      lineup: { audit: { created_by: (reply3?.show?.audit?.created_by ?? '') } },
      result: { status: (reply3?.result?.status ?? '') },
    },
    {
      lineup: {
        counterparty: request?.lineup?.counterparty,
        lineup_id: (request?.lineup_id ?? ''),
      },
    },
  );
}

/**
 * Explains a lineup.
 */
async function explainLineupCalculation(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'ExplainLineupCalculation', request);
  logger.info('explain_lineup_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue: {
      address: {
        locality: (request?.lineup?.billing_address?.locality ?? ''),
        recipient: (request?.lineup?.billing_address?.recipient ?? ''),
      },
      audit: { etag: (request?.lineup?.audit?.etag ?? '') },
      owner: { display_name: (request?.lineup?.counterparty?.display_name ?? '') },
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => venuesVenueClient.createVenue(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      lineup: { audit: { created_by: (reply1?.venue?.audit?.created_by ?? '') } },
    },
    {
      lineup: {
        lineup_id: (request?.lineup_id ?? ''),
        total: request?.lineup?.total,
      },
    },
  );
}

/**
 * Returns a lineup.
 */
async function getLineupRates(request) {
  validateContext(request.context);
  if (!request.lineup_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'lineup_id is required');
  }
  await store.record(request.context.tenant_id, 'GetLineupRates', request);
  logger.info('get_lineup_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_policy: {
      audit: { created_at: request?.context?.deadline },
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const reply1 = await call((done) => clients.venuesVenuePolicyAdminClient.repairVenuePolicy(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  return assemble(
    {
      lineup: {
        lines: [{
          amount: (reply1?.venue_policy?.action === 'discount' ? reply1?.venue_policy.discount : undefined)?.amount,
          tax: {
            amount: (reply1?.venue_policy?.action === 'discount' ? reply1?.venue_policy.discount : undefined)?.amount,
          },
        }],
      },
    },
    { lineup: { lineup_id: (request?.lineup_id ?? '') } },
  );
}

/**
 * Quotes a lineup.
 */
async function quoteLineup(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'QuoteLineup', request);
  logger.info('quote_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  let selected1_2;
  switch (request?.lineup?.settlement) {
    case 'card':
      selected1_2 = ((request?.lineup?.settlement === 'card' ? request?.lineup.card : undefined)?.token ?? '');
      break;
    default:
      selected1_2 = '';
  }
  let selected1_3;
  switch (request?.lineup?.settlement) {
    case 'card':
      selected1_3 = ((request?.lineup?.settlement === 'card' ? request?.lineup.card : undefined)?.last4 ?? '');
      break;
    default:
      selected1_3 = '';
  }
  const request1 = {
    box_office_sale: {
      billing_address: { lines: [...(request?.lineup?.billing_address?.lines ?? [])] },
      card: {
        last4: selected1_3,
        token: selected1_2,
      },
    },
    context: request?.context,
  };
  const reply1 = await call((done) => boxofficeBoxOfficeSaleValidationClient.normalizeBoxOfficeSale(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    show: {
      audit: {
        created_at: reply1?.box_office_sale?.audit?.created_at,
        updated_by: (reply1?.box_office_sale?.audit?.updated_by ?? ''),
      },
      location: { locality: (request?.details?.destination?.locality ?? '') },
    },
  };
  const estimateShow2 = promisify(showsShowCalculationClient.estimateShow.bind(showsShowCalculationClient));
  const reply2 = await estimateShow2(request2, metadataFor(request2.context), { deadline: deadline() });
  const request3 = {
    context: request?.context,
    performance: {
      participants: [{ contact: { email: (reply2?.show?.organizer?.email ?? '') } }],
    },
  };
  const reply3 = await call((done) => showsPerformanceCalculationClient.simulatePerformance(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      lineup: {
        audit: { updated_at: reply1?.box_office_sale?.audit?.updated_at },
        billing_address: {
          recipient: (reply1?.box_office_sale?.billing_address?.recipient ?? ''),
        },
      },
    },
    {
      lineup: {
        billing_address: { postal_code: (reply2?.show?.location?.postal_code ?? '') },
      },
    },
    {
      lineup: {
        billing_address: { locality: (reply3?.performance?.location?.locality ?? '') },
      },
      result: { details: (reply3?.result?.details ?? {}) },
    },
    {
      breakdown: { total: request?.lineup?.total },
      lineup: { lineup_id: (request?.lineup_id ?? '') },
    },
  );
}

/**
 * Recalculates a lineup.
 */
async function recalculateLineups(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'RecalculateLineups', request);
  logger.info('recalculate_lineups', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    show: {
      audit: {
        etag: (request?.lineup?.audit?.etag ?? ''),
        updated_at: request?.lineup?.audit?.updated_at,
      },
    },
  };
  const method1 = 'calculateShow';
  const reply1 = await call((done) => showsShowCalculationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { destination: { lines: [...(reply1?.show?.location?.lines ?? [])] } },
    show: {
      participants: [{ contact: { email: (reply1?.show?.organizer?.email ?? '') } }],
    },
  };
  const client2 = clients.registry.get('acme.shows.v1.ShowCalculationService');
  const reply2 = await call((done) => client2.estimateShow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    show: { audit: { created_at: reply2?.show?.audit?.created_at } },
  };
  const send3 = showsShowValidationClient.scoreShow.bind(showsShowValidationClient);
  const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    {
      lineup: {
        counterparty: { display_name: (reply2?.show?.organizer?.display_name ?? '') },
      },
    },
    {
      lineup: {
        audit: { created_by: (reply3?.show?.audit?.created_by ?? '') },
        billing_address: { region: (reply3?.show?.location?.region ?? '') },
      },
    },
    {
      lineup: {
        lineup_id: (request?.lineup_id ?? ''),
        number: (request?.lineup?.number ?? ''),
      },
    },
  );
}

/**
 * Simulates a lineup.
 */
async function simulateLineup(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'SimulateLineup', request);
  logger.info('simulate_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => boxofficeBoxOfficeSaleQueryClient.resolveBoxOfficeSales(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        details: (reply1?.result?.details ?? {}),
      },
    },
    {
      lineup: {
        lineup_id: (request?.lineup_id ?? ''),
        status: (request?.lineup?.status ?? ''),
      },
    },
  );
}

module.exports = { calculateLineup, compareLineupOptions, estimateLineup, explainLineupCalculation, getLineupRates, quoteLineup, recalculateLineups, simulateLineup };
