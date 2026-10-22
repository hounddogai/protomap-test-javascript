'use strict';

/**
 * Computes the figures of a concession stand, such as totals, taxes, and estimates. Each operation validates,
 * records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  broadcastsBroadcastClient,
  broadcastsBroadcastValidationClient,
  broadcastsReplayClipCalculationClient,
  broadcastsReplayClipValidationClient,
  broadcastsStreamKeyQueryClient,
  documentsDocumentValidationClient,
  metricsMetricSeriesValidationClient,
  notificationsNotificationValidationClient,
  sponsorshipsActivationCalculationClient,
  sponsorshipsActivationValidationClient,
  sponsorshipsSponsorValidationClient,
  sponsorshipsSponsorshipDealValidationClient,
  venuesVenuePolicyHistoryClient,
  venuesVenuePolicyNotificationClient,
  venuesVenueSectionExportClient,
  venuesVenueValidationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Calculates a concession stand.
 */
async function calculateConcessionStand(request) {
  validateContext(request.context);
  if (request.concession_stand) {
    await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
  }
  await store.record(request.context.tenant_id, 'CalculateConcessionStand', request);
  logger.info('calculate_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsorship_deal: {
      billing_address: {
        locality: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.locality ?? ''),
      },
    },
  };
  const client1 = clients.registry.get('acme.sponsorships.v1.SponsorshipDealValidationService');
  const reply1 = await call((done) => client1.explainSponsorshipDealDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  let selected2_2;
  switch (request?.concession_stand?.locator) {
    case 'address':
      selected2_2 = ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.region ?? '');
      break;
    default:
      selected2_2 = '';
  }
  const request2 = {
    context: request?.context,
    replay_clip: {
      address: {
        country_code: (request?.details?.destination?.country_code ?? ''),
        lines: [...(request?.details?.destination?.lines ?? [])],
        region: selected2_2,
      },
    },
  };
  const reply2 = await call((done) => broadcastsReplayClipValidationClient.previewReplayClip(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    sponsor: {
      audit: { created_at: reply1?.sponsorship_deal?.audit?.created_at },
      primary_address: { region: (reply1?.sponsorship_deal?.billing_address?.region ?? '') },
    },
  };
  const spread3 = {
    ...request3,
    context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
  };
  const reply3 = await call((done) => sponsorshipsSponsorValidationClient.scoreSponsor(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.outcome?.message ?? '') } },
    {
      concession_stand: {
        capacity: (reply2?.replay_clip?.capacity ?? '0'),
        dimensions: { unit: (reply2?.replay_clip?.dimensions?.unit ?? '') },
      },
    },
    {
      concession_stand: { owner: { email: (reply3?.sponsor?.contact?.email ?? '') } },
    },
    {
      concession_stand: {
        code: (request?.concession_stand?.code ?? ''),
        concession_stand_id: (request?.concession_stand_id ?? ''),
      },
    },
  );
}

/**
 * Compares a concession stand.
 */
async function compareConcessionStandOptions(request) {
  validateContext(request.context);
  if (request.concession_stand) {
    await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
  }
  await store.record(request.context.tenant_id, 'CompareConcessionStandOptions', request);
  logger.info('compare_concession_stand_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    replay_clip: {
      address: {
        locality: ((input1?.concession_stand?.locator === 'address' ? input1?.concession_stand.address : undefined)?.locality ?? ''),
      },
    },
  };
  const reply1 = await call((done) => broadcastsReplayClipValidationClient.previewReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    activation: { audit: { created_at: reply1?.replay_clip?.audit?.created_at } },
    context: request?.context,
    details: {
      destination: {
        country_code: (request?.details?.destination?.country_code ?? ''),
        recipient: (request?.details?.destination?.recipient ?? ''),
        region: (request?.details?.destination?.region ?? ''),
      },
    },
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => sponsorshipsActivationValidationClient.scoreActivation(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      concession_stand: {
        dimensions: { length: (reply1?.replay_clip?.dimensions?.length ?? 0) },
        owner: { display_name: (reply1?.replay_clip?.owner?.display_name ?? '') },
      },
    },
    {
      concession_stand: {
        code: ((reply2?.activation?.origin === 'customer' ? reply2?.activation.customer : undefined)?.code ?? ''),
      },
      result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
    },
    {
      concession_stand: {
        concession_stand_id: (request?.concession_stand_id ?? ''),
        status: (request?.concession_stand?.status ?? ''),
      },
    },
  );
}

/**
 * Estimates a concession stand.
 */
async function estimateConcessionStand(request) {
  validateContext(request.context);
  if (request.concession_stand) {
    await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
  }
  await store.record(request.context.tenant_id, 'EstimateConcessionStand', request);
  logger.info('estimate_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: {
      relationship_manager: { display_name: (request?.concession_stand?.owner?.display_name ?? '') },
    },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => sponsorshipsSponsorValidationClient.previewSponsor(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      recipient: {
        email: (reply1?.sponsor?.relationship_manager?.email ?? ''),
        phone: (reply1?.sponsor?.contact?.phone ?? ''),
      },
    },
  };
  const merged2 = Object.assign({}, request2, { context: request2.context });
  const reply2 = await call((done) => venuesVenuePolicyNotificationClient.subscribeVenuePolicyUpdates(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.concession_stand?.audit?.updated_at,
      owner_id: (reply1?.sponsor?.relationship_manager?.user_id ?? ''),
      region: (reply1?.sponsor?.primary_address?.region ?? ''),
    },
  };
  const reply3 = await withRetry(() => call((done) => venuesVenueSectionExportClient.listVenueSectionExports(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  const request4 = {
    context: request?.context,
    metric_series: {
      audit: {
        updated_at: reply2?.venue_policy?.audit?.updated_at,
        updated_by: (request?.concession_stand?.audit?.updated_by ?? ''),
      },
      components: { primary: {
        component_id: (request?.concession_stand?.components?.primary?.component_id ?? ''),
      } },
      owner: { user_id: (request?.concession_stand?.owner?.user_id ?? '') },
    },
  };
  const task4 = deferred(() => call((done) => metricsMetricSeriesValidationClient.lintMetricSerieses(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  const reply4 = await task4();
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        message: (reply1?.outcome?.message ?? ''),
        status: (reply1?.outcome?.status ?? ''),
      },
    },
    {
      concession_stand: {
        components: { primary: { description: (reply2?.venue_policy?.description ?? '') } },
      },
      result: { status: (reply2?.result?.status ?? '') },
    },
    { result: { status: (reply3?.result?.status ?? '') } },
    {
      concession_stand: {
        name: (reply4?.metric_series?.name ?? ''),
        specs: (reply4?.metric_series?.specs ?? {}),
      },
    },
    {
      concession_stand: {
        capacity: (request?.concession_stand?.capacity ?? '0'),
        concession_stand_id: (request?.concession_stand_id ?? ''),
      },
    },
  );
}

/**
 * Explains a concession stand.
 */
async function explainConcessionStandCalculation(request) {
  validateContext(request.context);
  if (request.concession_stand) {
    await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
  }
  await store.record(request.context.tenant_id, 'ExplainConcessionStandCalculation', request);
  logger.info('explain_concession_stand_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    activation: { audit: { etag: (request?.concession_stand?.audit?.etag ?? '') } },
    context: request?.context,
    details: {
      destination: {
        lines: [...((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.lines ?? [])],
      },
    },
  };
  const reply1 = await bus.request('acme.concessions.v1.ConcessionStandCalculationService/ExplainConcessionStandCalculation:0', request1);
  const request2 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: { filters: { ...(reply1?.result?.details ?? {}) } },
    reason: (request?.reason ?? ''),
  };
  const reply2 = await runInContext(request.context, () => call((done) => broadcastsStreamKeyQueryClient.summarizeStreamKeys(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const request3 = {
    context: request?.context,
    sponsorship_deal: {
      audit: { created_at: reply1?.activation?.audit?.created_at },
      counterparty: { display_name: (reply1?.activation?.assignee?.display_name ?? '') },
    },
  };
  const reply3 = await call((done) => clients.sponsorshipsSponsorshipDealValidationClient.previewSponsorshipDeal(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.contact?.preferred_channel ?? '') });
  return assemble(
    {
      concession_stand: { owner: { user_id: (reply1?.activation?.assignee?.user_id ?? '') } },
      result: { status: (reply1?.result?.status ?? '') },
    },
    {
      outcome: { message: (reply2?.result?.message ?? '') },
      result: { details: (reply2?.result?.details ?? {}) },
    },
    {
      concession_stand: { audit: { created_at: reply3?.sponsorship_deal?.audit?.created_at } },
      outcome: { reference_id: (reply3?.outcome?.reference_id ?? '') },
    },
    {
      concession_stand: {
        code: (request?.concession_stand?.code ?? ''),
        concession_stand_id: (request?.concession_stand_id ?? ''),
      },
    },
  );
}

/**
 * Returns a concession stand.
 */
async function getConcessionStandRates(request) {
  validateContext(request.context);
  if (!request.concession_stand_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
  }
  await store.record(request.context.tenant_id, 'GetConcessionStandRates', request);
  logger.info('get_concession_stand_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { locale: (request?.context?.locale ?? '') },
    sponsorship_deal: {
      audit: { created_at: request?.context?.deadline },
      counterparty: { email: (request?.context?.actor?.email ?? '') },
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    sponsorshipsSponsorshipDealValidationClient.previewSponsorshipDeal(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    activation: { audit: { updated_at: reply1?.sponsorship_deal?.audit?.updated_at } },
    context: request?.context,
  };
  const normalizeActivation2 = promisify(sponsorshipsActivationValidationClient.normalizeActivation.bind(sponsorshipsActivationValidationClient));
  const reply2 = await normalizeActivation2(request2, metadataFor(request2.context), { deadline: deadline() });
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    {
      concession_stand: {
        audit: { created_at: reply2?.activation?.audit?.created_at },
        owner: { email: (reply2?.activation?.assignee?.email ?? '') },
      },
    },
    {
      concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
    },
  );
}

/**
 * Quotes a concession stand.
 */
async function quoteConcessionStand(request) {
  validateContext(request.context);
  if (request.concession_stand) {
    await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
  }
  await store.record(request.context.tenant_id, 'QuoteConcessionStand', request);
  logger.info('quote_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      destination: { postal_code: (request?.details?.destination?.postal_code ?? '') },
    },
    replay_clip: {
      components: { primary: {
        component_id: (request?.concession_stand?.components?.primary?.component_id ?? ''),
        description: (request?.concession_stand?.components?.primary?.description ?? ''),
      } },
    },
  };
  const reply1 = await call((done) => broadcastsReplayClipCalculationClient.calculateReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    broadcast: { audit: { created_at: request?.concession_stand?.audit?.created_at } },
    context: request?.context,
  };
  const method2 = 'createBroadcast';
  const reply2 = await call((done) => broadcastsBroadcastClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      concession_stand: {
        owner: { display_name: (reply1?.replay_clip?.owner?.display_name ?? '') },
      },
    },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    {
      concession_stand: {
        code: (request?.concession_stand?.code ?? ''),
        concession_stand_id: (request?.concession_stand_id ?? ''),
      },
    },
  );
}

/**
 * Recalculates a concession stand.
 */
async function recalculateConcessionStands(request) {
  validateContext(request.context);
  if (request.concession_stand) {
    await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
  }
  await store.record(request.context.tenant_id, 'RecalculateConcessionStands', request);
  logger.info('recalculate_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue: {
      address: { recipient: (request?.details?.destination?.recipient ?? '') },
      dimensions: { width: (request?.concession_stand?.dimensions?.width ?? 0) },
      owner: { user_id: (request?.concession_stand?.owner?.user_id ?? '') },
    },
  };
  const client1 = clients.registry.get('acme.venues.v1.VenueValidationService');
  const reply1 = await call((done) => client1.normalizeVenue(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    venue_policy: {
      audit: {
        etag: (request?.concession_stand?.audit?.etag ?? ''),
        updated_at: reply1?.venue?.audit?.updated_at,
      },
      owner: { display_name: (request?.concession_stand?.owner?.display_name ?? '') },
    },
  };
  const send2 = venuesVenuePolicyHistoryClient.annotateVenuePolicyVersion.bind(venuesVenuePolicyHistoryClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    document: {
      audit: {
        created_at: reply1?.venue?.audit?.created_at,
        updated_by: (request?.concession_stand?.audit?.updated_by ?? ''),
      },
      billing_address: {
        country_code: (request?.details?.destination?.country_code ?? ''),
        postal_code: (request?.details?.destination?.postal_code ?? ''),
      },
    },
  };
  const spread3 = {
    ...request3,
    context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
  };
  const reply3 = await call((done) => documentsDocumentValidationClient.normalizeDocument(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      concession_stand: {
        components: { primary: {
          component_id: (reply1?.venue?.components?.primary?.component_id ?? ''),
        } },
      },
    },
    {
      concession_stand: {
        audit: { created_by: (reply2?.venue_policy?.audit?.created_by ?? '') },
        owner: { email: (reply2?.venue_policy?.owner?.email ?? '') },
      },
    },
    {
      concession_stand: { audit: { created_by: (reply3?.document?.audit?.created_by ?? '') } },
      result: { status: (reply3?.result?.status ?? '') },
    },
    {
      concession_stand: {
        capacity: (request?.concession_stand?.capacity ?? '0'),
        concession_stand_id: (request?.concession_stand_id ?? ''),
      },
    },
  );
}

/**
 * Simulates a concession stand.
 */
async function simulateConcessionStand(request) {
  validateContext(request.context);
  if (request.concession_stand) {
    await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
  }
  await store.record(request.context.tenant_id, 'SimulateConcessionStand', request);
  logger.info('simulate_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    notification: {
      assignee: { email: (input1?.concession_stand?.owner?.email ?? '') },
      audit: {
        etag: (input1?.concession_stand?.audit?.etag ?? ''),
        updated_by: (input1?.concession_stand?.audit?.updated_by ?? ''),
      },
    },
  };
  const reply1 = await call((done) => notificationsNotificationValidationClient.explainNotificationDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    broadcast: {
      location: { country_code: (request?.details?.destination?.country_code ?? '') },
    },
    context: request?.context,
    details: { contact: { email: (request?.concession_stand?.owner?.email ?? '') } },
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => broadcastsBroadcastValidationClient.explainBroadcastDecision(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.concession_stand?.audit?.created_at,
      labels: { ...(reply2?.result?.details ?? {}) },
      owner_id: (request?.concession_stand?.owner?.user_id ?? ''),
      region: (reply2?.broadcast?.location?.region ?? ''),
    },
  };
  const messages3 = await collect(venuesVenueSectionExportClient.streamVenueSectionExport(request3, metadataFor(request3.context), { deadline: deadline() }));
  const reply3 = messages3.at(-1) ?? {};
  const request4 = {
    context: request?.context,
    filter: {
      created_after: reply1?.notification?.sla?.due_at,
      owner_id: ((reply1?.notification?.origin === 'customer' ? reply1?.notification.customer : undefined)?.notification_template_id ?? ''),
      region: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.region ?? ''),
    },
  };
  const copied4 = structuredClone(request4);
  const reply4 = await call((done) => broadcastsBroadcastClient.listBroadcasts(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      concession_stand: { audit: { updated_at: reply1?.notification?.audit?.updated_at } },
    },
    {
      concession_stand: {
        audit: { updated_at: reply2?.broadcast?.audit?.updated_at },
        owner: { display_name: (reply2?.broadcast?.organizer?.display_name ?? '') },
      },
    },
    { result: { message: (reply4?.result?.message ?? '') } },
    {
      concession_stand: {
        capacity: (request?.concession_stand?.capacity ?? '0'),
        concession_stand_id: (request?.concession_stand_id ?? ''),
      },
    },
  );
}

module.exports = { calculateConcessionStand, compareConcessionStandOptions, estimateConcessionStand, explainConcessionStandCalculation, getConcessionStandRates, quoteConcessionStand, recalculateConcessionStands, simulateConcessionStand };
