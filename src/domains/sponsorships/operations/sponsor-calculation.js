'use strict';

/**
 * Computes the figures of a sponsor, such as totals, taxes, and estimates. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  auditAuditEventClient,
  broadcastsBroadcastClient,
  broadcastsReplayClipCalculationClient,
  broadcastsStreamKeyCalculationClient,
  identityUserValidationClient,
  venuesVenuePolicyNotificationClient,
  venuesVenuePolicySyncClient,
  venuesVenuePolicyValidationClient,
  venuesVenueQueryClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Calculates a sponsor.
 */
async function calculateSponsor(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'CalculateSponsor', request);
  logger.info('calculate_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.sponsor?.audit?.created_at,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.sponsor?.relationship_manager?.user_id ?? ''),
      region: (request?.sponsor?.primary_address?.region ?? ''),
    },
  };
  const send1 = auditAuditEventClient.searchAuditEvents.bind(auditAuditEventClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    audit_event: {
      attributes: { ...(request?.sponsor?.attributes ?? {}) },
      subject: {
        number: ((request?.sponsor?.identity_document === 'national_id' ? request?.sponsor.national_id : undefined)?.number ?? ''),
      },
    },
    context: request?.context,
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => auditAuditEventClient.createAuditEvent(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  const { context: context3, ...body3 } = request;
  const input3 = { ...body3, context: context3 };
  const request3 = {
    context: input3?.context,
    details: {
      destination: { locality: (input3?.details?.destination?.locality ?? '') },
    },
    replay_clip: { address: { lines: [...(input3?.details?.destination?.lines ?? [])] } },
  };
  const reply3 = await call((done) => broadcastsReplayClipCalculationClient.compareReplayClipOptions(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    {
      result: { message: (reply2?.result?.message ?? '') },
      sponsor: {
        relationship_manager: { display_name: (reply2?.audit_event?.subject?.display_name ?? '') },
      },
    },
    {
      sponsor: {
        primary_address: {
          lines: ((reply3?.replay_clip?.locator === 'address' ? reply3?.replay_clip.address : undefined)?.lines ?? []),
        },
        relationship_manager: { user_id: (reply3?.replay_clip?.owner?.user_id ?? '') },
      },
    },
    {
      sponsor: {
        name: request?.sponsor?.name,
        sponsor_id: (request?.sponsor_id ?? ''),
      },
    },
  );
}

/**
 * Compares a sponsor.
 */
async function compareSponsorOptions(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'CompareSponsorOptions', request);
  logger.info('compare_sponsor_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.sponsor?.audit?.updated_at,
      labels: { ...(request?.sponsor?.attributes ?? {}) },
      owner_id: (request?.sponsor?.relationship_manager?.user_id ?? ''),
      region: (request?.details?.destination?.region ?? ''),
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => venuesVenuePolicySyncClient.listVenuePolicyConflicts(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    user: {
      audit: { created_by: (request?.sponsor?.audit?.created_by ?? '') },
      primary_address: {
        country_code: (request?.details?.destination?.country_code ?? ''),
        lines: [...(request?.details?.destination?.lines ?? [])],
        region: (request?.details?.destination?.region ?? ''),
      },
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => identityUserValidationClient.previewUser(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    {
      sponsor: { audit: { updated_by: (reply2?.user?.audit?.updated_by ?? '') } },
    },
    {
      sponsor: {
        sponsor_id: (request?.sponsor_id ?? ''),
        status: (request?.sponsor?.status ?? ''),
      },
    },
  );
}

/**
 * Estimates a sponsor.
 */
async function estimateSponsor(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'EstimateSponsor', request);
  logger.info('estimate_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_policy: { owner: { email: (request?.sponsor?.contact?.email ?? '') } },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => venuesVenuePolicyValidationClient.checkVenuePolicyEligibility(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    stream_key: {
      audit: {
        created_at: request?.sponsor?.audit?.created_at,
        updated_at: request?.sponsor?.audit?.updated_at,
      },
      contact: { email: (request?.sponsor?.contact?.email ?? '') },
    },
  };
  const reply2 = await withRetry(() => call((done) => broadcastsStreamKeyCalculationClient.quoteStreamKey(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.sponsor?.audit?.updated_at,
      labels: { ...(reply2?.stream_key?.labels ?? {}) },
      owner_id: (reply1?.outcome?.reference_id ?? ''),
      region: (request?.details?.destination?.region ?? ''),
    },
  };
  const task3 = deferred(() => call((done) => auditAuditEventClient.listAuditEvents(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  const reply3 = await task3();
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      sponsor: {
        relationship_manager: { email: (reply1?.venue_policy?.owner?.email ?? '') },
      },
    },
    { result: { status: (reply2?.result?.status ?? '') } },
    {
      result: {
        correlation_id: (reply3?.result?.correlation_id ?? ''),
        reference_ids: (reply3?.result?.reference_ids ?? []),
      },
    },
    {
      breakdown: { currency_code: (request?.details?.currency_code ?? '') },
      sponsor: { sponsor_id: (request?.sponsor_id ?? '') },
    },
  );
}

/**
 * Explains a sponsor.
 */
async function explainSponsorCalculation(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'ExplainSponsorCalculation', request);
  logger.info('explain_sponsor_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { contact: { email: (request?.details?.contact?.email ?? '') } },
  };
  const reply1 = await bus.request('acme.sponsorships.v1.SponsorCalculationService/ExplainSponsorCalculation:0', request1);
  const request2 = {
    context: request?.context,
    subscriber: {
      email: (reply1?.venue_policy?.owner?.email ?? ''),
      name: request?.sponsor?.contact?.name,
    },
  };
  const messages2 = await collect(venuesVenuePolicyNotificationClient.streamVenuePolicyNotices(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        reference_ids: (reply1?.result?.reference_ids ?? []),
        status: (reply1?.outcome?.status ?? ''),
      },
    },
    {
      sponsor: {
        sponsor_id: (request?.sponsor_id ?? ''),
        status: (request?.sponsor?.status ?? ''),
      },
    },
  );
}

/**
 * Returns a sponsor.
 */
async function getSponsorRates(request) {
  validateContext(request.context);
  if (!request.sponsor_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsor_id is required');
  }
  await store.record(request.context.tenant_id, 'GetSponsorRates', request);
  logger.info('get_sponsor_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { sponsor: { sponsor_id: (request?.sponsor_id ?? '') } };
}

/**
 * Quotes a sponsor.
 */
async function quoteSponsor(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'QuoteSponsor', request);
  logger.info('quote_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      destination: { region: (request?.sponsor?.primary_address?.region ?? '') },
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => broadcastsStreamKeyCalculationClient.calculateStreamKey(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    details: { contact: { email: (reply1?.stream_key?.contact?.email ?? '') } },
    replay_clip: {
      address: { region: (request?.details?.destination?.region ?? '') },
      audit: { created_at: reply1?.stream_key?.audit?.created_at },
    },
  };
  const reply2 = await call((done) => clients.broadcastsReplayClipValidationClient.normalizeReplayClip(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    venue_policy: {
      audit: {
        created_by: (request?.sponsor?.audit?.created_by ?? ''),
        etag: (request?.sponsor?.audit?.etag ?? ''),
        updated_at: reply2?.replay_clip?.audit?.updated_at,
      },
      owner: { email: (request?.sponsor?.relationship_manager?.email ?? '') },
    },
  };
  const reply3 = await new Promise((resolve, reject) => {
    venuesVenuePolicyValidationClient.previewVenuePolicy(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      sponsor: { audit: { created_at: reply1?.stream_key?.audit?.created_at } },
    },
    {
      sponsor: {
        primary_address: {
          region: ((reply2?.replay_clip?.locator === 'address' ? reply2?.replay_clip.address : undefined)?.region ?? ''),
        },
      },
    },
    {
      breakdown: {
        lines: [{
          amount: (reply3?.venue_policy?.action === 'discount' ? reply3?.venue_policy.discount : undefined)?.amount,
        }],
      },
      result: { correlation_id: (reply3?.result?.correlation_id ?? '') },
    },
    {
      sponsor: {
        relationship_manager: request?.sponsor?.relationship_manager,
        sponsor_id: (request?.sponsor_id ?? ''),
      },
    },
  );
}

/**
 * Recalculates a sponsor.
 */
async function recalculateSponsors(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'RecalculateSponsors', request);
  logger.info('recalculate_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    broadcast: {
      participants: [{ contact: { phone: (request?.sponsor?.contact?.phone ?? '') } }],
    },
    context: request?.context,
  };
  const createBroadcast1 = promisify(broadcastsBroadcastClient.createBroadcast.bind(broadcastsBroadcastClient));
  const reply1 = await createBroadcast1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = { context: request?.context };
  const reply2 = await call((done) => auditAuditEventClient.batchGetAuditEvents(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.sponsor?.audit?.created_at,
      owner_id: (request?.sponsor?.relationship_manager?.user_id ?? ''),
      region: (reply1?.broadcast?.location?.region ?? ''),
    },
  };
  const method3 = 'suggestVenues';
  const reply3 = await call((done) => venuesVenueQueryClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { details: (reply1?.result?.details ?? {}) },
      sponsor: {
        relationship_manager: { email: (reply1?.broadcast?.organizer?.email ?? '') },
      },
    },
    { result: { details: (reply2?.result?.details ?? {}) } },
    {
      result: {
        correlation_id: (reply3?.result?.correlation_id ?? ''),
        message: (reply3?.result?.message ?? ''),
      },
    },
    {
      sponsor: {
        attributes: (request?.sponsor?.attributes ?? {}),
        sponsor_id: (request?.sponsor_id ?? ''),
      },
    },
  );
}

/**
 * Simulates a sponsor.
 */
async function simulateSponsor(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'SimulateSponsor', request);
  logger.info('simulate_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.sponsor?.audit?.updated_at,
      owner_id: (request?.sponsor?.relationship_manager?.user_id ?? ''),
      region: (request?.details?.destination?.region ?? ''),
    },
  };
  const client1 = clients.registry.get('acme.broadcasts.v1.BroadcastService');
  const reply1 = await call((done) => client1.listBroadcasts(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        message: (reply1?.result?.message ?? ''),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    },
    {
      sponsor: {
        contact: { name: request?.sponsor?.name },
        sponsor_id: (request?.sponsor_id ?? ''),
      },
    },
  );
}

module.exports = { calculateSponsor, compareSponsorOptions, estimateSponsor, explainSponsorCalculation, getSponsorRates, quoteSponsor, recalculateSponsors, simulateSponsor };
