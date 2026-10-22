'use strict';

/**
 * Checks sponsors against the rules before anything acts on them. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  auditAuditEventClient,
  authorizationAccessPolicyValidationClient,
  broadcastsBroadcastClient,
  identityUserQueryClient,
  identityUserValidationClient,
  sponsorshipsSponsorshipDealCalculationClient,
  venuesVenueExportClient,
  venuesVenuePolicyHistoryClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Checks a sponsor.
 */
async function checkSponsorEligibility(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'CheckSponsorEligibility', request);
  logger.info('check_sponsor_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.sponsor?.audit?.updated_at,
      owner_id: (request?.sponsor?.relationship_manager?.user_id ?? ''),
      region: (request?.sponsor?.primary_address?.region ?? ''),
    },
  };
  const reply1 = await call((done) => venuesVenuePolicyHistoryClient.listVenuePolicyVersions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      destination: { country_code: (request?.sponsor?.primary_address?.country_code ?? '') },
    },
    sponsorship_deal: {
      audit: { etag: (request?.sponsor?.audit?.etag ?? '') },
      billing_address: { region: (request?.sponsor?.primary_address?.region ?? '') },
      number: ((request?.sponsor?.identity_document === 'national_id' ? request?.sponsor.national_id : undefined)?.number ?? ''),
    },
  };
  const method2 = 'compareSponsorshipDealOptions';
  const reply2 = await call((done) => sponsorshipsSponsorshipDealCalculationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  let selected3_2;
  switch (request?.sponsor?.identity_document) {
    case 'national_id':
      selected3_2 = ((request?.sponsor?.identity_document === 'national_id' ? request?.sponsor.national_id : undefined)?.number ?? '');
      break;
    default:
      selected3_2 = '';
  }
  const request3 = {
    audit_event: {
      attributes: { ...(request?.sponsor?.attributes ?? {}) },
      subject: {
        display_name: (reply2?.sponsorship_deal?.counterparty?.display_name ?? ''),
        number: selected3_2,
      },
    },
    context: request?.context,
  };
  const reply3 = await call((done) => auditAuditEventClient.createAuditEvent(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { status: (reply1?.result?.status ?? '') },
      result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
    },
    {
      sponsor: {
        sponsor_id: (reply2?.sponsorship_deal?.counterparty?.sponsor_id ?? ''),
      },
    },
    { result: { correlation_id: (reply3?.result?.correlation_id ?? '') } },
    {
      sponsor: {
        contact: { name: request?.sponsor?.name },
        primary_address: request?.sponsor?.primary_address,
      },
    },
  );
}

/**
 * Explains a sponsor.
 */
async function explainSponsorDecision(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'ExplainSponsorDecision', request);
  logger.info('explain_sponsor_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      destination: { lines: [...(request?.sponsor?.primary_address?.lines ?? [])] },
    },
  };
  const send1 = sponsorshipsSponsorshipDealCalculationClient.quoteSponsorshipDeal.bind(sponsorshipsSponsorshipDealCalculationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    user_id: (request?.sponsor?.relationship_manager?.user_id ?? ''),
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => identityUserQueryClient.lookupUser(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    {
      sponsor: { preferences: { quiet_hours: reply2?.user?.preferences?.quiet_hours } },
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
 * Returns a sponsor.
 */
async function getSponsorRules(request) {
  validateContext(request.context);
  if (!request.sponsor_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsor_id is required');
  }
  await store.record(request.context.tenant_id, 'GetSponsorRules', request);
  logger.info('get_sponsor_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { sponsor: { sponsor_id: (request?.sponsor_id ?? '') } };
}

/**
 * Normalizes a sponsor.
 */
async function normalizeSponsor(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'NormalizeSponsor', request);
  logger.info('normalize_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  let selected1_2;
  switch (request?.sponsor?.identity_document) {
    case 'passport':
      selected1_2 = ((request?.sponsor?.identity_document === 'passport' ? request?.sponsor.passport : undefined)?.number ?? '');
      break;
    default:
      selected1_2 = '';
  }
  const request1 = {
    context: request?.context,
    details: {
      destination: { recipient: (request?.sponsor?.primary_address?.recipient ?? '') },
    },
    sponsorship_deal: {
      counterparty: { email: (request?.details?.contact?.email ?? '') },
      number: selected1_2,
    },
  };
  const reply1 = await call((done) => sponsorshipsSponsorshipDealCalculationClient.estimateSponsorshipDeal(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    broadcast: {
      audit: { updated_by: (reply1?.sponsorship_deal?.audit?.updated_by ?? '') },
      participants: [{
        contact: {
          email: (request?.sponsor?.relationship_manager?.email ?? ''),
          name: request?.details?.contact?.name,
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      }],
    },
    context: request?.context,
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => broadcastsBroadcastClient.updateBroadcast(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  const request3 = {
    access_policy: {
      audit: {
        etag: (reply2?.broadcast?.audit?.etag ?? ''),
        updated_by: (request?.sponsor?.audit?.updated_by ?? ''),
      },
      owner: { email: (request?.sponsor?.relationship_manager?.email ?? '') },
    },
    context: request?.context,
    details: {
      contact: {
        preferred_channel: (request?.sponsor?.contact?.preferred_channel ?? ''),
      },
    },
  };
  const copied3 = structuredClone(request3);
  const reply3 = await call((done) => authorizationAccessPolicyValidationClient.normalizeAccessPolicy(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    {
      sponsor: {
        relationship_manager: { email: (reply2?.broadcast?.organizer?.email ?? '') },
      },
    },
    {
      result: {
        correlation_id: (reply3?.result?.correlation_id ?? ''),
        message: (reply3?.result?.message ?? ''),
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
 * Previews a sponsor.
 */
async function previewSponsor(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'PreviewSponsor', request);
  logger.info('preview_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      destination: { lines: [...(request?.sponsor?.primary_address?.lines ?? [])] },
    },
    sponsorship_deal: { counterparty: { email: (request?.details?.contact?.email ?? '') } },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => sponsorshipsSponsorshipDealCalculationClient.quoteSponsorshipDeal(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      sponsor: {
        primary_address: {
          postal_code: (reply1?.sponsorship_deal?.billing_address?.postal_code ?? ''),
        },
      },
    },
    {
      sponsor: {
        contact: request?.sponsor?.contact,
        sponsor_id: (request?.sponsor_id ?? ''),
      },
    },
  );
}

/**
 * Scores a sponsor.
 */
async function scoreSponsor(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'ScoreSponsor', request);
  logger.info('score_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    user: {
      contact: {
        email: (request?.sponsor?.contact?.email ?? ''),
        phone: (request?.sponsor?.contact?.phone ?? ''),
      },
      national_id: {
        issuing_country: ((request?.sponsor?.identity_document === 'national_id' ? request?.sponsor.national_id : undefined)?.issuing_country ?? ''),
      },
      primary_address: { region: (request?.sponsor?.primary_address?.region ?? '') },
    },
  };
  const reply1 = await withRetry(() => call((done) => identityUserValidationClient.validateUser(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    sponsorship_deal: { audit: { updated_at: request?.sponsor?.audit?.updated_at } },
  };
  const task2 = deferred(() => call((done) => sponsorshipsSponsorshipDealCalculationClient.compareSponsorshipDealOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { sponsor: { contact: { email: (reply1?.user?.contact?.email ?? '') } } },
    { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
    {
      sponsor: {
        sponsor_id: (request?.sponsor_id ?? ''),
        status: (request?.sponsor?.status ?? ''),
      },
    },
  );
}

/**
 * Validates a sponsor.
 */
async function validateSponsor(request) {
  validateContext(request.context);
  if (request.sponsor) {
    await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
  }
  await store.record(request.context.tenant_id, 'ValidateSponsor', request);
  logger.info('validate_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsorship_deal: { counterparty: { email: (request?.details?.contact?.email ?? '') } },
  };
  const reply1 = await bus.request('acme.sponsorships.v1.SponsorValidationService/ValidateSponsor:0', request1);
  const request2 = {
    context: request?.context,
    details: { notify: { phone: (request?.details?.contact?.phone ?? '') } },
    venue: { audit: { etag: (reply1?.sponsorship_deal?.audit?.etag ?? '') } },
  };
  const reply2 = await runInContext(request.context, () => call((done) => venuesVenueExportClient.retryVenueExport(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const request3 = {
    context: request?.context,
    details: {
      destination: {
        postal_code: ((reply2?.venue?.locator === 'address' ? reply2?.venue.address : undefined)?.postal_code ?? ''),
        region: ((reply2?.venue?.locator === 'address' ? reply2?.venue.address : undefined)?.region ?? ''),
      },
    },
  };
  const reply3 = await call((done) => clients.sponsorshipsSponsorshipDealCalculationClient.recalculateSponsorshipDeals(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { status: (reply1?.result?.status ?? '') },
      sponsor: {
        sponsor_id: (reply1?.sponsorship_deal?.counterparty?.sponsor_id ?? ''),
      },
    },
    { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
    {
      sponsor: {
        primary_address: { region: (reply3?.sponsorship_deal?.billing_address?.region ?? '') },
        relationship_manager: { email: (reply3?.sponsorship_deal?.counterparty?.email ?? '') },
      },
    },
    {
      sponsor: {
        contact: request?.details?.contact,
        status: (request?.sponsor?.status ?? ''),
      },
    },
  );
}

module.exports = { checkSponsorEligibility, explainSponsorDecision, getSponsorRules, normalizeSponsor, previewSponsor, scoreSponsor, validateSponsor };
