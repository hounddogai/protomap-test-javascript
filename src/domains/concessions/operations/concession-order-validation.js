'use strict';

/**
 * Checks concession orders against the rules before anything acts on them. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const clients = require('../../../platform/clients');
const {
  auditAuditEventClient,
  broadcastsReplayClipCalculationClient,
  concessionsConcessionOrderCalculationClient,
  concessionsConcessionStandCalculationClient,
  concessionsConcessionStandValidationClient,
  featureflagsFeatureFlagQueryClient,
  identityUserValidationClient,
  searchSearchIndexQueryClient,
  sponsorshipsSponsorshipDealValidationClient,
  venuesVenuePolicyHistoryClient,
  venuesVenueSectionQueryClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Explains a concession order.
 */
async function explainConcessionOrderDecision(request) {
  validateContext(request.context);
  if (request.concession_order) {
    await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
  }
  await store.record(request.context.tenant_id, 'ExplainConcessionOrderDecision', request);
  logger.info('explain_concession_order_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.concession_order?.issued_at,
      region: (request?.concession_order?.billing_address?.region ?? ''),
    },
  };
  const messages1 = await collect(searchSearchIndexQueryClient.exportSearchIndexes(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    concession_order: {
      bank: {
        account_holder: ((request?.concession_order?.settlement === 'bank' ? request?.concession_order.bank : undefined)?.account_holder ?? ''),
        iban: ((request?.concession_order?.settlement === 'bank' ? request?.concession_order.bank : undefined)?.iban ?? ''),
      },
      billing_address: {
        recipient: (request?.concession_order?.billing_address?.recipient ?? ''),
      },
    },
    context: request?.context,
  };
  const reply2 = await new Promise((resolve, reject) => {
    concessionsConcessionOrderCalculationClient.estimateConcessionOrder(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request3 = {
    context: request?.context,
    user: {
      primary_address: {
        country_code: (request?.concession_order?.billing_address?.country_code ?? ''),
        postal_code: (request?.concession_order?.billing_address?.postal_code ?? ''),
      },
    },
  };
  const previewUser3 = promisify(identityUserValidationClient.previewUser.bind(identityUserValidationClient));
  const reply3 = await previewUser3(request3, metadataFor(request3.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply2?.result?.status ?? '') } },
    {
      concession_order: { audit: { created_at: reply3?.user?.audit?.created_at } },
    },
    {
      concession_order: {
        concession_order_id: (request?.concession_order_id ?? ''),
        counterparty: request?.concession_order?.counterparty,
      },
    },
  );
}

/**
 * Returns a concession order.
 */
async function getConcessionOrderRules(request) {
  validateContext(request.context);
  if (!request.concession_order_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
  }
  await store.record(request.context.tenant_id, 'GetConcessionOrderRules', request);
  logger.info('get_concession_order_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return {
    concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
  };
}

/**
 * Normalizes a concession order.
 */
async function normalizeConcessionOrder(request) {
  validateContext(request.context);
  if (request.concession_order) {
    await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
  }
  await store.record(request.context.tenant_id, 'NormalizeConcessionOrder', request);
  logger.info('normalize_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: {
        preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
      },
    },
  };
  const reply1 = await call((done) => concessionsConcessionStandValidationClient.checkConcessionStandEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    concession_stand: {
      address: {
        lines: [...(request?.concession_order?.billing_address?.lines ?? [])],
        recipient: (request?.concession_order?.billing_address?.recipient ?? ''),
      },
    },
    context: request?.context,
    details: {
      destination: {
        postal_code: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.postal_code ?? ''),
      },
    },
  };
  const method2 = 'scoreConcessionStand';
  const reply2 = await call((done) => concessionsConcessionStandValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const results3 = await Promise.all((request?.concession_order?.lines ?? []).map(async (item) => {
    const request3 = {
      context: request?.context,
      user: {
        attributes: { ...(item?.attributes ?? {}) },
        audit: { etag: (reply1?.concession_stand?.audit?.etag ?? '') },
        primary_address: {
          country_code: (request?.concession_order?.billing_address?.country_code ?? ''),
        },
        relationship_manager: { user_id: (reply1?.concession_stand?.owner?.user_id ?? '') },
      },
    };
    const reply3 = await call((done) => identityUserValidationClient.normalizeUser(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    return reply3;
  }));
  const reply3 = results3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      concession_order: { audit: { updated_at: reply1?.concession_stand?.audit?.updated_at } },
      result: { message: (reply1?.result?.message ?? '') },
    },
    {
      concession_order: {
        billing_address: {
          region: ((reply2?.concession_stand?.locator === 'address' ? reply2?.concession_stand.address : undefined)?.region ?? ''),
        },
      },
    },
    {
      concession_order: {
        concession_order_id: (request?.concession_order_id ?? ''),
        counterparty: request?.concession_order?.counterparty,
      },
    },
  );
}

/**
 * Previews a concession order.
 */
async function previewConcessionOrder(request) {
  validateContext(request.context);
  if (request.concession_order) {
    await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
  }
  await store.record(request.context.tenant_id, 'PreviewConcessionOrder', request);
  logger.info('preview_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    concession_stand: {
      audit: { updated_by: (request?.concession_order?.audit?.updated_by ?? '') },
    },
    context: request?.context,
  };
  const client1 = clients.registry.get('acme.concessions.v1.ConcessionStandValidationService');
  const reply1 = await call((done) => client1.explainConcessionStandDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    concession_order_id: (request?.concession_order_id ?? ''),
    context: request?.context,
  };
  const send2 = concessionsConcessionOrderCalculationClient.getConcessionOrderRates.bind(concessionsConcessionOrderCalculationClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    replay_clip: {
      address: {
        postal_code: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.postal_code ?? ''),
      },
    },
  };
  const spread3 = {
    ...request3,
    context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
  };
  const reply3 = await call((done) => broadcastsReplayClipCalculationClient.calculateReplayClip(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      concession_order: {
        billing_address: {
          recipient: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.recipient ?? ''),
        },
      },
    },
    {
      concession_order: {
        audit: { updated_by: (reply2?.concession_order?.audit?.updated_by ?? '') },
        counterparty: { code: (reply2?.concession_order?.counterparty?.code ?? '') },
      },
    },
    {
      concession_order: { audit: { updated_at: reply3?.replay_clip?.audit?.updated_at } },
    },
    {
      concession_order: {
        concession_order_id: (request?.concession_order_id ?? ''),
        notes: (request?.concession_order?.notes ?? ''),
      },
    },
  );
}

/**
 * Scores a concession order.
 */
async function scoreConcessionOrder(request) {
  validateContext(request.context);
  if (request.concession_order) {
    await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
  }
  await store.record(request.context.tenant_id, 'ScoreConcessionOrder', request);
  logger.info('score_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    details: { destination: { region: (input1?.details?.destination?.region ?? '') } },
  };
  const reply1 = await call((done) => concessionsConcessionStandCalculationClient.simulateConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    audit_event: {
      measurements: [{ unit: (reply1?.concession_stand?.dimensions?.unit ?? '') }],
      subject: {
        display_name: (request?.concession_order?.counterparty?.display_name ?? ''),
        number: (request?.concession_order?.number ?? ''),
      },
    },
    context: request?.context,
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => auditAuditEventClient.createAuditEvent(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  const settled3 = await Promise.allSettled((request?.concession_order?.lines ?? []).map(async (item) => {
    const request3 = {
      context: request?.context,
      sponsorship_deal: {
        lines: [{
          quantity: (item?.quantity ?? '0'),
          tax: {
            amount: item?.tax?.amount,
            jurisdiction: (item?.tax?.jurisdiction ?? ''),
            rate: (item?.tax?.rate ?? 0),
          },
        }],
      },
    };
    const reply3 = await call((done) => sponsorshipsSponsorshipDealValidationClient.explainSponsorshipDealDecision(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    return reply3;
  }));
  const results3 = settled3.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply3 = results3.at(-1) ?? {};
  const results4 = [];
  for (const item of (request?.concession_order?.lines ?? [])) {
    const request4 = {
      context: request?.context,
      filter: {
        labels: { ...(item?.attributes ?? {}) },
        owner_id: (item?.line_id ?? ''),
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const reply4 = await call((done) => featureflagsFeatureFlagQueryClient.countFeatureFlags(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    results4.push(reply4);
  }
  const reply4 = results4.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      breakdown: { currency_code: (reply1?.breakdown?.currency_code ?? '') },
    },
    {
      result: {
        message: (reply2?.result?.message ?? ''),
        status: (reply2?.result?.status ?? ''),
      },
    },
    {
      concession_order: {
        concession_order_id: (request?.concession_order_id ?? ''),
        total: request?.concession_order?.total,
      },
    },
  );
}

/**
 * Validates a concession order.
 */
async function validateConcessionOrder(request) {
  validateContext(request.context);
  if (request.concession_order) {
    await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
  }
  await store.record(request.context.tenant_id, 'ValidateConcessionOrder', request);
  logger.info('validate_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.concession_order?.audit?.updated_at,
      labels: { ...(request?.concession_order?.metadata ?? {}) },
      owner_id: (request?.concession_order?.counterparty?.menu_item_id ?? ''),
      region: (request?.concession_order?.billing_address?.region ?? ''),
    },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => venuesVenueSectionQueryClient.findVenueSections(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    venue_policy: {
      audit: { updated_by: (request?.concession_order?.audit?.updated_by ?? '') },
      owner: {
        display_name: (request?.concession_order?.counterparty?.display_name ?? ''),
        email: (request?.details?.contact?.email ?? ''),
      },
    },
  };
  const merged2 = Object.assign({}, request2, { context: request2.context });
  const reply2 = await call((done) => venuesVenuePolicyHistoryClient.recordVenuePolicyVersion(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { status: (reply1?.result?.status ?? '') },
      result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
    },
    {
      concession_order: {
        lines: [{
          tax: {
            amount: (reply2?.venue_policy?.action === 'discount' ? reply2?.venue_policy.discount : undefined)?.amount,
          },
        }],
      },
      result: { message: (reply2?.outcome?.message ?? '') },
    },
    {
      concession_order: {
        billing_address: request?.concession_order?.billing_address,
        concession_order_id: (request?.concession_order_id ?? ''),
      },
    },
  );
}

module.exports = { explainConcessionOrderDecision, getConcessionOrderRules, normalizeConcessionOrder, previewConcessionOrder, scoreConcessionOrder, validateConcessionOrder };
