'use strict';

/**
 * Checks sponsorship deals against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of SponsorshipDealValidationService, over the dependencies the sponsorships team is given.
 */
function createSponsorshipDealValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks a sponsorship deal.
   */
  async function checkSponsorshipDealEligibility(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'CheckSponsorshipDealEligibility', request);
    logger.info('check_sponsorship_deal_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      subscriber: {
        preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
      },
    };
    const messages1 = await collect(clients.broadcastsBroadcastClient.watchBroadcasts(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      stream_key: { contact: { phone: (request?.details?.contact?.phone ?? '') } },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.broadcastsStreamKeyValidationClient.validateStreamKey(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        sponsorship_deal: {
          billing_address: request?.sponsorship_deal?.billing_address,
          sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
        },
      },
    );
  }

  /**
   * Explains a sponsorship deal.
   */
  async function explainSponsorshipDealDecision(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'ExplainSponsorshipDealDecision', request);
    logger.info('explain_sponsorship_deal_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      activation: {
        audit: { updated_at: input1?.sponsorship_deal?.audit?.updated_at },
        requester: { email: (input1?.sponsorship_deal?.counterparty?.email ?? '') },
      },
      context: input1?.context,
      details: {
        destination: {
          lines: [...(input1?.sponsorship_deal?.billing_address?.lines ?? [])],
          locality: (input1?.sponsorship_deal?.billing_address?.locality ?? ''),
        },
      },
    };
    const reply1 = await call((done) => clients.sponsorshipsActivationCalculationClient.calculateActivation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        destination: { lines: [...(request?.sponsorship_deal?.billing_address?.lines ?? [])] },
      },
      sponsorship_deal: {
        audit: { updated_at: request?.sponsorship_deal?.audit?.updated_at },
        bank: {
          bic: ((request?.sponsorship_deal?.settlement === 'bank' ? request?.sponsorship_deal.bank : undefined)?.bic ?? ''),
        },
        counterparty: {
          sponsor_id: (request?.sponsorship_deal?.counterparty?.sponsor_id ?? ''),
        },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.sponsorshipsSponsorshipDealCalculationClient.estimateSponsorshipDeal(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        sponsorship_deal: { status: (reply2?.sponsorship_deal?.status ?? '') },
      },
      {
        sponsorship_deal: {
          notes: (request?.sponsorship_deal?.notes ?? ''),
          sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a sponsorship deal.
   */
  async function getSponsorshipDealRules(request) {
    validateContext(request.context);
    if (!request.sponsorship_deal_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsorship_deal_id is required');
    }
    await store.record(request.context.tenant_id, 'GetSponsorshipDealRules', request);
    logger.info('get_sponsorship_deal_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        audit: { created_at: request?.context?.deadline },
        contact: { email: (request?.context?.actor?.email ?? '') },
        relationship_manager: { user_id: (request?.context?.actor?.user_id ?? '') },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.sponsorshipsSponsorValidationClient.scoreSponsor(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: (reply1?.sponsor?.identity_document === 'passport' ? reply1?.sponsor.passport : undefined)?.expires_at,
        region: (reply1?.sponsor?.primary_address?.region ?? ''),
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.ledgerLedgerAccountQueryClient.countLedgerAccounts(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    return assemble(
      {
        sponsorship_deal: { counterparty: { email: (reply1?.sponsor?.contact?.email ?? '') } },
      },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      {
        sponsorship_deal: { sponsorship_deal_id: (request?.sponsorship_deal_id ?? '') },
      },
    );
  }

  /**
   * Lints a sponsorship deal.
   */
  async function lintSponsorshipDeals(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'LintSponsorshipDeals', request);
    logger.info('lint_sponsorship_deals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
    };
    const reply1 = await withRetry(() => call((done) => clients.sponsorshipsSponsorshipDealCalculationClient.getSponsorshipDealRates(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      sponsor: {
        audit: { created_by: (request?.sponsorship_deal?.audit?.created_by ?? '') },
        primary_address: { locality: (reply1?.sponsorship_deal?.billing_address?.locality ?? '') },
      },
    };
    const task2 = deferred(() => call((done) => clients.sponsorshipsSponsorCalculationClient.calculateSponsor(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsorship_deal: {
          audit: { etag: (reply1?.sponsorship_deal?.audit?.etag ?? '') },
          sponsorship_deal_id: (reply1?.sponsorship_deal?.sponsorship_deal_id ?? ''),
        },
      },
      {
        sponsorship_deal: { audit: { updated_by: (reply2?.sponsor?.audit?.updated_by ?? '') } },
      },
      {
        sponsorship_deal: {
          counterparty: request?.sponsorship_deal?.counterparty,
          number: (request?.sponsorship_deal?.number ?? ''),
        },
      },
    );
  }

  /**
   * Normalizes a sponsorship deal.
   */
  async function normalizeSponsorshipDeal(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'NormalizeSponsorshipDeal', request);
    logger.info('normalize_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      activation: {
        comments: [{ created_at: request?.sponsorship_deal?.audit?.created_at }],
        requester: { email: (request?.details?.contact?.email ?? '') },
      },
      context: request?.context,
    };
    const reply1 = await bus.request('acme.sponsorships.v1.SponsorshipDealValidationService/NormalizeSponsorshipDeal:0', request1);
    const request2 = {
      context: request?.context,
      details: { notify: { email: (request?.details?.contact?.email ?? '') } },
      venue_policy: { audit: { updated_by: (reply1?.activation?.audit?.updated_by ?? '') } },
    };
    const reply2 = await runInContext(request.context, () => call((done) => clients.venuesVenuePolicyExportClient.retryVenuePolicyExport(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    let selected3_1;
    switch (reply2?.venue_policy?.action) {
      case 'block':
        selected3_1 = ((reply2?.venue_policy?.action === 'block' ? reply2?.venue_policy.block : undefined)?.reason ?? '');
        break;
      default:
        selected3_1 = '';
    }
    const request3 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      reason: selected3_1,
    };
    const reply3 = await call((done) => clients.broadcastsStreamKeyQueryClient.summarizeStreamKeys(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      filter: {
        created_after: reply2?.venue_policy?.audit?.updated_at,
        region: (request?.sponsorship_deal?.billing_address?.region ?? ''),
      },
    };
    const reply4 = await new Promise((resolve, reject) => {
      clients.broadcastsBroadcastQueryClient.suggestBroadcasts(request4, metadataFor(request4.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { status: (reply1?.result?.status ?? '') },
        sponsorship_deal: { audit: { created_by: (reply1?.activation?.audit?.created_by ?? '') } },
      },
      { result: { status: (reply2?.outcome?.status ?? '') } },
      {
        outcome: { status: (reply3?.result?.status ?? '') },
        result: { status: (reply3?.result?.status ?? '') },
      },
      {
        result: {
          message: (reply4?.result?.message ?? ''),
          status: (reply4?.result?.status ?? ''),
        },
      },
      {
        sponsorship_deal: {
          sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
          total: request?.sponsorship_deal?.total,
        },
      },
    );
  }

  /**
   * Previews a sponsorship deal.
   */
  async function previewSponsorshipDeal(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'PreviewSponsorshipDeal', request);
    logger.info('preview_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: {
        participants: [{
          contact: {
            name: request?.details?.contact?.name,
            phone: (request?.details?.contact?.phone ?? ''),
          },
        }],
      },
      context: request?.context,
      details: {
        contact: { email: (request?.sponsorship_deal?.counterparty?.email ?? '') },
      },
    };
    const checkBroadcastEligibility1 = promisify(clients.broadcastsBroadcastValidationClient.checkBroadcastEligibility.bind(clients.broadcastsBroadcastValidationClient));
    const reply1 = await checkBroadcastEligibility1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      details: {
        destination: {
          lines: [...(reply1?.broadcast?.location?.lines ?? [])],
          locality: (reply1?.broadcast?.location?.locality ?? ''),
        },
      },
      sponsor: {
        primary_address: { region: (request?.sponsorship_deal?.billing_address?.region ?? '') },
      },
    };
    const reply2 = await call((done) => clients.sponsorshipsSponsorCalculationClient.quoteSponsor(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: {
        destination: {
          postal_code: (request?.sponsorship_deal?.billing_address?.postal_code ?? ''),
        },
      },
      sponsorship_deal: {
        billing_address: { lines: [...(reply1?.broadcast?.location?.lines ?? [])] },
        counterparty: { email: (request?.details?.contact?.email ?? '') },
      },
    };
    const method3 = 'compareSponsorshipDealOptions';
    const reply3 = await call((done) => clients.sponsorshipsSponsorshipDealCalculationClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { sponsorship_deal: { metadata: (reply1?.broadcast?.metadata ?? {}) } },
      {
        sponsorship_deal: {
          billing_address: { locality: (reply2?.sponsor?.primary_address?.locality ?? '') },
        },
      },
      {
        result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
        sponsorship_deal: {
          billing_address: { lines: (reply3?.sponsorship_deal?.billing_address?.lines ?? []) },
        },
      },
      {
        sponsorship_deal: {
          number: (request?.sponsorship_deal?.number ?? ''),
          sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
        },
      },
    );
  }

  /**
   * Scores a sponsorship deal.
   */
  async function scoreSponsorshipDeal(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'ScoreSponsorshipDeal', request);
    logger.info('score_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.sponsorship_deal?.billing_address?.region ?? '') },
    };
    const messages1 = await collect(clients.venuesVenueNotificationClient.streamVenueNotices(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      sponsorship_deal: {
        audit: {
          created_at: request?.sponsorship_deal?.audit?.created_at,
          created_by: (request?.sponsorship_deal?.audit?.created_by ?? ''),
        },
      },
    };
    const client2 = clients.registry.get('acme.sponsorships.v1.SponsorshipDealCalculationService');
    const reply2 = await call((done) => client2.calculateSponsorshipDeal(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const results3 = await Promise.all((request?.sponsorship_deal?.lines ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        sponsorship_deal: {
          lines: [{
            tax: {
              jurisdiction: (item?.tax?.jurisdiction ?? ''),
              rate: (item?.tax?.rate ?? 0),
            },
          }],
        },
      };
      const reply3 = await call((done) => clients.sponsorshipsSponsorshipDealCalculationClient.quoteSponsorshipDeal(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        sponsorship_deal: {
          audit: { updated_by: (reply2?.sponsorship_deal?.audit?.updated_by ?? '') },
        },
      },
      {
        sponsorship_deal: {
          due_at: request?.sponsorship_deal?.due_at,
          sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
        },
      },
    );
  }

  /**
   * Validates a sponsorship deal.
   */
  async function validateSponsorshipDeal(request) {
    validateContext(request.context);
    if (request.sponsorship_deal) {
      await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
    }
    await store.record(request.context.tenant_id, 'ValidateSponsorshipDeal', request);
    logger.info('validate_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        recipient: {
          email: (request?.details?.contact?.email ?? ''),
          phone: (request?.details?.contact?.phone ?? ''),
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const send1 = clients.venuesVenueNotificationClient.subscribeVenueUpdates.bind(clients.venuesVenueNotificationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
      stream_key: {
        audit: { created_by: (reply1?.venue?.audit?.created_by ?? '') },
        bank: {
          account_holder: ((request?.sponsorship_deal?.settlement === 'bank' ? request?.sponsorship_deal.bank : undefined)?.account_holder ?? ''),
        },
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.broadcastsStreamKeyValidationClient.lintStreamKeys(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      context: input3?.context,
      filter: {
        labels: { ...(reply2?.stream_key?.labels ?? {}) },
        region: (input3?.sponsorship_deal?.billing_address?.region ?? ''),
      },
    };
    const reply3 = await call((done) => clients.broadcastsBroadcastClient.searchBroadcasts(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      notification: { assignee: { email: (reply2?.stream_key?.contact?.email ?? '') } },
    };
    const cloned4 = JSON.parse(JSON.stringify(request4));
    const reply4 = await call((done) => clients.notificationsNotificationValidationClient.validateNotification(cloned4, metadataFor(cloned4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') },
        sponsorship_deal: {
          billing_address: {
            recipient: ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.recipient ?? ''),
          },
        },
      },
      {
        outcome: { effective_at: reply2?.outcome?.effective_at },
        sponsorship_deal: { counterparty: { email: (reply2?.stream_key?.contact?.email ?? '') } },
      },
      { result: { details: (reply3?.result?.details ?? {}) } },
      { outcome: { message: (reply4?.outcome?.message ?? '') } },
      {
        sponsorship_deal: {
          sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
          status: (request?.sponsorship_deal?.status ?? ''),
        },
      },
    );
  }

  return {
    checkSponsorshipDealEligibility: unary(checkSponsorshipDealEligibility),
    explainSponsorshipDealDecision: unary(explainSponsorshipDealDecision),
    getSponsorshipDealRules: unary(getSponsorshipDealRules),
    lintSponsorshipDeals: unary(lintSponsorshipDeals),
    normalizeSponsorshipDeal: unary(normalizeSponsorshipDeal),
    previewSponsorshipDeal: unary(previewSponsorshipDeal),
    scoreSponsorshipDeal: unary(scoreSponsorshipDeal),
    validateSponsorshipDeal: unary(validateSponsorshipDeal),
  };
}

module.exports = { createSponsorshipDealValidationHandlers };
