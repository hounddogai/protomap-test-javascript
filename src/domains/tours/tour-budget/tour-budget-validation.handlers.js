'use strict';

/**
 * Checks tour budgets against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of TourBudgetValidationService, over the dependencies the tours team is given.
 */
function createTourBudgetValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks a tour budget.
   */
  async function checkTourBudgetEligibility(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await store.record(request.context.tenant_id, 'CheckTourBudgetEligibility', request);
    logger.info('check_tour_budget_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.tour_budget?.audit?.created_at,
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        owner_id: (request?.tour_budget?.holder?.venue_id ?? ''),
      },
    };
    const send1 = clients.venuesVenueHistoryClient.listVenueVersions.bind(clients.venuesVenueHistoryClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      admission: {
        subject: { display_name: (request?.tour_budget?.holder?.display_name ?? '') },
      },
      context: request?.context,
      details: { currency_code: (request?.tour_budget?.currency_code ?? '') },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.admissionsAdmissionCalculationClient.calculateAdmission(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        tour_budget: {
          holder: { display_name: (reply2?.admission?.subject?.display_name ?? '') },
        },
      },
      {
        tour_budget: {
          contact: request?.details?.contact,
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Explains a tour budget.
   */
  async function explainTourBudgetDecision(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await store.record(request.context.tenant_id, 'ExplainTourBudgetDecision', request);
    logger.info('explain_tour_budget_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      filter: {
        created_after: input1?.tour_budget?.audit?.created_at,
        labels: { ...(input1?.tour_budget?.labels ?? {}) },
        owner_id: (input1?.tour_budget?.tour_budget_id ?? ''),
      },
    };
    const reply1 = await call((done) => clients.venuesVenueSectionSyncClient.listVenueSectionConflicts(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { filters: { ...(reply1?.result?.details ?? {}) } },
      reason: (request?.reason ?? ''),
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.sponsorshipsSponsorshipDealQueryClient.summarizeSponsorshipDeals(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.locale ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
      },
      {
        tour_budget: {
          contact: request?.details?.contact,
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a tour budget.
   */
  async function getTourBudgetRules(request) {
    validateContext(request.context);
    if (!request.tour_budget_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tour_budget_id is required');
    }
    await store.record(request.context.tenant_id, 'GetTourBudgetRules', request);
    logger.info('get_tour_budget_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.tour_budget_id ?? '') },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.sponsorshipsSponsorshipDealQueryClient.findSponsorshipDeals(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { filters: { ...(reply1?.result?.details ?? {}) } },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.broadcastsBroadcastQueryClient.summarizeBroadcasts(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          message: (reply2?.result?.message ?? ''),
        },
      },
      { tour_budget: { tour_budget_id: (request?.tour_budget_id ?? '') } },
    );
  }

  /**
   * Lints a tour budget.
   */
  async function lintTourBudgets(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await store.record(request.context.tenant_id, 'LintTourBudgets', request);
    logger.info('lint_tour_budgets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.tour_budget?.limits ?? [])) {
      const request1 = {
        concession_order: { lines: [{ tax: { amount: item?.amount } }] },
        context: request?.context,
      };
      const reply1 = await call((done) => clients.concessionsConcessionOrderCalculationClient.compareConcessionOrderOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      sponsor: {
        relationship_manager: {
          display_name: (reply1?.concession_order?.counterparty?.display_name ?? ''),
        },
      },
    };
    const reply2 = await withRetry(() => call((done) => clients.sponsorshipsSponsorCalculationClient.estimateSponsor(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: (reply2?.sponsor?.identity_document === 'passport' ? reply2?.sponsor.passport : undefined)?.expires_at,
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        owner_id: (reply1?.concession_order?.counterparty?.menu_item_id ?? ''),
        region: (reply1?.concession_order?.billing_address?.region ?? ''),
      },
    };
    const task3 = deferred(() => call((done) => clients.authorizationAccessPolicyQueryClient.countAccessPolicies(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    const request4 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        region: (reply2?.sponsor?.primary_address?.region ?? ''),
      },
    };
    const reply4 = await bus.request('acme.tours.v1.TourBudgetValidationService/LintTourBudgets:3', request4);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        tour_budget: { audit: { updated_by: (reply2?.sponsor?.audit?.updated_by ?? '') } },
      },
      {
        result: {
          correlation_id: (reply3?.result?.correlation_id ?? ''),
          reference_ids: (reply3?.result?.reference_ids ?? []),
        },
      },
      { outcome: { message: (reply4?.result?.message ?? '') } },
      {
        tour_budget: {
          contact: request?.tour_budget?.contact,
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Normalizes a tour budget.
   */
  async function normalizeTourBudget(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await store.record(request.context.tenant_id, 'NormalizeTourBudget', request);
    logger.info('normalize_tour_budget', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.tour_budget?.audit?.updated_at,
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        owner_id: (request?.tour_budget?.holder?.venue_id ?? ''),
      },
    };
    const messages1 = await collect(clients.broadcastsBroadcastQueryClient.exportBroadcasts(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            audit: {
              created_at: request?.tour_budget?.audit?.created_at,
              created_by: (request?.tour_budget?.audit?.created_by ?? ''),
            },
          }],
        }],
      },
    };
    const reply2 = await runInContext(request.context, () => call((done) => clients.venuesVenueSectionSyncClient.pushVenueSection(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply2?.outcome?.message ?? '') },
        tour_budget: { audit: { updated_at: reply2?.venue_section?.audit?.updated_at } },
      },
      {
        tour_budget: {
          holder: request?.tour_budget?.holder,
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Previews a tour budget.
   */
  async function previewTourBudget(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await store.record(request.context.tenant_id, 'PreviewTourBudget', request);
    logger.info('preview_tour_budget', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: {
        audit: {
          created_at: request?.tour_budget?.audit?.created_at,
          etag: (request?.tour_budget?.audit?.etag ?? ''),
          updated_at: request?.tour_budget?.audit?.updated_at,
        },
        owner: { email: (request?.tour_budget?.contact?.email ?? '') },
      },
    };
    const reply1 = await call((done) => clients.concessionsMenuItemCalculationClient.calculateMenuItem(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        tour_budget: { audit: { updated_at: reply1?.menu_item?.audit?.updated_at } },
      },
      {
        tour_budget: {
          status: (request?.tour_budget?.status ?? ''),
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Scores a tour budget.
   */
  async function scoreTourBudget(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await store.record(request.context.tenant_id, 'ScoreTourBudget', request);
    logger.info('score_tour_budget', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.broadcastsStreamKeyQueryClient.suggestStreamKeys(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    let selected2_1;
    switch (request?.tour_budget?.funding) {
      case 'bank':
        selected2_1 = ((request?.tour_budget?.funding === 'bank' ? request?.tour_budget.bank : undefined)?.iban ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      details: {
        destination: { locality: (request?.details?.destination?.locality ?? '') },
      },
      sponsorship_deal: {
        audit: { created_by: (request?.tour_budget?.audit?.created_by ?? '') },
        bank: { iban: selected2_1 },
      },
    };
    const reply2 = await call((done) => clients.sponsorshipsSponsorshipDealCalculationClient.recalculateSponsorshipDeals(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const results3 = await Promise.all((request?.tour_budget?.limits ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        venue_section: {
          children: [{ children: [{ children: [{ name: (item?.name ?? '') }] }] }],
        },
      };
      const reply3 = await call((done) => clients.venuesVenueSectionSyncClient.reconcileVenueSections(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    const request4 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        region: (reply3?.venue_section?.location?.region ?? ''),
      },
    };
    const reply4 = await call((done) => clients.featureflagsFeatureFlagQueryClient.suggestFeatureFlags(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        tour_budget: {
          audit: { updated_by: (reply2?.sponsorship_deal?.audit?.updated_by ?? '') },
        },
      },
      {
        result: {
          details: (reply4?.result?.details ?? {}),
          status: (reply4?.result?.status ?? ''),
        },
      },
      {
        tour_budget: {
          opened_at: request?.tour_budget?.opened_at,
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Validates a tour budget.
   */
  async function validateTourBudget(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await store.record(request.context.tenant_id, 'ValidateTourBudget', request);
    logger.info('validate_tour_budget', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        audit: { created_by: (request?.tour_budget?.audit?.created_by ?? '') },
        contact: {
          preferred_channel: (request?.tour_budget?.contact?.preferred_channel ?? ''),
        },
        name: request?.details?.contact?.name,
        relationship_manager: { display_name: (request?.tour_budget?.holder?.display_name ?? '') },
      },
    };
    const method1 = 'calculateSponsor';
    const reply1 = await call((done) => clients.sponsorshipsSponsorCalculationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.sponsor?.audit?.updated_at,
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        owner_id: (reply1?.sponsor?.sponsor_id ?? ''),
        region: (reply1?.sponsor?.primary_address?.region ?? ''),
      },
    };
    const client2 = clients.registry.get('acme.sponsorships.v1.SponsorshipDealQueryService');
    const reply2 = await call((done) => client2.browseSponsorshipDeals(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const settled3 = await Promise.allSettled((request?.tour_budget?.limits ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        venue_policy: {
          discount: { amount: item?.amount },
          name: (item?.name ?? ''),
          owner: { email: (reply1?.sponsor?.contact?.email ?? '') },
        },
      };
      const reply3 = await call((done) => clients.venuesVenuePolicyHistoryClient.recordVenuePolicyVersion(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const results3 = settled3.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply3 = results3.at(-1) ?? {};
    const request4 = {
      context: request?.context,
      metric_series: {
        owner: {
          display_name: (reply1?.sponsor?.relationship_manager?.display_name ?? ''),
        },
      },
    };
    const send4 = clients.metricsMetricSeriesValidationClient.lintMetricSerieses.bind(clients.metricsMetricSeriesValidationClient);
    const reply4 = await call((done) => send4(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        tour_budget: {
          holder: { code: (reply4?.metric_series?.code ?? '') },
          limits: [{ name: (reply4?.metric_series?.name ?? '') }],
        },
      },
      {
        tour_budget: {
          contact: request?.tour_budget?.contact,
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  return {
    checkTourBudgetEligibility: unary(checkTourBudgetEligibility),
    explainTourBudgetDecision: unary(explainTourBudgetDecision),
    getTourBudgetRules: unary(getTourBudgetRules),
    lintTourBudgets: unary(lintTourBudgets),
    normalizeTourBudget: unary(normalizeTourBudget),
    previewTourBudget: unary(previewTourBudget),
    scoreTourBudget: unary(scoreTourBudget),
    validateTourBudget: unary(validateTourBudget),
  };
}

module.exports = { createTourBudgetValidationHandlers };
