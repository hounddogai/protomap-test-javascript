'use strict';

/**
 * Checks lineups against the rules before anything acts on them.
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
 * The handlers of LineupValidationService, over the dependencies the shows team is given.
 */
function createLineupValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks a lineup.
   */
  async function checkLineupEligibility(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'CheckLineupEligibility', request);
    logger.info('check_lineup_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: { country_code: (request?.lineup?.billing_address?.country_code ?? '') },
      },
    };
    const method1 = 'scoreShow';
    const reply1 = await call((done) => clients.showsShowValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.lineup?.issued_at,
        region: (request?.lineup?.billing_address?.region ?? ''),
      },
    };
    const client2 = clients.registry.get('acme.sponsorships.v1.SponsorshipDealQueryService');
    const reply2 = await call((done) => client2.countSponsorshipDeals(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.lineup?.audit?.updated_at,
        labels: { ...(reply1?.show?.metadata ?? {}) },
        owner_id: (request?.lineup?.lineup_id ?? ''),
        region: (reply1?.show?.location?.region ?? ''),
      },
    };
    const messages3 = await collect(clients.identityUserQueryClient.exportUsers(request3, metadataFor(request3.context), { deadline: deadline() }));
    const reply3 = messages3.at(-1) ?? {};
    const request4 = {
      box_office_sale: {
        billing_address: {
          country_code: (request?.lineup?.billing_address?.country_code ?? ''),
          locality: (request?.lineup?.billing_address?.locality ?? ''),
        },
      },
      context: request?.context,
    };
    const send4 = clients.boxofficeBoxOfficeSaleValidationClient.previewBoxOfficeSale.bind(clients.boxofficeBoxOfficeSaleValidationClient);
    const reply4 = await call((done) => send4(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        lineup: { audit: { etag: (reply1?.show?.audit?.etag ?? '') } },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          status: (reply2?.result?.status ?? ''),
        },
      },
      { lineup: { metadata: (reply4?.box_office_sale?.metadata ?? {}) } },
      {
        lineup: {
          issued_at: request?.lineup?.issued_at,
          lineup_id: (request?.lineup_id ?? ''),
        },
      },
    );
  }

  /**
   * Explains a lineup.
   */
  async function explainLineupDecision(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'ExplainLineupDecision', request);
    logger.info('explain_lineup_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.toursTourQueryClient.resolveTours(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      details: {
        destination: { lines: [...(input2?.lineup?.billing_address?.lines ?? [])] },
      },
      performance: {
        audit: { updated_at: input2?.lineup?.audit?.updated_at },
        participants: [{
          contact: {
            email: (input2?.details?.contact?.email ?? ''),
            phone: (input2?.details?.contact?.phone ?? ''),
          },
        }],
      },
    };
    const reply2 = await call((done) => clients.showsPerformanceCalculationClient.comparePerformanceOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      performance: {
        organizer: { user_id: (reply2?.performance?.organizer?.user_id ?? '') },
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => clients.showsPerformanceCalculationClient.calculatePerformance(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        lineup: { audit: { updated_by: (reply2?.performance?.audit?.updated_by ?? '') } },
        result: { details: (reply2?.result?.details ?? {}) },
      },
      {
        lineup: {
          billing_address: { recipient: (reply3?.performance?.location?.recipient ?? '') },
        },
        result: { status: (reply3?.result?.status ?? '') },
      },
      {
        lineup: {
          lineup_id: (request?.lineup_id ?? ''),
          metadata: (request?.lineup?.metadata ?? {}),
        },
      },
    );
  }

  /**
   * Returns a lineup.
   */
  async function getLineupRules(request) {
    validateContext(request.context);
    if (!request.lineup_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'lineup_id is required');
    }
    await store.record(request.context.tenant_id, 'GetLineupRules', request);
    logger.info('get_lineup_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { lineup: { lineup_id: (request?.lineup_id ?? '') } };
  }

  /**
   * Lints a lineup.
   */
  async function lintLineups(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'LintLineups', request);
    logger.info('lint_lineups', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          locality: (request?.lineup?.billing_address?.locality ?? ''),
          region: (request?.lineup?.billing_address?.region ?? ''),
        },
      },
      performance: {
        participants: [{
          contact: {
            preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.showsPerformanceCalculationClient.recalculatePerformances(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      show: {
        participants: [{ contact: { email: (request?.details?.contact?.email ?? '') } }],
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.showsShowCalculationClient.explainShowCalculation(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    const request3 = {
      box_office_sale: { audit: { updated_at: reply1?.performance?.audit?.updated_at } },
      context: request?.context,
    };
    const reply3 = await withRetry(() => call((done) => clients.boxofficeBoxOfficeSaleValidationClient.explainBoxOfficeSaleDecision(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        lineup: {
          billing_address: { country_code: (reply1?.performance?.location?.country_code ?? '') },
        },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      { outcome: { status: (reply2?.result?.status ?? '') } },
      {
        lineup: {
          billing_address: { locality: (reply3?.box_office_sale?.billing_address?.locality ?? '') },
        },
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
   * Normalizes a lineup.
   */
  async function normalizeLineup(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'NormalizeLineup', request);
    logger.info('normalize_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      lineup: {
        billing_address: { country_code: (request?.lineup?.billing_address?.country_code ?? '') },
      },
    };
    const task1 = deferred(() => call((done) => clients.showsLineupCalculationClient.calculateLineup(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      show: {
        location: { postal_code: (reply1?.lineup?.billing_address?.postal_code ?? '') },
        participants: [{
          contact: {
            phone: (request?.details?.contact?.phone ?? ''),
            preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const reply2 = await bus.request('acme.shows.v1.LineupValidationService/NormalizeLineup:1', request2);
    const request3 = {
      context: request?.context,
      lineup: {
        counterparty: { venue_id: (reply1?.lineup?.counterparty?.venue_id ?? '') },
      },
    };
    const reply3 = await runInContext(request.context, () => call((done) => clients.showsLineupCalculationClient.simulateLineup(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const request4 = {
      context: request?.context,
      filter: {
        created_after: reply3?.lineup?.audit?.updated_at,
        region: (reply2?.show?.location?.region ?? ''),
      },
    };
    const reply4 = await call((done) => clients.identityUserQueryClient.countUsers(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        lineup: { audit: { created_at: reply1?.lineup?.audit?.created_at } },
        outcome: { message: (reply1?.result?.message ?? '') },
      },
      {
        lineup: { billing_address: { lines: (reply2?.show?.location?.lines ?? []) } },
      },
      {
        lineup: {
          billing_address: { lines: (reply3?.lineup?.billing_address?.lines ?? []) },
          due_at: reply3?.lineup?.due_at,
        },
      },
      { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
      {
        lineup: {
          lineup_id: (request?.lineup_id ?? ''),
          metadata: (request?.lineup?.metadata ?? {}),
        },
      },
    );
  }

  /**
   * Previews a lineup.
   */
  async function previewLineup(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'PreviewLineup', request);
    logger.info('preview_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      performance: {
        audit: { etag: (request?.lineup?.audit?.etag ?? '') },
        location: { locality: (request?.lineup?.billing_address?.locality ?? '') },
        participants: [{
          contact: {
            email: (request?.details?.contact?.email ?? ''),
            preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.showsPerformanceValidationClient.previewPerformance(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      details: {
        destination: { region: (reply1?.performance?.location?.region ?? '') },
      },
      performance: {
        location: { region: (request?.lineup?.billing_address?.region ?? '') },
        organizer: { display_name: (reply1?.performance?.organizer?.display_name ?? '') },
        participants: [{ contact: { name: request?.details?.contact?.name } }],
      },
    };
    const estimatePerformance2 = promisify(clients.showsPerformanceCalculationClient.estimatePerformance.bind(clients.showsPerformanceCalculationClient));
    const reply2 = await estimatePerformance2(request2, metadataFor(request2.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        lineup: {
          audit: { updated_at: reply2?.performance?.audit?.updated_at },
          metadata: (reply2?.performance?.metadata ?? {}),
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

  /**
   * Scores a lineup.
   */
  async function scoreLineup(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'ScoreLineup', request);
    logger.info('score_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.lineup?.billing_address?.country_code ?? ''),
          recipient: (request?.lineup?.billing_address?.recipient ?? ''),
        },
      },
      performance: {
        location: { recipient: (request?.details?.destination?.recipient ?? '') },
      },
    };
    const reply1 = await call((done) => clients.showsPerformanceCalculationClient.estimatePerformance(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.lineup?.audit?.updated_at,
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const method2 = 'suggestLedgerAccounts';
    const reply2 = await call((done) => clients.ledgerLedgerAccountQueryClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        lineup: {
          audit: {
            created_at: reply1?.performance?.audit?.created_at,
            etag: (reply1?.performance?.audit?.etag ?? ''),
          },
        },
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          message: (reply2?.result?.message ?? ''),
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

  /**
   * Validates a lineup.
   */
  async function validateLineup(request) {
    validateContext(request.context);
    if (request.lineup) {
      await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await store.record(request.context.tenant_id, 'ValidateLineup', request);
    logger.info('validate_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: { region: (request?.lineup?.billing_address?.region ?? '') },
      },
      performance: { audit: { etag: (request?.lineup?.audit?.etag ?? '') } },
    };
    const client1 = clients.registry.get('acme.shows.v1.PerformanceCalculationService');
    const reply1 = await call((done) => client1.recalculatePerformances(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      seat_assignment: {
        audit: {
          created_at: request?.lineup?.audit?.created_at,
          etag: (request?.lineup?.audit?.etag ?? ''),
          updated_at: request?.lineup?.audit?.updated_at,
        },
        comments: [{ created_at: reply1?.performance?.audit?.created_at }],
      },
    };
    const send2 = clients.seatingSeatAssignmentCalculationClient.calculateSeatAssignment.bind(clients.seatingSeatAssignmentCalculationClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        lineup: {
          billing_address: { country_code: (reply1?.performance?.location?.country_code ?? '') },
        },
      },
      {
        outcome: { status: (reply2?.result?.status ?? '') },
        result: { details: (reply2?.result?.details ?? {}) },
      },
      {
        lineup: {
          lineup_id: (request?.lineup_id ?? ''),
          notes: (request?.lineup?.notes ?? ''),
        },
      },
    );
  }

  return {
    checkLineupEligibility: unary(checkLineupEligibility),
    explainLineupDecision: unary(explainLineupDecision),
    getLineupRules: unary(getLineupRules),
    lintLineups: unary(lintLineups),
    normalizeLineup: unary(normalizeLineup),
    previewLineup: unary(previewLineup),
    scoreLineup: unary(scoreLineup),
    validateLineup: unary(validateLineup),
  };
}

module.exports = { createLineupValidationHandlers };
