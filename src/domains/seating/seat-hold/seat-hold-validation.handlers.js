'use strict';

/**
 * Checks seat holds against the rules before anything acts on them.
 */

const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of SeatHoldValidationService, over the dependencies the seating team is given.
 */
function createSeatHoldValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks a seat hold.
   */
  async function checkSeatHoldEligibility(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await store.record(request.context.tenant_id, 'CheckSeatHoldEligibility', request);
    logger.info('check_seat_hold_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.sponsorshipsSponsorshipDealQueryClient.summarizeSponsorshipDeals(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      seat_assignment: { requester: { email: (request?.details?.contact?.email ?? '') } },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.seatingSeatAssignmentCalculationClient.compareSeatAssignmentOptions(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.contact?.preferred_channel ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        seat_hold: {
          audit: { updated_by: (reply2?.seat_assignment?.audit?.updated_by ?? '') },
        },
      },
      {
        seat_hold: {
          location: request?.seat_hold?.location,
          seat_hold_id: (request?.seat_hold_id ?? ''),
        },
      },
    );
  }

  /**
   * Explains a seat hold.
   */
  async function explainSeatHoldDecision(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await store.record(request.context.tenant_id, 'ExplainSeatHoldDecision', request);
    logger.info('explain_seat_hold_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { phone: (request?.details?.contact?.phone ?? '') } },
      user: {
        primary_address: { postal_code: (request?.seat_hold?.location?.postal_code ?? '') },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.identityUserValidationClient.normalizeUser(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        seat_hold: {
          seat_hold_id: (request?.seat_hold_id ?? ''),
          title: (request?.seat_hold?.title ?? ''),
        },
      },
    );
  }

  /**
   * Lints a seat hold.
   */
  async function lintSeatHolds(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await store.record(request.context.tenant_id, 'LintSeatHolds', request);
    logger.info('lint_seat_holds', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: {
              updated_at: request?.seat_hold?.audit?.updated_at,
              updated_by: (request?.seat_hold?.audit?.updated_by ?? ''),
            },
          }],
        }],
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.seatingSeatMapCalculationClient.calculateSeatMap(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const results2 = [];
    for (const item of (request?.seat_hold?.participants ?? [])) {
      const request2 = {
        context: request?.context,
        venue_section: {
          children: [{ children: [{ manager: { email: (item?.contact?.email ?? '') } }] }],
        },
      };
      const reply2 = await call((done) => clients.venuesVenueSectionHistoryClient.recordVenueSectionVersion(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      results2.push(reply2);
    }
    const reply2 = results2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{ audit: { etag: (request?.seat_hold?.audit?.etag ?? '') } }],
        }],
      },
    };
    const task3 = deferred(() => call((done) => clients.seatingSeatMapValidationClient.normalizeSeatMap(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    const request4 = {
      box_office_sale: {
        billing_address: {
          country_code: (reply3?.seat_map?.location?.country_code ?? ''),
          recipient: (reply3?.seat_map?.location?.recipient ?? ''),
        },
      },
      context: request?.context,
    };
    const reply4 = await bus.request('acme.seating.v1.SeatHoldValidationService/LintSeatHolds:3', request4);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        seat_hold: {
          organizer: { display_name: (reply1?.seat_map?.manager?.display_name ?? '') },
        },
      },
      {
        seat_hold: { organizer: { user_id: (reply3?.seat_map?.manager?.user_id ?? '') } },
      },
      {
        outcome: { message: (reply4?.result?.message ?? '') },
        result: { reference_ids: (reply4?.result?.reference_ids ?? []) },
      },
      {
        seat_hold: {
          location: request?.seat_hold?.location,
          seat_hold_id: (request?.seat_hold_id ?? ''),
        },
      },
    );
  }

  /**
   * Previews a seat hold.
   */
  async function previewSeatHold(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await store.record(request.context.tenant_id, 'PreviewSeatHold', request);
    logger.info('preview_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.seat_hold?.audit?.created_at,
        region: (request?.seat_hold?.location?.region ?? ''),
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.ledgerLedgerAccountQueryClient.browseLedgerAccounts(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        seat_hold: {
          seat_hold_id: (request?.seat_hold_id ?? ''),
          status: (request?.seat_hold?.status ?? ''),
        },
      },
    );
  }

  /**
   * Scores a seat hold.
   */
  async function scoreSeatHold(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await store.record(request.context.tenant_id, 'ScoreSeatHold', request);
    logger.info('score_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: {
        address: {
          country_code: (request?.seat_hold?.location?.country_code ?? ''),
          locality: (request?.seat_hold?.location?.locality ?? ''),
          region: (request?.details?.destination?.region ?? ''),
        },
        audit: { etag: (request?.seat_hold?.audit?.etag ?? '') },
      },
    };
    const reply1 = await call((done) => clients.concessionsMenuItemCalculationClient.quoteMenuItem(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const results2 = await Promise.all((request?.seat_hold?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        feature_flag: {
          owner: {
            display_name: (request?.seat_hold?.organizer?.display_name ?? ''),
            email: (item?.contact?.email ?? ''),
          },
        },
      };
      const reply2 = await call((done) => clients.featureflagsFeatureFlagClient.cloneFeatureFlag(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_hold: {
          audit: { etag: (reply1?.menu_item?.audit?.etag ?? '') },
          location: {
            country_code: ((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.country_code ?? ''),
          },
        },
      },
      {
        seat_hold: {
          seat_hold_id: (request?.seat_hold_id ?? ''),
          window: request?.seat_hold?.window,
        },
      },
    );
  }

  /**
   * Validates a seat hold.
   */
  async function validateSeatHold(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await store.record(request.context.tenant_id, 'ValidateSeatHold', request);
    logger.info('validate_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.seat_hold?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        metric_series: { owner: { email: (item?.contact?.email ?? '') } },
      };
      const reply1 = await call((done) => clients.metricsMetricSeriesValidationClient.normalizeMetricSeries(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      seat_hold: {
        seat_hold_id: (request?.seat_hold_id ?? ''),
        window: request?.seat_hold?.window,
      },
    };
  }

  return {
    checkSeatHoldEligibility: unary(checkSeatHoldEligibility),
    explainSeatHoldDecision: unary(explainSeatHoldDecision),
    lintSeatHolds: unary(lintSeatHolds),
    previewSeatHold: unary(previewSeatHold),
    scoreSeatHold: unary(scoreSeatHold),
    validateSeatHold: unary(validateSeatHold),
  };
}

module.exports = { createSeatHoldValidationHandlers };
