'use strict';

/**
 * Computes the figures of a performance, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred } = require('../../../platform/rpc');

/**
 * The handlers of PerformanceCalculationService, over the dependencies the shows team is given.
 */
function createPerformanceCalculationHandlers({ store, clients, bus, logger }) {
  /**
   * Calculates a performance.
   */
  async function calculatePerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'CalculatePerformance', request);
    logger.info('calculate_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        daily: {
          at_hour: ((request?.performance?.recurrence === 'daily' ? request?.performance.daily : undefined)?.at_hour ?? 0),
        },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.showsShowCalculationClient.recalculateShows(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        performance: {
          location: { postal_code: (reply1?.show?.location?.postal_code ?? '') },
        },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
        performance: { performance_id: (request?.performance_id ?? '') },
      },
    );
  }

  /**
   * Compares a performance.
   */
  async function comparePerformanceOptions(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'ComparePerformanceOptions', request);
    logger.info('compare_performance_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_2;
    switch (request?.performance?.recurrence) {
      case 'weekly':
        selected1_2 = ((request?.performance?.recurrence === 'weekly' ? request?.performance.weekly : undefined)?.interval ?? 0);
        break;
      default:
        selected1_2 = 0;
    }
    const request1 = {
      context: request?.context,
      show: {
        audit: { updated_at: request?.performance?.audit?.updated_at },
        location: { lines: [...(request?.details?.destination?.lines ?? [])] },
        weekly: { interval: selected1_2 },
      },
    };
    const reply1 = await call((done) => clients.showsShowCalculationClient.quoteShow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.performance?.audit?.updated_at,
        owner_id: (reply1?.show?.organizer?.user_id ?? ''),
        region: (reply1?.show?.location?.region ?? ''),
      },
    };
    const task2 = deferred(() => call((done) => clients.metricsMetricSeriesQueryClient.countMetricSerieses(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        performance: {
          audit: {
            created_at: reply1?.show?.audit?.created_at,
            created_by: (reply1?.show?.audit?.created_by ?? ''),
          },
        },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
        performance: { performance_id: (request?.performance_id ?? '') },
      },
    );
  }

  /**
   * Estimates a performance.
   */
  async function estimatePerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'EstimatePerformance', request);
    logger.info('estimate_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_hold: {
        location: { locality: (request?.details?.destination?.locality ?? '') },
      },
    };
    const reply1 = await bus.request('acme.shows.v1.PerformanceCalculationService/EstimatePerformance:0', request1);
    let selected2_3;
    switch (reply1?.seat_hold?.recurrence) {
      case 'daily':
        selected2_3 = ((reply1?.seat_hold?.recurrence === 'daily' ? reply1?.seat_hold.daily : undefined)?.interval ?? 0);
        break;
      default:
        selected2_3 = 0;
    }
    const request2 = {
      context: request?.context,
      show: {
        location: {
          recipient: (reply1?.seat_hold?.location?.recipient ?? ''),
          region: (request?.details?.destination?.region ?? ''),
        },
        weekly: { interval: selected2_3 },
      },
    };
    const reply2 = await call((done) => clients.showsShowCalculationClient.explainShowCalculation(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const results3 = [];
    for (const item of (request?.performance?.participants ?? [])) {
      const request3 = {
        context: request?.context,
        show: {
          participants: [{
            contact: {
              email: (item?.contact?.email ?? ''),
              name: item?.contact?.name,
              preferred_channel: (item?.contact?.preferred_channel ?? ''),
            },
          }],
        },
      };
      const reply3 = await call((done) => clients.showsShowCalculationClient.simulateShow(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      results3.push(reply3);
    }
    const reply3 = results3.at(-1) ?? {};
    const request4 = {
      context: request?.context,
      wristband: { owner: { email: (reply2?.show?.organizer?.email ?? '') } },
    };
    const reply4 = await call((done) => clients.admissionsWristbandCalculationClient.recalculateWristbands(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { performance: { window: reply1?.seat_hold?.window } },
      {
        performance: { audit: { etag: (reply2?.show?.audit?.etag ?? '') } },
        result: { details: (reply2?.result?.details ?? {}) },
      },
      {
        breakdown: { currency_code: (reply4?.breakdown?.currency_code ?? '') },
        result: { status: (reply4?.result?.status ?? '') },
      },
      {
        performance: {
          performance_id: (request?.performance_id ?? ''),
          title: (request?.performance?.title ?? ''),
        },
      },
    );
  }

  /**
   * Explains a performance.
   */
  async function explainPerformanceCalculation(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'ExplainPerformanceCalculation', request);
    logger.info('explain_performance_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.performance?.audit?.updated_at,
        region: (request?.performance?.location?.region ?? ''),
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.ticketingTicketQueryClient.countTickets(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      tour: {
        participants: [{ contact: { email: (request?.performance?.organizer?.email ?? '') } }],
      },
    };
    const updateTour2 = promisify(clients.toursTourClient.updateTour.bind(clients.toursTourClient));
    const reply2 = await updateTour2(request2, metadataFor(request2.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        performance: { organizer: { email: (reply2?.tour?.organizer?.email ?? '') } },
      },
      {
        performance: {
          performance_id: (request?.performance_id ?? ''),
          status: (request?.performance?.status ?? ''),
        },
      },
    );
  }

  /**
   * Returns a performance.
   */
  async function getPerformanceRates(request) {
    validateContext(request.context);
    if (!request.performance_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performance_id is required');
    }
    await store.record(request.context.tenant_id, 'GetPerformanceRates', request);
    logger.info('get_performance_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      seat_hold: { organizer: { user_id: (request?.context?.actor?.user_id ?? '') } },
    };
    const reply1 = await call((done) => clients.seatingSeatHoldValidationClient.previewSeatHold(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      {
        performance: {
          location: { recipient: (reply1?.seat_hold?.location?.recipient ?? '') },
          organizer: { display_name: (reply1?.seat_hold?.organizer?.display_name ?? '') },
        },
      },
      { performance: { performance_id: (request?.performance_id ?? '') } },
    );
  }

  /**
   * Quotes a performance.
   */
  async function quotePerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'QuotePerformance', request);
    logger.info('quote_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.performance?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        show: {
          participants: [{
            contact: { preferred_channel: (item?.contact?.preferred_channel ?? '') },
          }],
        },
      };
      const reply1 = await call((done) => clients.showsShowCalculationClient.calculateShow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const settled2 = await Promise.allSettled((request?.performance?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        details: {
          destination: { country_code: (request?.performance?.location?.country_code ?? '') },
        },
        search_index: { owner: { email: (item?.contact?.email ?? '') } },
      };
      const reply2 = await call((done) => clients.searchSearchIndexValidationClient.scoreSearchIndex(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      performance: {
        metadata: (request?.performance?.metadata ?? {}),
        performance_id: (request?.performance_id ?? ''),
      },
    };
  }

  /**
   * Recalculates a performance.
   */
  async function recalculatePerformances(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'RecalculatePerformances', request);
    logger.info('recalculate_performances', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      access_policy: { audit: { etag: (request?.performance?.audit?.etag ?? '') } },
      context: request?.context,
      details: { contact: { email: (request?.performance?.organizer?.email ?? '') } },
    };
    const method1 = 'normalizeAccessPolicy';
    const reply1 = await call((done) => clients.authorizationAccessPolicyValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        destination: { postal_code: (request?.details?.destination?.postal_code ?? '') },
      },
      wristband: {
        audit: {
          etag: (request?.performance?.audit?.etag ?? ''),
          updated_by: (reply1?.access_policy?.audit?.updated_by ?? ''),
        },
        owner: { email: (request?.performance?.organizer?.email ?? '') },
      },
    };
    const client2 = clients.registry.get('acme.admissions.v1.WristbandCalculationService');
    const reply2 = await call((done) => client2.compareWristbandOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        performance: { audit: { created_by: (reply2?.wristband?.audit?.created_by ?? '') } },
        result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
      },
      {
        performance: {
          performance_id: (request?.performance_id ?? ''),
          window: request?.performance?.window,
        },
      },
    );
  }

  /**
   * Simulates a performance.
   */
  async function simulatePerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'SimulatePerformance', request);
    logger.info('simulate_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_hold: {
        audit: { created_at: request?.performance?.audit?.created_at },
        location: { lines: [...(request?.performance?.location?.lines ?? [])] },
      },
    };
    const send1 = clients.seatingSeatHoldValidationClient.validateSeatHold.bind(clients.seatingSeatHoldValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      show: { audit: { etag: (reply1?.seat_hold?.audit?.etag ?? '') } },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.showsShowCalculationClient.quoteShow(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      context: input3?.context,
      show: {
        location: { locality: (reply1?.seat_hold?.location?.locality ?? '') },
        organizer: { email: (input3?.performance?.organizer?.email ?? '') },
        participants: [{ contact: { email: (reply1?.seat_hold?.organizer?.email ?? '') } }],
      },
    };
    const reply3 = await call((done) => clients.showsShowCalculationClient.estimateShow(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        performance: { organizer: { user_id: (reply1?.seat_hold?.organizer?.user_id ?? '') } },
      },
      {
        performance: { audit: { updated_at: reply2?.show?.audit?.updated_at } },
      },
      { result: { message: (reply3?.result?.message ?? '') } },
      {
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
        performance: { performance_id: (request?.performance_id ?? '') },
      },
    );
  }

  return {
    calculatePerformance: unary(calculatePerformance),
    comparePerformanceOptions: unary(comparePerformanceOptions),
    estimatePerformance: unary(estimatePerformance),
    explainPerformanceCalculation: unary(explainPerformanceCalculation),
    getPerformanceRates: unary(getPerformanceRates),
    quotePerformance: unary(quotePerformance),
    recalculatePerformances: unary(recalculatePerformances),
    simulatePerformance: unary(simulatePerformance),
  };
}

module.exports = { createPerformanceCalculationHandlers };
