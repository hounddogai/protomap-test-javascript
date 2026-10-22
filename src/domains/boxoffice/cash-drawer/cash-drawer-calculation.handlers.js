'use strict';

/**
 * Computes the figures of a cash drawer, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of CashDrawerCalculationService, over the dependencies the boxoffice team is given.
 */
function createCashDrawerCalculationHandlers({ store, clients, bus, logger }) {
  /**
   * Calculates a cash drawer.
   */
  async function calculateCashDrawer(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await store.record(request.context.tenant_id, 'CalculateCashDrawer', request);
    logger.info('calculate_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { labels: { ...(request?.cash_drawer?.labels ?? {}) } },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.auditAuditEventQueryClient.suggestAuditEvents(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        cash_drawer: {
          available: request?.cash_drawer?.available,
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
        },
      },
    );
  }

  /**
   * Compares a cash drawer.
   */
  async function compareCashDrawerOptions(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await store.record(request.context.tenant_id, 'CompareCashDrawerOptions', request);
    logger.info('compare_cash_drawer_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      box_office_sale: {
        card: {
          network: ((request?.cash_drawer?.funding === 'card' ? request?.cash_drawer.card : undefined)?.network ?? ''),
        },
      },
      context: request?.context,
      details: { contact: { email: (request?.cash_drawer?.contact?.email ?? '') } },
    };
    const reply1 = await withRetry(() => call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.explainBoxOfficeSaleCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      concession_stand: {
        audit: { updated_by: (reply1?.box_office_sale?.audit?.updated_by ?? '') },
        owner: { display_name: (request?.cash_drawer?.holder?.display_name ?? '') },
      },
      context: request?.context,
    };
    const task2 = deferred(() => call((done) => clients.concessionsConcessionStandValidationClient.validateConcessionStand(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        cash_drawer: {
          audit: { created_by: (reply1?.box_office_sale?.audit?.created_by ?? '') },
        },
      },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        cash_drawer: {
          balance: request?.cash_drawer?.balance,
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
        },
      },
    );
  }

  /**
   * Estimates a cash drawer.
   */
  async function estimateCashDrawer(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await store.record(request.context.tenant_id, 'EstimateCashDrawer', request);
    logger.info('estimate_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      box_office_sale: {
        audit: {
          created_by: (request?.cash_drawer?.audit?.created_by ?? ''),
          etag: (request?.cash_drawer?.audit?.etag ?? ''),
        },
        billing_address: { recipient: (request?.details?.destination?.recipient ?? '') },
      },
      context: request?.context,
    };
    const reply1 = await bus.request('acme.boxoffice.v1.CashDrawerCalculationService/EstimateCashDrawer:0', request1);
    const request2 = {
      box_office_sale: { audit: { etag: (request?.cash_drawer?.audit?.etag ?? '') } },
      context: request?.context,
      details: {
        destination: {
          lines: [...(request?.details?.destination?.lines ?? [])],
          region: (request?.details?.destination?.region ?? ''),
        },
      },
    };
    const reply2 = await runInContext(request.context, () => call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.recalculateBoxOfficeSales(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.outcome?.message ?? '') } },
      {
        cash_drawer: { holder: { number: (reply2?.box_office_sale?.number ?? '') } },
        result: { status: (reply2?.result?.status ?? '') },
      },
      {
        cash_drawer: {
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
          contact: request?.cash_drawer?.contact,
        },
      },
    );
  }

  /**
   * Explains a cash drawer.
   */
  async function explainCashDrawerCalculation(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await store.record(request.context.tenant_id, 'ExplainCashDrawerCalculation', request);
    logger.info('explain_cash_drawer_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.cash_drawer?.labels ?? {}) },
        owner_id: (request?.cash_drawer?.holder?.box_office_sale_id ?? ''),
      },
    };
    const reply1 = await call((done) => clients.identityUserQueryClient.browseUsers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const settled2 = await Promise.allSettled((request?.cash_drawer?.limits ?? []).map(async (item) => {
      const request2 = {
        box_office_sale: {
          counterparty: { display_name: (request?.cash_drawer?.holder?.display_name ?? '') },
          lines: [{ amount: item?.amount }],
        },
        context: request?.context,
      };
      const reply2 = await call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.calculateBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        cash_drawer: {
          balance: request?.cash_drawer?.balance,
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a cash drawer.
   */
  async function getCashDrawerRates(request) {
    validateContext(request.context);
    if (!request.cash_drawer_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
    }
    await store.record(request.context.tenant_id, 'GetCashDrawerRates', request);
    logger.info('get_cash_drawer_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      tour_budget: {
        audit: { created_at: request?.context?.deadline },
        contact: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.toursTourBudgetValidationClient.scoreTourBudget(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    return assemble(
      {
        cash_drawer: { contact: { name: reply1?.tour_budget?.contact?.name } },
      },
      { cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') } },
    );
  }

  /**
   * Recalculates a cash drawer.
   */
  async function recalculateCashDrawers(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await store.record(request.context.tenant_id, 'RecalculateCashDrawers', request);
    logger.info('recalculate_cash_drawers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { phone: (request?.cash_drawer?.contact?.phone ?? '') } },
      tour_budget: {
        audit: { updated_by: (request?.cash_drawer?.audit?.updated_by ?? '') },
        contact: {
          name: request?.cash_drawer?.contact?.name,
          preferred_channel: (request?.cash_drawer?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const checkTourBudgetEligibility1 = promisify(clients.toursTourBudgetValidationClient.checkTourBudgetEligibility.bind(clients.toursTourBudgetValidationClient));
    const reply1 = await checkTourBudgetEligibility1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      box_office_sale_id: (request?.cash_drawer?.holder?.box_office_sale_id ?? ''),
      context: request?.context,
    };
    const reply2 = await call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.getBoxOfficeSaleRates(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        cash_drawer: {
          holder: { display_name: (reply1?.tour_budget?.holder?.display_name ?? '') },
        },
        result: { status: (reply1?.outcome?.status ?? '') },
      },
      {
        cash_drawer: {
          holder: {
            box_office_sale_id: (reply2?.box_office_sale?.box_office_sale_id ?? ''),
          },
        },
      },
      {
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
        cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') },
      },
    );
  }

  return {
    calculateCashDrawer: unary(calculateCashDrawer),
    compareCashDrawerOptions: unary(compareCashDrawerOptions),
    estimateCashDrawer: unary(estimateCashDrawer),
    explainCashDrawerCalculation: unary(explainCashDrawerCalculation),
    getCashDrawerRates: unary(getCashDrawerRates),
    recalculateCashDrawers: unary(recalculateCashDrawers),
  };
}

module.exports = { createCashDrawerCalculationHandlers };
