'use strict';

/**
 * Computes the figures of a box office sale, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of BoxOfficeSaleCalculationService, over the dependencies the boxoffice team is given.
 */
function createBoxOfficeSaleCalculationHandlers({ store, clients, bus, logger }) {
  /**
   * Calculates a box office sale.
   */
  async function calculateBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'CalculateBoxOfficeSale', request);
    logger.info('calculate_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
      reason: (request?.reason ?? ''),
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.venuesVenueSectionNotificationClient.notifyVenueSectionWatchers(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.destination?.country_code ?? '') });
    return assemble(
      {
        box_office_sale: { audit: { created_at: reply1?.venue_section?.audit?.created_at } },
      },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          issued_at: request?.box_office_sale?.issued_at,
        },
      },
    );
  }

  /**
   * Compares a box office sale.
   */
  async function compareBoxOfficeSaleOptions(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'CompareBoxOfficeSaleOptions', request);
    logger.info('compare_box_office_sale_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      annotations: { ...(input1?.annotations ?? {}) },
      context: input1?.context,
      details: { filters: { ...(input1?.box_office_sale?.metadata ?? {}) } },
      reason: (input1?.reason ?? ''),
    };
    const reply1 = await call((done) => clients.performersPerformerContractQueryClient.summarizePerformerContracts(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.box_office_sale?.audit?.updated_at,
        region: (request?.box_office_sale?.billing_address?.region ?? ''),
      },
      since: request?.box_office_sale?.audit?.created_at,
      subscriber: { email: (request?.context?.actor?.email ?? '') },
    };
    const messages2 = await collect(clients.venuesVenueSectionNotificationClient.streamVenueSectionNotices(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      filter: { region: (request?.box_office_sale?.billing_address?.region ?? '') },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => clients.auditAuditEventQueryClient.suggestAuditEvents(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.destination?.country_code ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        result: {
          details: (reply3?.result?.details ?? {}),
          message: (reply3?.result?.message ?? ''),
        },
      },
      {
        box_office_sale: { box_office_sale_id: (request?.box_office_sale_id ?? '') },
        breakdown: { total: request?.box_office_sale?.total },
      },
    );
  }

  /**
   * Estimates a box office sale.
   */
  async function estimateBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'EstimateBoxOfficeSale', request);
    logger.info('estimate_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.box_office_sale?.audit?.updated_at,
        labels: { ...(request?.box_office_sale?.metadata ?? {}) },
        owner_id: (request?.box_office_sale?.box_office_sale_id ?? ''),
        region: (request?.box_office_sale?.billing_address?.region ?? ''),
      },
    };
    const messages1 = await collect(clients.admissionsAdmissionQueryClient.exportAdmissions(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      box_office_sale: {
        box_office_sale_id: (request?.box_office_sale_id ?? ''),
        metadata: (request?.box_office_sale?.metadata ?? {}),
      },
    };
  }

  /**
   * Explains a box office sale.
   */
  async function explainBoxOfficeSaleCalculation(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'ExplainBoxOfficeSaleCalculation', request);
    logger.info('explain_box_office_sale_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      access_policy: {
        audit: {
          etag: (request?.box_office_sale?.audit?.etag ?? ''),
          updated_at: request?.box_office_sale?.audit?.updated_at,
        },
        owner: {
          display_name: (request?.box_office_sale?.counterparty?.display_name ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: { region: (request?.box_office_sale?.billing_address?.region ?? '') },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.authorizationAccessPolicyValidationClient.scoreAccessPolicy(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply1?.result?.status ?? '') } },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          due_at: request?.box_office_sale?.due_at,
        },
      },
    );
  }

  /**
   * Returns a box office sale.
   */
  async function getBoxOfficeSaleRates(request) {
    validateContext(request.context);
    if (!request.box_office_sale_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'box_office_sale_id is required');
    }
    await store.record(request.context.tenant_id, 'GetBoxOfficeSaleRates', request);
    logger.info('get_box_office_sale_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: { measurements: [{ observed_at: request?.context?.deadline }] },
      context: request?.context,
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.admissionsAdmissionClient.cloneAdmission(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        labels: { ...(reply1?.admission?.attributes ?? {}) },
        owner_id: (reply1?.admission?.subject?.gate_scan_id ?? ''),
      },
    };
    const reply2 = await withRetry(() => call((done) => clients.identityUserQueryClient.browseUsers(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        box_office_sale: { box_office_sale_id: (request?.box_office_sale_id ?? '') },
      },
    );
  }

  /**
   * Quotes a box office sale.
   */
  async function quoteBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'QuoteBoxOfficeSale', request);
    logger.info('quote_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.box_office_sale?.billing_address?.country_code ?? ''),
          lines: [...(request?.details?.destination?.lines ?? [])],
        },
      },
      menu_item: {
        address: { region: (request?.box_office_sale?.billing_address?.region ?? '') },
        audit: { etag: (request?.box_office_sale?.audit?.etag ?? '') },
      },
    };
    const task1 = deferred(() => call((done) => clients.concessionsMenuItemCalculationClient.recalculateMenuItems(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      user: {
        primary_address: {
          recipient: ((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.recipient ?? ''),
        },
      },
    };
    const reply2 = await bus.request('acme.boxoffice.v1.BoxOfficeSaleCalculationService/QuoteBoxOfficeSale:1', request2);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        box_office_sale: { audit: { etag: (reply1?.menu_item?.audit?.etag ?? '') } },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        box_office_sale: { box_office_sale_id: (request?.box_office_sale_id ?? '') },
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
      },
    );
  }

  /**
   * Recalculates a box office sale.
   */
  async function recalculateBoxOfficeSales(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'RecalculateBoxOfficeSales', request);
    logger.info('recalculate_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.box_office_sale?.audit?.created_at,
        region: (request?.box_office_sale?.billing_address?.region ?? ''),
      },
      since: request?.box_office_sale?.audit?.updated_at,
      subscriber: { email: (request?.context?.actor?.email ?? '') },
    };
    const messages1 = await collect(clients.venuesVenueSectionNotificationClient.streamVenueSectionNotices(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      filter: { region: (request?.details?.destination?.region ?? '') },
    };
    const reply2 = await runInContext(request.context, () => call((done) => clients.performersPerformerQueryClient.countPerformers(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { filters: { ...(reply2?.result?.details ?? {}) } },
      reason: (request?.reason ?? ''),
    };
    const reply3 = await call((done) => clients.performersPerformerContractQueryClient.summarizePerformerContracts(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.destination?.country_code ?? '') });
    return assemble(
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        result: {
          details: (reply3?.result?.details ?? {}),
          status: (reply3?.result?.status ?? ''),
        },
      },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          number: (request?.box_office_sale?.number ?? ''),
        },
      },
    );
  }

  return {
    calculateBoxOfficeSale: unary(calculateBoxOfficeSale),
    compareBoxOfficeSaleOptions: unary(compareBoxOfficeSaleOptions),
    estimateBoxOfficeSale: unary(estimateBoxOfficeSale),
    explainBoxOfficeSaleCalculation: unary(explainBoxOfficeSaleCalculation),
    getBoxOfficeSaleRates: unary(getBoxOfficeSaleRates),
    quoteBoxOfficeSale: unary(quoteBoxOfficeSale),
    recalculateBoxOfficeSales: unary(recalculateBoxOfficeSales),
  };
}

module.exports = { createBoxOfficeSaleCalculationHandlers };
