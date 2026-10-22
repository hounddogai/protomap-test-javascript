'use strict';

/**
 * Builds and schedules reports about concession orders.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect, exchange } = require('../../../platform/streams');

/**
 * The handlers of ConcessionOrderReportingService, over the dependencies the concessions team is given.
 */
function createConcessionOrderReportingHandlers({ store, clients, bus, logger }) {
  /**
   * Counts concession orders by status.
   */
  async function countConcessionOrderTotals(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'CountConcessionOrderTotals', request);
    logger.info('count_concession_order_totals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Deletes a concession order.
   */
  async function deleteConcessionOrderReport(request) {
    validateContext(request.context);
    if (!request.concession_order_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
    }
    await store.record(request.context.tenant_id, 'DeleteConcessionOrderReport', request);
    logger.info('delete_concession_order_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: { audit: { etag: (request?.etag ?? '') } },
      context: request?.context,
    };
    const reply1 = await call((done) => clients.concessionsConcessionStandCalculationClient.compareConcessionStandOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        destination: {
          country_code: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.country_code ?? ''),
        },
      },
      menu_item: {
        address: {
          locality: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.locality ?? ''),
        },
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      clients.concessionsMenuItemCalculationClient.estimateMenuItem(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          reference_ids: (reply1?.result?.reference_ids ?? []),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { result: { details: (reply2?.result?.details ?? {}) } },
    );
  }

  /**
   * Returns a concession order.
   */
  async function fetchConcessionOrderReport(request) {
    validateContext(request.context);
    if (!request.concession_order_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
    }
    await store.record(request.context.tenant_id, 'FetchConcessionOrderReport', request);
    logger.info('fetch_concession_order_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: {
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const simulateMenuItem1 = promisify(clients.concessionsMenuItemCalculationClient.simulateMenuItem.bind(clients.concessionsMenuItemCalculationClient));
    const reply1 = await simulateMenuItem1(request1, metadataFor(request1.context), { deadline: deadline() });
    let selected2_2;
    switch (reply1?.menu_item?.locator) {
      case 'address':
        selected2_2 = ((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.recipient ?? '');
        break;
      default:
        selected2_2 = '';
    }
    const request2 = {
      concession_stand: {
        address: { recipient: selected2_2 },
        dimensions: { width: (reply1?.menu_item?.dimensions?.width ?? 0) },
      },
      context: request?.context,
    };
    const reply2 = await call((done) => clients.concessionsConcessionStandValidationClient.scoreConcessionStand(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      change: {
        children: [{
          children: [{ parent_id: (request?.concession_order_id ?? '') }],
          parent_id: (request?.concession_order_id ?? ''),
        }],
        parent_id: (request?.concession_order_id ?? ''),
      },
      context: request?.context,
    };
    const messages3 = await exchange(clients.venuesVenueSectionSyncClient.streamVenueSectionChanges(metadataFor(request3.context), { deadline: deadline() }), [request3]);
    const reply3 = messages3.at(-1) ?? {};
    return assemble(
      {
        concession_order: {
          lines: [{
            description: (reply1?.menu_item?.components?.primary?.description ?? ''),
          }],
        },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        concession_order: {
          audit: {
            created_by: (reply2?.concession_stand?.audit?.created_by ?? ''),
            etag: (reply2?.concession_stand?.audit?.etag ?? ''),
          },
        },
      },
      {
        concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      },
    );
  }

  /**
   * Generates a concession order.
   */
  async function generateConcessionOrderReport(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'GenerateConcessionOrderReport', request);
    logger.info('generate_concession_order_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const method1 = 'resolveAnomalies';
    const reply1 = await call((done) => clients.anomaliesAnomalyQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      metric_series: {
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const client2 = clients.registry.get('acme.metrics.v1.MetricSeriesValidationService');
    const reply2 = await call((done) => client2.scoreMetricSeries(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply2?.metric_series?.audit?.created_at,
        region: ((reply2?.metric_series?.locator === 'address' ? reply2?.metric_series.address : undefined)?.region ?? ''),
      },
      format: (request?.details?.format ?? ''),
    };
    const messages3 = await collect(clients.venuesVenueSectionQueryClient.exportVenueSections(request3, metadataFor(request3.context), { deadline: deadline() }));
    const reply3 = messages3.at(-1) ?? {};
    let selected4_1;
    switch (reply2?.metric_series?.locator) {
      case 'address':
        selected4_1 = ((reply2?.metric_series?.locator === 'address' ? reply2?.metric_series.address : undefined)?.postal_code ?? '');
        break;
      default:
        selected4_1 = '';
    }
    let selected4_2;
    switch (reply2?.metric_series?.locator) {
      case 'address':
        selected4_2 = ((reply2?.metric_series?.locator === 'address' ? reply2?.metric_series.address : undefined)?.country_code ?? '');
        break;
      default:
        selected4_2 = '';
    }
    let selected4_3;
    switch (reply2?.metric_series?.locator) {
      case 'address':
        selected4_3 = ((reply2?.metric_series?.locator === 'address' ? reply2?.metric_series.address : undefined)?.lines ?? []);
        break;
      default:
        selected4_3 = [];
    }
    const request4 = {
      context: request?.context,
      sponsorship_deal: {
        billing_address: {
          country_code: selected4_2,
          lines: selected4_3,
          postal_code: selected4_1,
        },
        lines: [{
          quantity: (reply2?.metric_series?.components?.primary?.quantity ?? '0'),
        }],
      },
    };
    const reply4 = await call((done) => clients.sponsorshipsSponsorshipDealWorkflowClient.pauseSponsorshipDealWorkflow(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          message: (reply1?.result?.message ?? ''),
        },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        result: {
          details: (reply4?.result?.details ?? {}),
          status: (reply4?.outcome?.status ?? ''),
        },
      },
      { period: request?.details?.period },
    );
  }

  /**
   * Lists concession orders a page at a time.
   */
  async function listConcessionOrderReports(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListConcessionOrderReports', request);
    logger.info('list_concession_order_reports', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: {
        audit: { updated_at: request?.filter?.created_after },
        contact: { email: (request?.context?.actor?.email ?? '') },
        external_account_id: (request?.filter?.owner_id ?? ''),
        labels: { ...(request?.filter?.labels ?? {}) },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.broadcastsStreamKeyWorkflowClient.advanceStreamKeyWorkflow(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      concession_stand: {
        audit: { created_by: (reply1?.stream_key?.audit?.created_by ?? '') },
        owner: { display_name: (reply1?.stream_key?.holder?.display_name ?? '') },
      },
      context: input2?.context,
      details: { contact: { email: (reply1?.stream_key?.contact?.email ?? '') } },
    };
    const reply2 = await call((done) => clients.concessionsConcessionStandValidationClient.lintConcessionStands(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      { result: { status: (reply1?.outcome?.status ?? '') } },
      {
        concession_orders: [{ audit: { created_at: reply2?.concession_stand?.audit?.created_at } }],
      },
    );
  }

  /**
   * Schedules a concession order.
   */
  async function scheduleConcessionOrderReport(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'ScheduleConcessionOrderReport', request);
    logger.info('schedule_concession_order_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      menu_item: {
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
      reason: (request?.reason ?? ''),
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.concessionsMenuItemCalculationClient.simulateMenuItem(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_stand: {
        address: {
          locality: ((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.locality ?? ''),
        },
      },
      context: request?.context,
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.concessionsConcessionStandValidationClient.explainConcessionStandDecision(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      { result: { status: (reply2?.result?.status ?? '') } },
      { period: request?.details?.period },
    );
  }

  /**
   * Shares a concession order.
   */
  async function shareConcessionOrderReport(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'ShareConcessionOrderReport', request);
    logger.info('share_concession_order_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          email: (request?.details?.recipient?.email ?? ''),
          preferred_channel: (request?.details?.recipient?.preferred_channel ?? ''),
        },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.concessionsConcessionOrderValidationClient.validateConcessionOrder(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_stand: {
        address: {
          country_code: (reply1?.concession_order?.billing_address?.country_code ?? ''),
          recipient: (reply1?.concession_order?.billing_address?.recipient ?? ''),
        },
        owner: { email: (request?.details?.recipient?.email ?? '') },
      },
      context: request?.context,
    };
    const reply2 = await withRetry(() => call((done) => clients.concessionsConcessionStandCalculationClient.explainConcessionStandCalculation(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_order: { counterparty: reply1?.concession_order?.counterparty },
      },
      {
        concession_order: {
          billing_address: {
            locality: ((reply2?.concession_stand?.locator === 'address' ? reply2?.concession_stand.address : undefined)?.locality ?? ''),
          },
          lines: [{
            description: (reply2?.concession_stand?.components?.primary?.description ?? ''),
          }],
        },
      },
      {
        concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      },
    );
  }

  /**
   * Streams the changes of concession orders as they happen.
   */
  async function streamConcessionOrderMetrics(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'StreamConcessionOrderMetrics', request);
    logger.info('stream_concession_order_metrics', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      expense_report: {
        billing_address: { region: (request?.filter?.region ?? '') },
        counterparty: { email: (request?.subscriber?.email ?? '') },
      },
    };
    const task1 = deferred(() => call((done) => clients.expensesExpenseReportClient.updateExpenseReport(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      sponsorship_deal: { audit: { etag: (reply1?.expense_report?.audit?.etag ?? '') } },
    };
    const reply2 = await bus.request('acme.concessions.v1.ConcessionOrderReportingService/StreamConcessionOrderMetrics:1', request2);
    const request3 = {
      context: request?.context,
      user_id: (request?.context?.actor?.user_id ?? ''),
    };
    const reply3 = await runInContext(request.context, () => call((done) => clients.identityUserQueryClient.lookupUser(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const request4 = {
      concession_stand: {
        address: { country_code: (reply3?.user?.primary_address?.country_code ?? '') },
        audit: { updated_by: (reply2?.sponsorship_deal?.audit?.updated_by ?? '') },
        owner: { email: (reply3?.user?.contact?.email ?? '') },
      },
      context: request?.context,
      details: { contact: { phone: (reply3?.user?.contact?.phone ?? '') } },
    };
    const reply4 = await call((done) => clients.concessionsConcessionStandValidationClient.checkConcessionStandEligibility(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        concession_order: {
          billing_address: {
            country_code: (reply2?.sponsorship_deal?.billing_address?.country_code ?? ''),
          },
        },
      },
      {
        concession_order: {
          billing_address: { locality: (reply3?.user?.primary_address?.locality ?? '') },
        },
        result: { status: (reply3?.result?.status ?? '') },
      },
      {
        concession_order: {
          billing_address: {
            region: ((reply4?.concession_stand?.locator === 'address' ? reply4?.concession_stand.address : undefined)?.region ?? ''),
          },
        },
        result: { details: (reply4?.result?.details ?? {}) },
      },
    );
  }

  return {
    countConcessionOrderTotals: unary(countConcessionOrderTotals),
    deleteConcessionOrderReport: unary(deleteConcessionOrderReport),
    fetchConcessionOrderReport: unary(fetchConcessionOrderReport),
    generateConcessionOrderReport: unary(generateConcessionOrderReport),
    listConcessionOrderReports: unary(listConcessionOrderReports),
    scheduleConcessionOrderReport: unary(scheduleConcessionOrderReport),
    shareConcessionOrderReport: unary(shareConcessionOrderReport),
    streamConcessionOrderMetrics: serverStream(streamConcessionOrderMetrics),
  };
}

module.exports = { createConcessionOrderReportingHandlers };
