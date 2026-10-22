'use strict';

/**
 * Answers read-only questions about shows, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of ShowQueryService, over the dependencies the shows team is given.
 */
function createShowQueryHandlers({ store, clients, bus, logger }) {
  /**
   * Lists shows a page at a time.
   */
  async function browseShows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'BrowseShows', request);
    logger.info('browse_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        owner_id: (request?.filter?.owner_id ?? ''),
        region: (request?.filter?.region ?? ''),
      },
      page: { page_size: (request?.page?.page_size ?? 0) },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.auditAuditEventClient.listAuditEvents(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        message: (reply1?.result?.message ?? ''),
      },
    };
  }

  /**
   * Counts shows by status.
   */
  async function countShows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'CountShows', request);
    logger.info('count_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.context?.baggage ?? {}) },
      context: request?.context,
      details: {
        recipient: { email: (request?.context?.actor?.email ?? '') },
        variables: { ...(request?.filter?.labels ?? {}) },
      },
    };
    const reply1 = await call((done) => clients.venuesVenuePolicyNotificationClient.subscribeVenuePolicyUpdates(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      performance: {
        audit: { created_at: reply1?.venue_policy?.audit?.created_at },
        organizer: {
          email: (reply1?.venue_policy?.owner?.email ?? ''),
          user_id: (reply1?.venue_policy?.owner?.user_id ?? ''),
        },
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      clients.showsPerformanceCalculationClient.calculatePerformance(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          message: (reply1?.outcome?.message ?? ''),
        },
      },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          status: (reply2?.result?.status ?? ''),
        },
      },
    );
  }

  /**
   * Streams every matching show.
   */
  async function exportShows(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'ExportShows', request);
    logger.info('export_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        location: { region: (request?.filter?.region ?? '') },
        participants: [{ contact: { email: (request?.context?.actor?.email ?? '') } }],
      },
    };
    const recalculateShows1 = promisify(clients.showsShowCalculationClient.recalculateShows.bind(clients.showsShowCalculationClient));
    const reply1 = await recalculateShows1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      details: {
        destination: {
          country_code: (reply1?.show?.location?.country_code ?? ''),
          recipient: (reply1?.show?.location?.recipient ?? ''),
        },
      },
      performance: {
        daily: {
          interval: ((reply1?.show?.recurrence === 'daily' ? reply1?.show.daily : undefined)?.interval ?? 0),
        },
      },
    };
    const reply2 = await call((done) => clients.showsPerformanceCalculationClient.recalculatePerformances(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        owner_id: (request?.filter?.owner_id ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const messages3 = await collect(clients.broadcastsStreamKeyQueryClient.exportStreamKeys(request3, metadataFor(request3.context), { deadline: deadline() }));
    const reply3 = messages3.at(-1) ?? {};
    const request4 = {
      context: request?.context,
      performance: {
        participants: [{ contact: { email: (reply1?.show?.organizer?.email ?? '') } }],
      },
    };
    const method4 = 'comparePerformanceOptions';
    const reply4 = await call((done) => clients.showsPerformanceCalculationClient[method4](request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      {
        show: { organizer: { email: (reply1?.show?.organizer?.email ?? '') } },
      },
      {
        show: {
          location: { postal_code: (reply2?.performance?.location?.postal_code ?? '') },
          organizer: reply2?.performance?.organizer,
        },
      },
      {
        show: {
          location: { country_code: (reply4?.performance?.location?.country_code ?? '') },
        },
      },
      { show: { location: { region: (request?.filter?.region ?? '') } } },
    );
  }

  /**
   * Lists shows a page at a time.
   */
  async function findShows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'FindShows', request);
    logger.info('find_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      performance: {
        location: { region: (request?.filter?.region ?? '') },
        participants: [{ contact: { email: (request?.context?.actor?.email ?? '') } }],
      },
    };
    const client1 = clients.registry.get('acme.shows.v1.PerformanceCalculationService');
    const reply1 = await call((done) => client1.recalculatePerformances(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } };
  }

  /**
   * Returns a show.
   */
  async function lookupShow(request) {
    validateContext(request.context);
    if (!request.show_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'show_id is required');
    }
    await store.record(request.context.tenant_id, 'LookupShow', request);
    logger.info('lookup_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { locale: (request?.context?.locale ?? '') },
      performance: {
        organizer: { user_id: (request?.context?.actor?.user_id ?? '') },
        participants: [{ contact: { email: (request?.context?.actor?.email ?? '') } }],
      },
    };
    const send1 = clients.showsPerformanceValidationClient.validatePerformance.bind(clients.showsPerformanceValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      performance: {
        audit: { etag: (reply1?.performance?.audit?.etag ?? '') },
        location: {
          locality: (reply1?.performance?.location?.locality ?? ''),
          recipient: (reply1?.performance?.location?.recipient ?? ''),
        },
        participants: [{ contact: { email: (reply1?.performance?.organizer?.email ?? '') } }],
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.showsPerformanceCalculationClient.estimatePerformance(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    return assemble(
      {
        show: {
          location: { recipient: (reply1?.performance?.location?.recipient ?? '') },
        },
      },
      {
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
        show: {
          organizer: { display_name: (reply2?.performance?.organizer?.display_name ?? '') },
        },
      },
      { show: { show_id: (request?.show_id ?? '') } },
    );
  }

  /**
   * Returns a show.
   */
  async function peekShow(request) {
    validateContext(request.context);
    if (!request.show_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'show_id is required');
    }
    await store.record(request.context.tenant_id, 'PeekShow', request);
    logger.info('peek_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      details: { filters: { ...(input1?.context?.baggage ?? {}) } },
    };
    const reply1 = await call((done) => clients.venuesVenueSectionExportClient.scheduleVenueSectionExport(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      { show: { show_id: (request?.show_id ?? '') } },
    );
  }

  /**
   * Returns several shows at once.
   */
  async function resolveShows(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'ResolveShows', request);
    logger.info('resolve_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      performance: {
        organizer: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.showsPerformanceCalculationClient.comparePerformanceOptions(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    return {
      shows: [{
        organizer: { user_id: (reply1?.performance?.organizer?.user_id ?? '') },
      }],
    };
  }

  /**
   * Lists shows a page at a time.
   */
  async function suggestShows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'SuggestShows', request);
    logger.info('suggest_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.context?.baggage ?? {}) },
      context: request?.context,
      details: {
        recipient: { email: (request?.context?.actor?.email ?? '') },
        variables: { ...(request?.filter?.labels ?? {}) },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.venuesVenueNotificationClient.subscribeVenueUpdates(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      box_office_sale: { audit: { created_by: (reply1?.venue?.audit?.created_by ?? '') } },
      context: request?.context,
      details: {
        destination: {
          locality: ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.locality ?? ''),
        },
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.recalculateBoxOfficeSales(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        shows: [{
          participants: [{ contact: { email: (reply1?.venue?.owner?.email ?? '') } }],
        }],
      },
      {
        result: { message: (reply2?.result?.message ?? '') },
        shows: [{
          location: {
            postal_code: (reply2?.box_office_sale?.billing_address?.postal_code ?? ''),
          },
        }],
      },
    );
  }

  /**
   * Summarizes a show.
   */
  async function summarizeShows(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'SummarizeShows', request);
    logger.info('summarize_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const reply1 = await withRetry(() => call((done) => clients.showsLineupCalculationClient.recalculateLineups(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      performance: {
        audit: { created_at: reply1?.lineup?.audit?.created_at },
        location: { country_code: (reply1?.lineup?.billing_address?.country_code ?? '') },
        organizer: { display_name: (reply1?.lineup?.counterparty?.display_name ?? '') },
      },
    };
    const task2 = deferred(() => call((done) => clients.showsPerformanceValidationClient.normalizePerformance(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      { period: request?.details?.period },
    );
  }

  return {
    browseShows: unary(browseShows),
    countShows: unary(countShows),
    exportShows: serverStream(exportShows),
    findShows: unary(findShows),
    lookupShow: unary(lookupShow),
    peekShow: unary(peekShow),
    resolveShows: unary(resolveShows),
    suggestShows: unary(suggestShows),
    summarizeShows: unary(summarizeShows),
  };
}

module.exports = { createShowQueryHandlers };
