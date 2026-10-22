'use strict';

/**
 * Answers read-only questions about tour stops, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve TourStopQueryService, recording every operation of the tours team.
 */
class TourStopQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'TourStopQueryService' });
  }

  /**
   * Lists tour stops a page at a time.
   */
  browseTourStops(call, callback) {
    this.executeBrowseTourStops(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseTourStops(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseTourStops', request);
    this.logger.info('browse_tour_stops', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      tour_budget: {
        contact: { email: (input1?.context?.actor?.email ?? '') },
        labels: { ...(input1?.filter?.labels ?? {}) },
      },
    };
    const reply1 = await call((done) => this.clients.toursTourBudgetValidationClient.previewTourBudget(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      admission: {
        subject: { display_name: (reply1?.tour_budget?.holder?.display_name ?? '') },
      },
      context: request?.context,
      details: {
        contact: {
          email: (reply1?.tour_budget?.contact?.email ?? ''),
          phone: (reply1?.tour_budget?.contact?.phone ?? ''),
          preferred_channel: (reply1?.tour_budget?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.admissionsAdmissionValidationClient.validateAdmission(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        tour_stops: [{
          participants: [{ contact: { phone: (reply1?.tour_budget?.contact?.phone ?? '') } }],
        }],
      },
      {
        result: {
          correlation_id: (reply2?.admission?.correlation_id ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
    );
  }

  /**
   * Counts tour stops by status.
   */
  countTourStops(call, callback) {
    this.executeCountTourStops(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountTourStops(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountTourStops', request);
    this.logger.info('count_tour_stops', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { query: (request?.filter?.query ?? '') },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.venuesVenueSectionQueryClient.browseVenueSections(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        details: (reply1?.result?.details ?? {}),
        status: (reply1?.result?.status ?? ''),
      },
    };
  }

  /**
   * Streams every matching tour stop.
   */
  exportTourStops(call) {
    serverStream((request) => this.executeExportTourStops(request))(call);
  }

  async executeExportTourStops(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportTourStops', request);
    this.logger.info('export_tour_stops', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
      metric_series: {
        address: { region: (request?.filter?.region ?? '') },
        owner: { user_id: (request?.context?.actor?.user_id ?? '') },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.metricsMetricSeriesValidationClient.previewMetricSeries(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      tour_budget: {
        audit: {
          created_at: reply1?.metric_series?.audit?.created_at,
          etag: (reply1?.metric_series?.audit?.etag ?? ''),
        },
      },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.toursTourBudgetValidationClient.scoreTourBudget(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      {
        tour_stop: {
          organizer: { display_name: (reply1?.metric_series?.owner?.display_name ?? '') },
        },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      { tour_stop: { location: { region: (request?.filter?.region ?? '') } } },
    );
  }

  /**
   * Lists tour stops a page at a time.
   */
  findTourStops(call, callback) {
    this.executeFindTourStops(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindTourStops(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindTourStops', request);
    this.logger.info('find_tour_stops', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { query: (request?.filter?.query ?? '') },
    };
    const task1 = deferred(() => call((done) => this.clients.identityUserQueryClient.browseUsers(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      venue_policy: {
        audit: { updated_at: request?.filter?.created_after },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
        scopes: [...(reply1?.result?.reference_ids ?? [])],
      },
    };
    const reply2 = await this.bus.request('acme.tours.v1.TourStopQueryService/FindTourStops:1', request2);
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      { page_info: { total_size: (reply1?.page_info?.total_size ?? '0') } },
      {
        tour_stops: [{
          audit: { created_by: (reply2?.venue_policy?.audit?.created_by ?? '') },
          organizer: { display_name: (reply2?.venue_policy?.owner?.display_name ?? '') },
        }],
      },
    );
  }

  /**
   * Returns a tour stop.
   */
  lookupTourStop(call, callback) {
    this.executeLookupTourStop(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupTourStop(request) {
    validateContext(request.context);
    if (!request.tour_stop_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tour_stop_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupTourStop', request);
    this.logger.info('lookup_tour_stop', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.tour_stop_id ?? ''),
      },
    };
    const messages1 = await collect(this.clients.toursTourQueryClient.exportTours(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.toursTourBudgetValidationClient.normalizeTourBudget(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    return assemble(
      {
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
        tour_stop: {
          organizer: { display_name: (reply2?.tour_budget?.holder?.display_name ?? '') },
        },
      },
      { tour_stop: { tour_stop_id: (request?.tour_stop_id ?? '') } },
    );
  }

  /**
   * Returns a tour stop.
   */
  peekTourStop(call, callback) {
    this.executePeekTourStop(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekTourStop(request) {
    validateContext(request.context);
    if (!request.tour_stop_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tour_stop_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekTourStop', request);
    this.logger.info('peek_tour_stop', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
    };
    const reply1 = await call((done) => this.clients.toursTourBudgetValidationClient.previewTourBudget(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.tour_budget?.audit?.updated_at,
        labels: { ...(reply1?.tour_budget?.labels ?? {}) },
        owner_id: (reply1?.tour_budget?.tour_budget_id ?? ''),
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      this.clients.toursTourQueryClient.findTours(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      { tour_stop: { tour_stop_id: (request?.tour_stop_id ?? '') } },
    );
  }

  /**
   * Returns several tour stops at once.
   */
  resolveTourStops(call, callback) {
    this.executeResolveTourStops(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveTourStops(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveTourStops', request);
    this.logger.info('resolve_tour_stops', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
      tour_budget: { audit: { updated_at: request?.context?.deadline } },
    };
    const previewTourBudget1 = promisify(this.clients.toursTourBudgetValidationClient.previewTourBudget.bind(this.clients.toursTourBudgetValidationClient));
    const reply1 = await previewTourBudget1(request1, metadataFor(request1.context), { deadline: deadline() });
    return {
      tour_stops: [{
        audit: { etag: (reply1?.tour_budget?.audit?.etag ?? '') },
        participants: [{
          contact: {
            preferred_channel: (reply1?.tour_budget?.contact?.preferred_channel ?? ''),
          },
        }],
      }],
    };
  }

  /**
   * Lists tour stops a page at a time.
   */
  suggestTourStops(call, callback) {
    this.executeSuggestTourStops(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuggestTourStops(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SuggestTourStops', request);
    this.logger.info('suggest_tour_stops', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { query: (request?.filter?.query ?? '') },
      page: { page_size: (request?.page?.page_size ?? 0) },
    };
    const reply1 = await call((done) => this.clients.admissionsGateScanQueryClient.browseGateScans(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      metric_series: { address: { region: (request?.filter?.region ?? '') } },
    };
    const method2 = 'checkMetricSeriesEligibility';
    const reply2 = await call((done) => this.clients.metricsMetricSeriesValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          message: (reply1?.result?.message ?? ''),
        },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
    );
  }

  /**
   * Summarizes a tour stop.
   */
  summarizeTourStops(call, callback) {
    this.executeSummarizeTourStops(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeTourStops(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeTourStops', request);
    this.logger.info('summarize_tour_stops', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { locale: (request?.context?.locale ?? '') },
      reason: (request?.reason ?? ''),
      tour_budget: { contact: { email: (request?.context?.actor?.email ?? '') } },
    };
    const client1 = this.clients.registry.get('acme.tours.v1.TourBudgetValidationService');
    const reply1 = await call((done) => client1.lintTourBudgets(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (reply1?.tour_budget?.contact?.preferred_channel ?? ''),
        },
      },
      tour_budget: {
        audit: { etag: (reply1?.tour_budget?.audit?.etag ?? '') },
        bank: {
          account_holder: ((reply1?.tour_budget?.funding === 'bank' ? reply1?.tour_budget.bank : undefined)?.account_holder ?? ''),
        },
      },
    };
    const send2 = this.clients.toursTourBudgetValidationClient.checkTourBudgetEligibility.bind(this.clients.toursTourBudgetValidationClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          details: (reply2?.result?.details ?? {}),
        },
      },
      { period: request?.details?.period },
    );
  }
}

module.exports = { TourStopQueryService };
