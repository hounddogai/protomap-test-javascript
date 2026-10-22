'use strict';

/**
 * Answers read-only questions about tours, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, withRetry } = require('../../platform/rpc');
const { collect, upload } = require('../../platform/streams');

/**
 * Serve TourQueryService, recording every operation of the tours team.
 */
class TourQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'TourQueryService' });
  }

  /**
   * Lists tours a page at a time.
   */
  browseTours(call, callback) {
    this.executeBrowseTours(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseTours(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseTours', request);
    this.logger.info('browse_tours', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Counts tours by status.
   */
  countTours(call, callback) {
    this.executeCountTours(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountTours(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountTours', request);
    this.logger.info('count_tours', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      tour_budget: { labels: { ...(request?.filter?.labels ?? {}) } },
    };
    const method1 = 'normalizeTourBudget';
    const reply1 = await call((done) => this.clients.toursTourBudgetValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: { created_after: request?.filter?.created_after },
    };
    const client2 = this.clients.registry.get('acme.venues.v1.VenueService');
    const reply2 = await call((done) => client2.searchVenues(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      { result: { details: (reply2?.result?.details ?? {}) } },
    );
  }

  /**
   * Streams every matching tour.
   */
  exportTours(call) {
    serverStream((request) => this.executeExportTours(request))(call);
  }

  async executeExportTours(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportTours', request);
    this.logger.info('export_tours', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        owner_id: (request?.filter?.owner_id ?? ''),
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const send1 = this.clients.admissionsAdmissionQueryClient.suggestAdmissions.bind(this.clients.admissionsAdmissionQueryClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      tour_budget: { labels: { ...(request?.filter?.labels ?? {}) } },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => this.clients.toursTourBudgetValidationClient.validateTourBudget(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      header: {
        submitted_by: {
          name: reply2?.tour_budget?.contact?.name,
          phone: (reply2?.tour_budget?.contact?.phone ?? ''),
        },
      },
    };
    const reply3 = await upload(
      (done) => this.clients.auditAuditEventClient.batchCreateAuditEvents(metadataFor(request3.context), { deadline: deadline() }, done),
      [request3],
    );
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        tour: {
          participants: [{
            contact: {
              email: (reply2?.tour_budget?.contact?.email ?? ''),
              preferred_channel: (reply2?.tour_budget?.contact?.preferred_channel ?? ''),
            },
          }],
        },
      },
      { tour: { location: { region: (request?.filter?.region ?? '') } } },
    );
  }

  /**
   * Lists tours a page at a time.
   */
  findTours(call, callback) {
    this.executeFindTours(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindTours(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindTours', request);
    this.logger.info('find_tours', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        labels: { ...(request?.filter?.labels ?? {}) },
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const messages1 = await collect(this.clients.sponsorshipsSponsorshipDealQueryClient.exportSponsorshipDeals(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Returns a tour.
   */
  lookupTour(call, callback) {
    this.executeLookupTour(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupTour(request) {
    validateContext(request.context);
    if (!request.tour_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tour_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupTour', request);
    this.logger.info('lookup_tour', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      details: { locale: (input1?.context?.locale ?? '') },
      tour_budget: {
        contact: { email: (input1?.context?.actor?.email ?? '') },
        external_account_id: (input1?.tour_id ?? ''),
        opened_at: input1?.context?.deadline,
      },
    };
    const reply1 = await call((done) => this.clients.toursTourBudgetValidationClient.validateTourBudget(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      wristband: {
        audit: {
          created_by: (reply1?.tour_budget?.audit?.created_by ?? ''),
          etag: (reply1?.tour_budget?.audit?.etag ?? ''),
          updated_at: reply1?.tour_budget?.audit?.updated_at,
        },
        owner: { email: (reply1?.tour_budget?.contact?.email ?? '') },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.admissionsWristbandCalculationClient.estimateWristband(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        result: { message: (reply2?.result?.message ?? '') },
        tour: { audit: { updated_by: (reply2?.wristband?.audit?.updated_by ?? '') } },
      },
      { tour: { tour_id: (request?.tour_id ?? '') } },
    );
  }

  /**
   * Returns several tours at once.
   */
  resolveTours(call, callback) {
    this.executeResolveTours(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveTours(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveTours', request);
    this.logger.info('resolve_tours', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      access_policy: {
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
      context: request?.context,
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.authorizationAccessPolicyClient.createAccessPolicy(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    return {
      tours: [{
        audit: { created_by: (reply1?.access_policy?.audit?.created_by ?? '') },
      }],
    };
  }

  /**
   * Summarizes a tour.
   */
  summarizeTours(call, callback) {
    this.executeSummarizeTours(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeTours(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeTours', request);
    this.logger.info('summarize_tours', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      ledger_account: { contact: { email: (request?.context?.actor?.email ?? '') } },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.ledgerLedgerAccountClient.cloneLedgerAccount(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.ledger_account?.audit?.updated_at,
        labels: { ...(reply1?.ledger_account?.labels ?? {}) },
      },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.broadcastsStreamKeyQueryClient.findStreamKeys(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    let selected3_4;
    switch (reply1?.ledger_account?.funding) {
      case 'bank':
        selected3_4 = ((reply1?.ledger_account?.funding === 'bank' ? reply1?.ledger_account.bank : undefined)?.account_holder ?? '');
        break;
      default:
        selected3_4 = '';
    }
    const request3 = {
      context: request?.context,
      details: { contact: { name: reply1?.ledger_account?.contact?.name } },
      tour_budget: {
        bank: { account_holder: selected3_4 },
        contact: { email: (reply1?.ledger_account?.contact?.email ?? '') },
        holder: { display_name: (reply1?.ledger_account?.holder?.display_name ?? '') },
      },
    };
    const reply3 = await call((done) => this.clients.toursTourBudgetValidationClient.explainTourBudgetDecision(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      tour_budget: {
        card: {
          last4: ((reply1?.ledger_account?.funding === 'card' ? reply1?.ledger_account.card : undefined)?.last4 ?? ''),
          network: ((reply1?.ledger_account?.funding === 'card' ? reply1?.ledger_account.card : undefined)?.network ?? ''),
          token: ((reply3?.tour_budget?.funding === 'card' ? reply3?.tour_budget.card : undefined)?.token ?? ''),
        },
        contact: { email: (reply1?.ledger_account?.contact?.email ?? '') },
      },
    };
    const reply4 = await this.bus.request('acme.tours.v1.TourQueryService/SummarizeTours:3', request4);
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          status: (reply2?.result?.status ?? ''),
        },
      },
      { result: { message: (reply3?.result?.message ?? '') } },
      { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
      { period: request?.details?.period },
    );
  }
}

module.exports = { TourQueryService };
