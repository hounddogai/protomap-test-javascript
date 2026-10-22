'use strict';

/**
 * Routes performances through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve PerformanceApprovalService, recording every operation of the shows team.
 */
class PerformanceApprovalService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'PerformanceApprovalService' });
  }

  /**
   * Approves a performance.
   */
  approvePerformance(call, callback) {
    this.executeApprovePerformance(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeApprovePerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'ApprovePerformance', request);
    this.logger.info('approve_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.performance?.audit?.created_at,
        labels: { ...(request?.performance?.metadata ?? {}) },
        owner_id: (request?.performance?.organizer?.user_id ?? ''),
        region: (request?.performance?.location?.region ?? ''),
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.boxofficeBoxOfficeSaleApprovalClient.listPendingBoxOfficeSales(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const settled2 = await Promise.allSettled((request?.performance?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        filter: {
          created_after: request?.performance?.audit?.created_at,
          labels: { ...(request?.performance?.metadata ?? {}) },
          owner_id: (item?.participant_id ?? ''),
          region: (request?.performance?.location?.region ?? ''),
        },
      };
      const reply2 = await call((done) => this.clients.seatingSeatAssignmentApprovalClient.listPendingSeatAssignments(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    const results3 = [];
    for (const item of (request?.performance?.participants ?? [])) {
      const request3 = {
        context: request?.context,
        details: {
          notify: {
            name: item?.contact?.name,
            phone: (item?.contact?.phone ?? ''),
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply3 = await call((done) => this.clients.ticketingTicketWorkflowClient.retryTicketWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      results3.push(reply3);
    }
    const reply3 = results3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        performance: {
          organizer: request?.performance?.organizer,
          performance_id: (request?.performance_id ?? ''),
        },
      },
    );
  }

  /**
   * Comments a performance.
   */
  commentPerformanceApproval(call, callback) {
    this.executeCommentPerformanceApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCommentPerformanceApproval(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'CommentPerformanceApproval', request);
    this.logger.info('comment_performance_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { approver: { user_id: (request?.details?.approver?.user_id ?? '') } },
      show: {
        daily: {
          interval: ((request?.performance?.recurrence === 'weekly' ? request?.performance.weekly : undefined)?.interval ?? 0),
        },
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.showsShowApprovalClient.escalateShow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        owner_id: (reply1?.show?.show_id ?? ''),
        region: (request?.performance?.location?.region ?? ''),
      },
    };
    const task2 = deferred(() => call((done) => this.clients.helpdeskTicketQueryClient.browseTickets(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    const request3 = {
      context: request?.context,
      filter: { region: (request?.performance?.location?.region ?? '') },
    };
    const reply3 = await this.bus.request('acme.shows.v1.PerformanceApprovalService/CommentPerformanceApproval:2', request3);
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply1?.outcome?.status ?? '') } },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      {
        outcome: { message: (reply3?.result?.message ?? '') },
        result: { message: (reply3?.result?.message ?? '') },
      },
      {
        performance: {
          location: request?.performance?.location,
          performance_id: (request?.performance_id ?? ''),
        },
      },
    );
  }

  /**
   * Delegates a performance.
   */
  delegatePerformanceApproval(call, callback) {
    this.executeDelegatePerformanceApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDelegatePerformanceApproval(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'DelegatePerformanceApproval', request);
    this.logger.info('delegate_performance_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.performance?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        details: { contact: { name: item?.contact?.name } },
      };
      const reply1 = await call((done) => this.clients.showsPerformanceValidationClient.lintPerformances(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const settled2 = await Promise.allSettled((request?.performance?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        details: {
          contact: {
            email: (item?.contact?.email ?? ''),
            phone: (item?.contact?.phone ?? ''),
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply2 = await call((done) => this.clients.showsLineupValidationClient.normalizeLineup(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    const results3 = [];
    for (const item of (request?.performance?.participants ?? [])) {
      const request3 = {
        context: request?.context,
        filter: {
          owner_id: (item?.participant_id ?? ''),
          region: (reply1?.performance?.location?.region ?? ''),
        },
      };
      const reply3 = await call((done) => this.clients.showsShowClient.listShows(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      results3.push(reply3);
    }
    const reply3 = results3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      performance: {
        performance_id: (request?.performance_id ?? ''),
        status: (request?.performance?.status ?? ''),
      },
    };
  }

  /**
   * Escalates a performance.
   */
  escalatePerformance(call, callback) {
    this.executeEscalatePerformance(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEscalatePerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'EscalatePerformance', request);
    this.logger.info('escalate_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.performance?.location?.region ?? '') },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.venuesVenueClient.searchVenues(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const results2 = await Promise.all((request?.performance?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        yard: {
          address: {
            country_code: (request?.performance?.location?.country_code ?? ''),
            lines: [...(request?.performance?.location?.lines ?? [])],
          },
          audit: { updated_at: request?.performance?.audit?.updated_at },
          owner: { email: (item?.contact?.email ?? '') },
        },
      };
      const reply2 = await call((done) => this.clients.yardsYardClient.cloneYard(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      route: {
        lines: [{ description: (reply2?.yard?.components?.primary?.description ?? '') }],
      },
    };
    const reply3 = await call((done) => this.clients.routingRouteClient.createRoute(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      show: {
        participants: [{ contact: { email: (request?.details?.approver?.email ?? '') } }],
        weekly: {
          interval: ((request?.performance?.recurrence === 'weekly' ? request?.performance.weekly : undefined)?.interval ?? 0),
        },
      },
    };
    const reply4 = await new Promise((resolve, reject) => {
      this.clients.showsShowApprovalClient.escalateShow(request4, metadataFor(request4.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        performance: {
          location: { postal_code: (reply3?.route?.billing_address?.postal_code ?? '') },
        },
      },
      {
        performance: { audit: { updated_by: (reply4?.show?.audit?.updated_by ?? '') } },
      },
      {
        performance: {
          metadata: (request?.performance?.metadata ?? {}),
          performance_id: (request?.performance_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a performance.
   */
  getPerformanceApprovalStatus(call, callback) {
    this.executeGetPerformanceApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetPerformanceApprovalStatus(request) {
    validateContext(request.context);
    if (!request.performance_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performance_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetPerformanceApprovalStatus', request);
    this.logger.info('get_performance_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      feature_flag: { owner: { email: (request?.context?.actor?.email ?? '') } },
    };
    const cloneFeatureFlag1 = promisify(this.clients.featureflagsFeatureFlagClient.cloneFeatureFlag.bind(this.clients.featureflagsFeatureFlagClient));
    const reply1 = await cloneFeatureFlag1(request1, metadataFor(request1.context), { deadline: deadline() });
    let selected2_1;
    switch (reply1?.feature_flag?.action) {
      case 'discount':
        selected2_1 = (reply1?.feature_flag?.action === 'discount' ? reply1?.feature_flag.discount : undefined)?.amount;
        break;
      default:
        selected2_1 = null;
    }
    const request2 = {
      concession_order: { lines: [{ tax: { amount: selected2_1 } }] },
      context: request?.context,
      details: { notify: { email: (reply1?.feature_flag?.owner?.email ?? '') } },
    };
    const reply2 = await call((done) => this.clients.concessionsConcessionOrderWorkflowClient.startConcessionOrderWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      {
        performance: {
          audit: { created_at: reply1?.feature_flag?.audit?.created_at },
          organizer: { display_name: (reply1?.feature_flag?.owner?.display_name ?? '') },
        },
      },
      {
        performance: {
          location: { lines: (reply2?.concession_order?.billing_address?.lines ?? []) },
        },
        result: { status: (reply2?.result?.status ?? '') },
      },
      { performance: { performance_id: (request?.performance_id ?? '') } },
    );
  }

  /**
   * Lists performances a page at a time.
   */
  listPendingPerformances(call, callback) {
    this.executeListPendingPerformances(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListPendingPerformances(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListPendingPerformances', request);
    this.logger.info('list_pending_performances', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { approver: { user_id: (request?.context?.actor?.user_id ?? '') } },
      show: {
        audit: { updated_at: request?.filter?.created_after },
        location: { region: (request?.filter?.region ?? '') },
        participants: [{ contact: { email: (request?.context?.actor?.email ?? '') } }],
      },
    };
    const method1 = 'approveShow';
    const reply1 = await call((done) => this.clients.showsShowApprovalClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      performances: [{ location: reply1?.show?.location }],
      result: { status: (reply1?.outcome?.status ?? '') },
    };
  }

  /**
   * Recalls a performance.
   */
  recallPerformance(call, callback) {
    this.executeRecallPerformance(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecallPerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'RecallPerformance', request);
    this.logger.info('recall_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.performance?.location?.region ?? '') },
    };
    const client1 = this.clients.registry.get('acme.featureflags.v1.FeatureFlagService');
    const reply1 = await call((done) => client1.listFeatureFlags(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: { region: (request?.performance?.location?.region ?? '') },
    };
    const send2 = this.clients.seatingSeatMapApprovalClient.listPendingSeatMaps.bind(this.clients.seatingSeatMapApprovalClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          status: (reply2?.result?.status ?? ''),
        },
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
   * Rejects a performance.
   */
  rejectPerformance(call, callback) {
    this.executeRejectPerformance(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRejectPerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'RejectPerformance', request);
    this.logger.info('reject_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(request?.performance?.metadata ?? {}) },
        owner_id: (request?.performance?.performance_id ?? ''),
        region: (request?.performance?.location?.region ?? ''),
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.helpdeskTicketQueryClient.browseTickets(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      show: {
        participants: [{ contact: { email: (input2?.details?.approver?.email ?? '') } }],
        weekly: {
          days: [...((input2?.performance?.recurrence === 'weekly' ? input2?.performance.weekly : undefined)?.days ?? [])],
        },
      },
    };
    const reply2 = await call((done) => this.clients.showsShowApprovalClient.rejectShow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const settled3 = await Promise.allSettled((request?.performance?.participants ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        details: {
          notify: {
            name: item?.contact?.name,
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply3 = await call((done) => this.clients.concessionsConcessionOrderWorkflowClient.startConcessionOrderWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const results3 = settled3.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply3 = results3.at(-1) ?? {};
    const request4 = {
      context: request?.context,
      route: {
        audit: {
          created_by: (reply3?.concession_order?.audit?.created_by ?? ''),
          updated_by: (request?.performance?.audit?.updated_by ?? ''),
        },
      },
    };
    const cloned4 = JSON.parse(JSON.stringify(request4));
    const reply4 = await call((done) => this.clients.routingRouteClient.createRoute(cloned4, metadataFor(cloned4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      {
        performance: {
          location: { region: (reply2?.show?.location?.region ?? '') },
          organizer: { display_name: (reply2?.show?.organizer?.display_name ?? '') },
        },
      },
      { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
      {
        performance: {
          performance_id: (request?.performance_id ?? ''),
          status: (request?.performance?.status ?? ''),
        },
      },
    );
  }

  /**
   * Submits a performance.
   */
  submitPerformance(call, callback) {
    this.executeSubmitPerformance(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSubmitPerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'SubmitPerformance', request);
    this.logger.info('submit_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        location: { lines: [...(request?.performance?.location?.lines ?? [])] },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.showsShowApprovalClient.submitShow(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.outcome?.status ?? '') },
        performance: {
          location: { country_code: (reply1?.show?.location?.country_code ?? '') },
        },
      },
      {
        performance: {
          performance_id: (request?.performance_id ?? ''),
          title: (request?.performance?.title ?? ''),
        },
      },
    );
  }
}

module.exports = { PerformanceApprovalService };
