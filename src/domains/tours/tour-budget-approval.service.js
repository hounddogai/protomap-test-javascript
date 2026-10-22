'use strict';

/**
 * Routes tour budgets through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect, exchange } = require('../../platform/streams');

/**
 * Serve TourBudgetApprovalService, recording every operation of the tours team.
 */
class TourBudgetApprovalService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'TourBudgetApprovalService' });
  }

  /**
   * Approves a tour budget.
   */
  approveTourBudget(call, callback) {
    this.executeApproveTourBudget(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeApproveTourBudget(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await this.store.save(this.store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await this.store.record(request.context.tenant_id, 'ApproveTourBudget', request);
    this.logger.info('approve_tour_budget', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue: {
        audit: { updated_by: (request?.tour_budget?.audit?.updated_by ?? '') },
      },
    };
    const repairVenue1 = promisify(this.clients.venuesVenueAdminClient.repairVenue.bind(this.clients.venuesVenueAdminClient));
    const reply1 = await repairVenue1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        region: ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.region ?? ''),
      },
    };
    const reply2 = await call((done) => this.clients.sponsorshipsSponsorshipDealApprovalClient.listPendingSponsorshipDeals(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    let selected3_2;
    switch (request?.tour_budget?.funding) {
      case 'bank':
        selected3_2 = ((request?.tour_budget?.funding === 'bank' ? request?.tour_budget.bank : undefined)?.bic ?? '');
        break;
      default:
        selected3_2 = '';
    }
    let selected3_3;
    switch (reply1?.venue?.locator) {
      case 'address':
        selected3_3 = ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.region ?? '');
        break;
      default:
        selected3_3 = '';
    }
    const request3 = {
      concession_order: {
        bank: { bic: selected3_2 },
        billing_address: { region: selected3_3 },
      },
      context: request?.context,
      details: {
        approver: { display_name: (request?.tour_budget?.holder?.display_name ?? '') },
      },
    };
    const reply3 = await call((done) => this.clients.concessionsConcessionOrderApprovalClient.rejectConcessionOrder(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      filter: {
        created_after: reply3?.concession_order?.audit?.created_at,
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        region: (reply3?.concession_order?.billing_address?.region ?? ''),
      },
    };
    const client4 = this.clients.registry.get('acme.tours.v1.TourStopQueryService');
    const reply4 = await call((done) => client4.suggestTourStops(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.outcome?.status ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          status: (reply2?.result?.status ?? ''),
        },
      },
      { result: { status: (reply3?.result?.status ?? '') } },
      {
        result: {
          reference_ids: (reply4?.result?.reference_ids ?? []),
          status: (reply4?.result?.status ?? ''),
        },
      },
      {
        tour_budget: {
          holder: request?.tour_budget?.holder,
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Comments a tour budget.
   */
  commentTourBudgetApproval(call, callback) {
    this.executeCommentTourBudgetApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCommentTourBudgetApproval(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await this.store.save(this.store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await this.store.record(request.context.tenant_id, 'CommentTourBudgetApproval', request);
    this.logger.info('comment_tour_budget_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          email: (request?.tour_budget?.contact?.email ?? ''),
          name: request?.tour_budget?.contact?.name,
          preferred_channel: (request?.tour_budget?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const send1 = this.clients.broadcastsReplayClipWorkflowClient.pauseReplayClipWorkflow.bind(this.clients.broadcastsReplayClipWorkflowClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      managed_device: {
        audit: {
          etag: (request?.tour_budget?.audit?.etag ?? ''),
          updated_by: (reply1?.replay_clip?.audit?.updated_by ?? ''),
        },
        owner: { user_id: (request?.details?.approver?.user_id ?? '') },
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => this.clients.devicesManagedDeviceCalculationClient.recalculateManagedDevices(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        tour_budget: {
          audit: { etag: (reply1?.replay_clip?.audit?.etag ?? '') },
          limits: [{ name: (reply1?.replay_clip?.name ?? '') }],
        },
      },
      {
        tour_budget: { audit: { etag: (reply2?.managed_device?.audit?.etag ?? '') } },
      },
      {
        tour_budget: {
          contact: request?.tour_budget?.contact,
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Delegates a tour budget.
   */
  delegateTourBudgetApproval(call, callback) {
    this.executeDelegateTourBudgetApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDelegateTourBudgetApproval(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await this.store.save(this.store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await this.store.record(request.context.tenant_id, 'DelegateTourBudgetApproval', request);
    this.logger.info('delegate_tour_budget_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      filter: {
        created_after: input1?.tour_budget?.audit?.created_at,
        labels: { ...(input1?.tour_budget?.labels ?? {}) },
      },
    };
    const reply1 = await call((done) => this.clients.toursTourStopQueryClient.findTourStops(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      tour: {
        audit: {
          created_by: (request?.tour_budget?.audit?.created_by ?? ''),
          updated_at: request?.tour_budget?.audit?.updated_at,
        },
        organizer: { user_id: (request?.details?.approver?.user_id ?? '') },
        participants: [{ contact: { email: (request?.details?.approver?.email ?? '') } }],
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.toursTourClient.createTour(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        tour_budget: {
          audit: {
            created_by: (reply2?.tour?.audit?.created_by ?? ''),
            updated_by: (reply2?.tour?.audit?.updated_by ?? ''),
          },
        },
      },
      {
        tour_budget: {
          holder: request?.tour_budget?.holder,
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Escalates a tour budget.
   */
  escalateTourBudget(call, callback) {
    this.executeEscalateTourBudget(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEscalateTourBudget(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await this.store.save(this.store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await this.store.record(request.context.tenant_id, 'EscalateTourBudget', request);
    this.logger.info('escalate_tour_budget', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.tour_budget?.audit?.created_at,
        labels: { ...(request?.tour_budget?.labels ?? {}) },
      },
    };
    const messages1 = await collect(this.clients.toursTourQueryClient.exportTours(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      details: {
        approver: { display_name: (request?.tour_budget?.holder?.display_name ?? '') },
      },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => this.clients.admissionsAdmissionApprovalClient.commentAdmissionApproval(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      etag: (request?.tour_budget?.audit?.etag ?? ''),
      reason: (request?.reason ?? ''),
      venue_id: (request?.tour_budget?.holder?.venue_id ?? ''),
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => this.clients.venuesVenueClient.deleteVenue(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      filter: {
        created_after: request?.tour_budget?.audit?.created_at,
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        owner_id: (reply2?.admission?.subject?.gate_scan_id ?? ''),
      },
    };
    const reply4 = await withRetry(() => call((done) => this.clients.auditAuditEventQueryClient.suggestAuditEvents(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
    return assemble(
      {
        outcome: { message: (reply2?.outcome?.message ?? '') },
        result: { status: (reply2?.result?.status ?? '') },
      },
      { result: { status: (reply3?.result?.status ?? '') } },
      {
        result: {
          correlation_id: (reply4?.result?.correlation_id ?? ''),
          message: (reply4?.result?.message ?? ''),
        },
      },
      {
        tour_budget: {
          status: (request?.tour_budget?.status ?? ''),
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a tour budget.
   */
  getTourBudgetApprovalStatus(call, callback) {
    this.executeGetTourBudgetApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetTourBudgetApprovalStatus(request) {
    validateContext(request.context);
    if (!request.tour_budget_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tour_budget_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetTourBudgetApprovalStatus', request);
    this.logger.info('get_tour_budget_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
    };
    const task1 = deferred(() => call((done) => this.clients.venuesVenueSectionNotificationClient.notifyVenueSectionWatchers(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.venue_section?.audit?.updated_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (reply1?.venue_section?.manager?.user_id ?? ''),
        region: (reply1?.venue_section?.location?.region ?? ''),
      },
    };
    const reply2 = await this.bus.request('acme.tours.v1.TourBudgetApprovalService/GetTourBudgetApprovalStatus:1', request2);
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      { tour_budget: { tour_budget_id: (request?.tour_budget_id ?? '') } },
    );
  }

  /**
   * Lists tour budgets a page at a time.
   */
  listPendingTourBudgets(call, callback) {
    this.executeListPendingTourBudgets(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListPendingTourBudgets(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListPendingTourBudgets', request);
    this.logger.info('list_pending_tour_budgets', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.filter?.owner_id ?? '') },
      page: { page_token: (request?.page?.page_token ?? '') },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.toursTourQueryClient.countTours(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        message: (reply1?.result?.message ?? ''),
      },
    };
  }

  /**
   * Recalls a tour budget.
   */
  recallTourBudget(call, callback) {
    this.executeRecallTourBudget(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecallTourBudget(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await this.store.save(this.store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await this.store.record(request.context.tenant_id, 'RecallTourBudget', request);
    this.logger.info('recall_tour_budget', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_3;
    switch (request?.tour_budget?.funding) {
      case 'external_account_id':
        selected1_3 = ((request?.tour_budget?.funding === 'external_account_id' ? request?.tour_budget.external_account_id : undefined) ?? '');
        break;
      default:
        selected1_3 = '';
    }
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.tour_budget?.audit?.created_at,
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        owner_id: selected1_3,
      },
    };
    const reply1 = await call((done) => this.clients.broadcastsReplayClipWorkflowClient.listReplayClipWorkflows(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.tour_budget?.audit?.updated_at,
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        owner_id: (reply1?.result?.correlation_id ?? ''),
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      this.clients.toursTourClient.listTours(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request3 = {
      context: request?.context,
      filter: { labels: { ...(request?.tour_budget?.labels ?? {}) } },
    };
    const suggestTourStops3 = promisify(this.clients.toursTourStopQueryClient.suggestTourStops.bind(this.clients.toursTourStopQueryClient));
    const reply3 = await suggestTourStops3(request3, metadataFor(request3.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      { outcome: { status: (reply2?.result?.status ?? '') } },
      { result: { status: (reply3?.result?.status ?? '') } },
      {
        tour_budget: {
          available: request?.tour_budget?.available,
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Rejects a tour budget.
   */
  rejectTourBudget(call, callback) {
    this.executeRejectTourBudget(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRejectTourBudget(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await this.store.save(this.store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await this.store.record(request.context.tenant_id, 'RejectTourBudget', request);
    this.logger.info('reject_tour_budget', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.tour_budget?.audit?.created_at,
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        owner_id: (request?.details?.approver?.user_id ?? ''),
      },
    };
    const reply1 = await call((done) => this.clients.toursTourStopQueryClient.suggestTourStops(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.tour_budget?.opened_at,
        labels: { ...(request?.tour_budget?.labels ?? {}) },
        owner_id: (request?.tour_budget?.tour_budget_id ?? ''),
      },
    };
    const method2 = 'listTours';
    const reply2 = await call((done) => this.clients.toursTourClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        outcome: {
          message: (reply2?.result?.message ?? ''),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        tour_budget: {
          holder: request?.tour_budget?.holder,
          tour_budget_id: (request?.tour_budget_id ?? ''),
        },
      },
    );
  }

  /**
   * Submits a tour budget.
   */
  submitTourBudget(call, callback) {
    this.executeSubmitTourBudget(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSubmitTourBudget(request) {
    validateContext(request.context);
    if (request.tour_budget) {
      await this.store.save(this.store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
    }
    await this.store.record(request.context.tenant_id, 'SubmitTourBudget', request);
    this.logger.info('submit_tour_budget', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { labels: { ...(request?.tour_budget?.labels ?? {}) } },
    };
    const client1 = this.clients.registry.get('acme.tours.v1.TourStopQueryService');
    const reply1 = await call((done) => client1.countTourStops(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: { labels: { ...(request?.tour_budget?.labels ?? {}) } },
    };
    const messages2 = await collect(this.clients.toursTourStopQueryClient.exportTourStops(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    const request3 = {
      change: {
        children: [{
          children: [{ parent_id: (request?.context?.actor?.user_id ?? '') }],
          parent_id: (request?.context?.actor?.user_id ?? ''),
        }],
        parent_id: (request?.context?.actor?.user_id ?? ''),
      },
      context: request?.context,
    };
    const messages3 = await exchange(this.clients.venuesVenueSectionSyncClient.streamVenueSectionChanges(metadataFor(request3.context), { deadline: deadline() }), [request3]);
    const reply3 = messages3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        outcome: { effective_at: request?.details?.effective_at },
        tour_budget: { tour_budget_id: (request?.tour_budget_id ?? '') },
      },
    );
  }
}

module.exports = { TourBudgetApprovalService };
