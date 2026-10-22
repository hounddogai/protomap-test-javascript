'use strict';

/**
 * Routes seat maps through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect, upload } = require('../../platform/streams');

/**
 * Serve SeatMapApprovalService, recording every operation of the seating team.
 */
class SeatMapApprovalService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'SeatMapApprovalService' });
  }

  /**
   * Approves a seat map.
   */
  approveSeatMap(call, callback) {
    this.executeApproveSeatMap(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeApproveSeatMap(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'ApproveSeatMap', request);
    this.logger.info('approve_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.seat_map?.audit?.created_at,
        labels: { ...(request?.seat_map?.attributes ?? {}) },
        owner_id: (request?.details?.approver?.user_id ?? ''),
        region: (request?.seat_map?.location?.region ?? ''),
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.corporateeventsCorporateEventQueryClient.findCorporateEvents(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { recipient: { email: (request?.seat_map?.manager?.email ?? '') } },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => this.clients.seatingSeatAssignmentReportingClient.shareSeatAssignmentReport(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const request3 = {
      broadcast: {
        participants: [{
          contact: {
            email: (request?.seat_map?.manager?.email ?? ''),
            name: reply2?.seat_assignment?.requester?.name,
          },
        }],
      },
      context: request?.context,
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => this.clients.broadcastsBroadcastWorkflowClient.signalBroadcastWorkflow(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      details: {
        notify: {
          email: (request?.details?.approver?.email ?? ''),
          name: reply2?.seat_assignment?.requester?.name,
        },
      },
      gate_scan: {
        subject: { display_name: (reply3?.broadcast?.organizer?.display_name ?? '') },
      },
    };
    const reply4 = await withRetry(() => call((done) => this.clients.admissionsGateScanWorkflowClient.advanceGateScanWorkflow(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        seat_map: {
          children: [{
            children: [{
              audit: { updated_by: (reply2?.seat_assignment?.audit?.updated_by ?? '') },
            }],
          }],
        },
      },
      {
        seat_map: {
          children: [{
            children: [{ manager: { email: (reply3?.broadcast?.organizer?.email ?? '') } }],
            location: { lines: (reply3?.broadcast?.location?.lines ?? []) },
          }],
        },
      },
      {
        result: {
          correlation_id: (reply4?.gate_scan?.correlation_id ?? ''),
          status: (reply4?.outcome?.status ?? ''),
        },
      },
      {
        seat_map: {
          manager: request?.seat_map?.manager,
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Comments a seat map.
   */
  commentSeatMapApproval(call, callback) {
    this.executeCommentSeatMapApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCommentSeatMapApproval(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'CommentSeatMapApproval', request);
    this.logger.info('comment_seat_map_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: { created_by: (request?.seat_map?.audit?.created_by ?? '') },
            manager: { display_name: (request?.seat_map?.manager?.display_name ?? '') },
          }],
        }],
      },
    };
    const task1 = deferred(() => call((done) => this.clients.seatingSeatMapLifecycleClient.scheduleSeatMapExpiry(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      header: {
        source: (request?.context?.actor?.user_id ?? ''),
        submitted_by: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const reply2 = await upload(
      (done) => this.clients.ticketingTicketClient.batchCreateTickets(metadataFor(request2.context), { deadline: deadline() }, done),
      [request2],
    );
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.outcome?.status ?? '') },
        seat_map: { children: [{ children: [{ name: (reply1?.seat_map?.name ?? '') }] }] },
      },
      {
        seat_map: {
          manager: request?.seat_map?.manager,
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Delegates a seat map.
   */
  delegateSeatMapApproval(call, callback) {
    this.executeDelegateSeatMapApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDelegateSeatMapApproval(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'DelegateSeatMapApproval', request);
    this.logger.info('delegate_seat_map_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { notify: { email: (request?.seat_map?.manager?.email ?? '') } },
      gate_scan: {
        attributes: { ...(request?.seat_map?.attributes ?? {}) },
        subject: { display_name: (request?.seat_map?.manager?.display_name ?? '') },
      },
    };
    const reply1 = await this.bus.request('acme.seating.v1.SeatMapApprovalService/DelegateSeatMapApproval:0', request1);
    const request2 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: { etag: (request?.seat_map?.audit?.etag ?? '') },
            location: { locality: (request?.seat_map?.location?.locality ?? '') },
            manager: { email: (request?.details?.approver?.email ?? '') },
          }],
        }],
      },
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.seatingSeatMapValidationClient.validateSeatMap(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      details: { contact: { email: (request?.seat_map?.manager?.email ?? '') } },
      expense_report: {
        audit: { created_by: (request?.seat_map?.audit?.created_by ?? '') },
        billing_address: { postal_code: (request?.seat_map?.location?.postal_code ?? '') },
        counterparty: { email: (reply2?.seat_map?.manager?.email ?? '') },
      },
    };
    const reply3 = await call((done) => this.clients.expensesExpenseReportCalculationClient.explainExpenseReportCalculation(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply1?.outcome?.status ?? '') } },
      {
        seat_map: { children: [{ status: (reply2?.seat_map?.status ?? '') }] },
      },
      { outcome: { status: (reply3?.outcome?.status ?? '') } },
      {
        seat_map: {
          name: (request?.seat_map?.name ?? ''),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a seat map.
   */
  getSeatMapApprovalStatus(call, callback) {
    this.executeGetSeatMapApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetSeatMapApprovalStatus(request) {
    validateContext(request.context);
    if (!request.seat_map_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetSeatMapApprovalStatus', request);
    this.logger.info('get_seat_map_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.context?.baggage ?? {}) },
      context: request?.context,
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.seatingSeatHoldReportingClient.generateSeatHoldReport(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      header: {
        source: (request?.context?.actor?.user_id ?? ''),
        submitted_by: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const reply2 = await upload(
      (done) => this.clients.seatingSeatMapClient.batchCreateSeatMaps(metadataFor(request2.context), { deadline: deadline() }, done),
      [request2],
    );
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { seat_map: { seat_map_id: (request?.seat_map_id ?? '') } },
    );
  }

  /**
   * Lists seat maps a page at a time.
   */
  listPendingSeatMaps(call, callback) {
    this.executeListPendingSeatMaps(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListPendingSeatMaps(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListPendingSeatMaps', request);
    this.logger.info('list_pending_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Recalls a seat map.
   */
  recallSeatMap(call, callback) {
    this.executeRecallSeatMap(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecallSeatMap(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'RecallSeatMap', request);
    this.logger.info('recall_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue: {
        audit: { created_by: (request?.seat_map?.audit?.created_by ?? '') },
        owner: { email: (request?.seat_map?.manager?.email ?? '') },
      },
    };
    const retryVenueExport1 = promisify(this.clients.venuesVenueExportClient.retryVenueExport.bind(this.clients.venuesVenueExportClient));
    const reply1 = await retryVenueExport1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      etag: (request?.seat_map?.audit?.etag ?? ''),
      seat_map_id: (request?.seat_map?.seat_map_id ?? ''),
    };
    const reply2 = await call((done) => this.clients.seatingSeatMapClient.deleteSeatMap(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_map: {
          children: [{ manager: { email: (reply1?.venue?.owner?.email ?? '') } }],
        },
      },
      { outcome: { effective_at: reply2?.effective_at } },
      {
        seat_map: {
          depth: (request?.seat_map?.depth ?? 0),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Rejects a seat map.
   */
  rejectSeatMap(call, callback) {
    this.executeRejectSeatMap(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRejectSeatMap(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'RejectSeatMap', request);
    this.logger.info('reject_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            location: {
              lines: [...(request?.seat_map?.location?.lines ?? [])],
              locality: (request?.seat_map?.location?.locality ?? ''),
            },
          }],
        }],
      },
    };
    const method1 = 'scoreSeatMap';
    const reply1 = await call((done) => this.clients.seatingSeatMapValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      seat_hold: {
        audit: {
          created_by: (request?.seat_map?.audit?.created_by ?? ''),
          updated_by: (reply1?.seat_map?.audit?.updated_by ?? ''),
        },
        location: { country_code: (request?.seat_map?.location?.country_code ?? '') },
        participants: [{ contact: { email: (reply1?.seat_map?.manager?.email ?? '') } }],
      },
    };
    const client2 = this.clients.registry.get('acme.seating.v1.SeatHoldLifecycleService');
    const reply2 = await call((done) => client2.scheduleSeatHoldExpiry(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      seat_assignment: {
        audit: {
          etag: (reply2?.seat_hold?.audit?.etag ?? ''),
          updated_by: (reply2?.seat_hold?.audit?.updated_by ?? ''),
        },
      },
    };
    const send3 = this.clients.seatingSeatAssignmentValidationClient.previewSeatAssignment.bind(this.clients.seatingSeatAssignmentValidationClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_map: {
          manager: { display_name: (reply1?.seat_map?.manager?.display_name ?? '') },
        },
      },
      { outcome: { status: (reply2?.outcome?.status ?? '') } },
      { result: { details: (reply3?.result?.details ?? {}) } },
      {
        seat_map: {
          seat_map_id: (request?.seat_map_id ?? ''),
          status: (request?.seat_map?.status ?? ''),
        },
      },
    );
  }

  /**
   * Submits a seat map.
   */
  submitSeatMap(call, callback) {
    this.executeSubmitSeatMap(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSubmitSeatMap(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'SubmitSeatMap', request);
    this.logger.info('submit_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        owner_id: (request?.details?.approver?.user_id ?? ''),
        region: (request?.seat_map?.location?.region ?? ''),
      },
      since: request?.details?.effective_at,
      subscriber: { email: (request?.seat_map?.manager?.email ?? '') },
    };
    const messages1 = await collect(this.clients.seatingSeatHoldReportingClient.streamSeatHoldMetrics(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      details: {
        new_owner: { user_id: (request?.seat_map?.manager?.user_id ?? '') },
        notify_contact: { email: (request?.seat_map?.manager?.email ?? '') },
      },
      seat_map_id: (request?.seat_map?.seat_map_id ?? ''),
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => this.clients.seatingSeatMapLifecycleClient.activateSeatMap(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      context: input3?.context,
      filter: { region: (input3?.seat_map?.location?.region ?? '') },
    };
    const reply3 = await call((done) => this.clients.metricsMetricSeriesQueryClient.countMetricSerieses(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_map: {
          children: [{
            children: [{
              location: { country_code: (reply2?.seat_map?.location?.country_code ?? '') },
            }],
          }],
        },
      },
      { result: { message: (reply3?.result?.message ?? '') } },
      {
        seat_map: {
          depth: (request?.seat_map?.depth ?? 0),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }
}

module.exports = { SeatMapApprovalService };
