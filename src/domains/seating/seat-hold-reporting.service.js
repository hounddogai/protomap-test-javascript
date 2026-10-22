'use strict';

/**
 * Builds and schedules reports about seat holds.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve SeatHoldReportingService, recording every operation of the seating team.
 */
class SeatHoldReportingService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'SeatHoldReportingService' });
  }

  /**
   * Counts seat holds by status.
   */
  countSeatHoldTotals(call, callback) {
    this.executeCountSeatHoldTotals(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountSeatHoldTotals(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountSeatHoldTotals', request);
    this.logger.info('count_seat_hold_totals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Deletes a seat hold.
   */
  deleteSeatHoldReport(call, callback) {
    this.executeDeleteSeatHoldReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDeleteSeatHoldReport(request) {
    validateContext(request.context);
    if (!request.seat_hold_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
    }
    await this.store.record(request.context.tenant_id, 'DeleteSeatHoldReport', request);
    this.logger.info('delete_seat_hold_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.seat_hold_id ?? ''),
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.seatingSeatHoldQueryClient.findSeatHolds(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (request?.seat_hold_id ?? ''),
      },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.ledgerLedgerAccountQueryClient.browseLedgerAccounts(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      seat_map: {
        audit: { etag: (request?.etag ?? '') },
        children: [{
          children: [{
            manager: {
              email: (request?.context?.actor?.email ?? ''),
              user_id: (request?.context?.actor?.user_id ?? ''),
            },
          }],
        }],
      },
    };
    const task3 = deferred(() => call((done) => this.clients.seatingSeatMapValidationClient.explainSeatMapDecision(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      { result: { details: (reply3?.result?.details ?? {}) } },
    );
  }

  /**
   * Returns a seat hold.
   */
  fetchSeatHoldReport(call, callback) {
    this.executeFetchSeatHoldReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFetchSeatHoldReport(request) {
    validateContext(request.context);
    if (!request.seat_hold_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
    }
    await this.store.record(request.context.tenant_id, 'FetchSeatHoldReport', request);
    this.logger.info('fetch_seat_hold_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        approver: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const reply1 = await this.bus.request('acme.seating.v1.SeatHoldReportingService/FetchSeatHoldReport:0', request1);
    return assemble(
      {
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
        seat_hold: {
          organizer: { display_name: (reply1?.admission?.subject?.display_name ?? '') },
        },
      },
      { seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') } },
    );
  }

  /**
   * Generates a seat hold.
   */
  generateSeatHoldReport(call, callback) {
    this.executeGenerateSeatHoldReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGenerateSeatHoldReport(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'GenerateSeatHoldReport', request);
    this.logger.info('generate_seat_hold_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      reason: (request?.reason ?? ''),
      seat_assignment: {
        assignee: { user_id: (request?.context?.actor?.user_id ?? '') },
        requester: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.seatingSeatAssignmentCalculationClient.estimateSeatAssignment(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      venue_section: {
        children: [{
          audit: { updated_at: reply1?.seat_assignment?.audit?.updated_at },
          children: [{
            audit: {
              created_at: reply1?.seat_assignment?.audit?.created_at,
              etag: (reply1?.seat_assignment?.audit?.etag ?? ''),
              updated_by: (reply1?.seat_assignment?.audit?.updated_by ?? ''),
            },
          }],
        }],
      },
    };
    const reply2 = await call((done) => this.clients.venuesVenueSectionSyncClient.pushVenueSection(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      venue_policy: {
        audit: {
          created_at: reply1?.seat_assignment?.audit?.created_at,
          updated_by: (reply1?.seat_assignment?.audit?.updated_by ?? ''),
        },
        owner: { email: (reply2?.venue_section?.manager?.email ?? '') },
      },
    };
    const reply3 = await new Promise((resolve, reject) => {
      this.clients.venuesVenuePolicyHistoryClient.compareVenuePolicyVersions(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    this.logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      {
        result: {
          reference_ids: (reply1?.result?.reference_ids ?? []),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        result: {
          details: (reply3?.result?.details ?? {}),
          message: (reply3?.result?.message ?? ''),
        },
      },
      { period: request?.details?.period },
    );
  }

  /**
   * Lists seat holds a page at a time.
   */
  listSeatHoldReports(call, callback) {
    this.executeListSeatHoldReports(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListSeatHoldReports(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListSeatHoldReports', request);
    this.logger.info('list_seat_hold_reports', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: {
        audit: { updated_at: request?.filter?.created_after },
        contact: { email: (request?.context?.actor?.email ?? '') },
        labels: { ...(request?.filter?.labels ?? {}) },
      },
    };
    const startStreamKeyWorkflow1 = promisify(this.clients.broadcastsStreamKeyWorkflowClient.startStreamKeyWorkflow.bind(this.clients.broadcastsStreamKeyWorkflowClient));
    const reply1 = await startStreamKeyWorkflow1(request1, metadataFor(request1.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      seat_holds: [{ participants: [{ contact: reply1?.stream_key?.contact }] }],
    };
  }

  /**
   * Schedules a seat hold.
   */
  scheduleSeatHoldReport(call, callback) {
    this.executeScheduleSeatHoldReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScheduleSeatHoldReport(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ScheduleSeatHoldReport', request);
    this.logger.info('schedule_seat_hold_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      ledger_account: { contact: { email: (request?.context?.actor?.email ?? '') } },
      reason: (request?.reason ?? ''),
    };
    const reply1 = await call((done) => this.clients.ledgerLedgerAccountValidationClient.explainLedgerAccountDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { phone: (reply1?.ledger_account?.contact?.phone ?? '') } },
      seat_assignment: {
        audit: { created_by: (reply1?.ledger_account?.audit?.created_by ?? '') },
        comments: [{ created_at: reply1?.ledger_account?.audit?.created_at }],
      },
    };
    const method2 = 'previewSeatAssignment';
    const reply2 = await call((done) => this.clients.seatingSeatAssignmentValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.ledger_account?.audit?.created_at,
        labels: { ...(reply1?.ledger_account?.labels ?? {}) },
      },
    };
    const client3 = this.clients.registry.get('acme.venues.v1.VenueSectionQueryService');
    const reply3 = await call((done) => client3.findVenueSections(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      { result: { details: (reply2?.result?.details ?? {}) } },
      {
        result: {
          correlation_id: (reply3?.result?.correlation_id ?? ''),
          reference_ids: (reply3?.result?.reference_ids ?? []),
        },
      },
      { period: request?.details?.period },
    );
  }

  /**
   * Shares a seat hold.
   */
  shareSeatHoldReport(call, callback) {
    this.executeShareSeatHoldReport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeShareSeatHoldReport(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ShareSeatHoldReport', request);
    this.logger.info('share_seat_hold_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{ manager: { user_id: (request?.context?.actor?.user_id ?? '') } }],
          manager: { email: (request?.details?.recipient?.email ?? '') },
        }],
      },
    };
    const send1 = this.clients.venuesVenueSectionValidationClient.scoreVenueSection.bind(this.clients.venuesVenueSectionValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { email: (request?.details?.recipient?.email ?? '') } },
      ledger_account: {
        audit: { updated_by: (reply1?.venue_section?.audit?.updated_by ?? '') },
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => this.clients.ledgerLedgerAccountValidationClient.checkLedgerAccountEligibility(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_hold: {
          location: { locality: (reply1?.venue_section?.location?.locality ?? '') },
          organizer: { email: (reply1?.venue_section?.manager?.email ?? '') },
        },
      },
      {
        outcome: { status: (reply2?.result?.status ?? '') },
        seat_hold: {
          participants: [{
            contact: {
              preferred_channel: (reply2?.ledger_account?.contact?.preferred_channel ?? ''),
            },
          }],
        },
      },
      { seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') } },
    );
  }

  /**
   * Streams the changes of seat holds as they happen.
   */
  streamSeatHoldMetrics(call) {
    serverStream((request) => this.executeStreamSeatHoldMetrics(request))(call);
  }

  async executeStreamSeatHoldMetrics(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'StreamSeatHoldMetrics', request);
    this.logger.info('stream_seat_hold_metrics', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      annotations: { ...(input1?.context?.baggage ?? {}) },
      context: input1?.context,
      details: { filters: { ...(input1?.filter?.labels ?? {}) } },
    };
    const reply1 = await call((done) => this.clients.seatingSeatHoldQueryClient.summarizeSeatHolds(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        seat_hold: {
          location: { region: (request?.filter?.region ?? '') },
          organizer: { email: (request?.subscriber?.email ?? '') },
        },
      },
    );
  }
}

module.exports = { SeatHoldReportingService };
