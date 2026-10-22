'use strict';

/**
 * Moves a lineup between the states of its lifecycle.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve LineupLifecycleService, recording every operation of the shows team.
 */
class LineupLifecycleService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'LineupLifecycleService' });
  }

  /**
   * Activates a lineup.
   */
  activateLineup(call, callback) {
    this.executeActivateLineup(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeActivateLineup(request) {
    validateContext(request.context);
    if (!request.lineup_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'lineup_id is required');
    }
    await this.store.record(request.context.tenant_id, 'ActivateLineup', request);
    this.logger.info('activate_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      performance: {
        participants: [{ contact: { name: input1?.details?.notify_contact?.name } }],
      },
    };
    const reply1 = await call((done) => this.clients.showsPerformanceCalculationClient.recalculatePerformances(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        notify: {
          preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
        },
      },
      performer_contract: {
        audit: {
          created_by: (reply1?.performance?.audit?.created_by ?? ''),
          updated_at: reply1?.performance?.audit?.updated_at,
        },
        counterparty: { display_name: (request?.details?.new_owner?.display_name ?? '') },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.performersPerformerContractWorkflowClient.pausePerformerContractWorkflow(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      guest_list: {
        billing_address: {
          country_code: (reply1?.performance?.location?.country_code ?? ''),
          lines: [...(reply1?.performance?.location?.lines ?? [])],
        },
      },
    };
    const copied3 = structuredClone(request3);
    const reply3 = await call((done) => this.clients.boxofficeGuestListApprovalClient.escalateGuestList(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        lineup: {
          billing_address: { recipient: (reply1?.performance?.location?.recipient ?? '') },
        },
        result: { status: (reply1?.result?.status ?? '') },
      },
      {
        lineup: {
          counterparty: reply2?.performer_contract?.counterparty,
          due_at: reply2?.performer_contract?.due_at,
        },
      },
      {
        lineup: { metadata: (reply3?.guest_list?.metadata ?? {}) },
        result: { status: (reply3?.result?.status ?? '') },
      },
      {
        lineup: { lineup_id: (request?.lineup_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Expires a lineup.
   */
  expireLineup(call, callback) {
    this.executeExpireLineup(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExpireLineup(request) {
    validateContext(request.context);
    if (!request.lineup_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'lineup_id is required');
    }
    await this.store.record(request.context.tenant_id, 'ExpireLineup', request);
    this.logger.info('expire_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(request?.annotations ?? {}) },
        owner_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.showsShowClient.listShows(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      lineup_id: (request?.lineup_id ?? ''),
    };
    const reply2 = await withRetry(() => call((done) => this.clients.showsLineupCalculationClient.getLineupRates(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { status: (reply1?.result?.status ?? '') },
      },
      {
        lineup: { audit: { created_at: reply2?.lineup?.audit?.created_at } },
        outcome: { message: (reply2?.result?.message ?? '') },
      },
      {
        lineup: { lineup_id: (request?.lineup_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Returns a lineup.
   */
  getLineupLifecycle(call, callback) {
    this.executeGetLineupLifecycle(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetLineupLifecycle(request) {
    validateContext(request.context);
    if (!request.lineup_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'lineup_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetLineupLifecycle', request);
    this.logger.info('get_lineup_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { lineup: { lineup_id: (request?.lineup_id ?? '') } };
  }

  /**
   * Reinstates a lineup.
   */
  reinstateLineup(call, callback) {
    this.executeReinstateLineup(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeReinstateLineup(request) {
    validateContext(request.context);
    if (!request.lineup_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'lineup_id is required');
    }
    await this.store.record(request.context.tenant_id, 'ReinstateLineup', request);
    this.logger.info('reinstate_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        recipient: {
          email: (request?.details?.new_owner?.email ?? ''),
          phone: (request?.details?.notify_contact?.phone ?? ''),
          preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
        },
      },
    };
    const task1 = deferred(() => call((done) => this.clients.seatingSeatHoldReportingClient.shareSeatHoldReport(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      subscriber: { phone: (request?.details?.notify_contact?.phone ?? '') },
    };
    const messages2 = await collect(this.clients.metricsMetricSeriesClient.watchMetricSerieses(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        lineup: {
          billing_address: { locality: (reply1?.seat_hold?.location?.locality ?? '') },
        },
        result: { message: (reply1?.outcome?.message ?? '') },
      },
      {
        lineup: { lineup_id: (request?.lineup_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Schedules a lineup.
   */
  scheduleLineupExpiry(call, callback) {
    this.executeScheduleLineupExpiry(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScheduleLineupExpiry(request) {
    validateContext(request.context);
    if (request.lineup) {
      await this.store.save(this.store.toLineupRecord(request.context.tenant_id, request.lineup));
    }
    await this.store.record(request.context.tenant_id, 'ScheduleLineupExpiry', request);
    this.logger.info('schedule_lineup_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.lineup?.audit?.updated_at,
        labels: { ...(request?.lineup?.metadata ?? {}) },
        region: (request?.lineup?.billing_address?.region ?? ''),
      },
    };
    const reply1 = await this.bus.request('acme.shows.v1.LineupLifecycleService/ScheduleLineupExpiry:0', request1);
    const request2 = {
      context: request?.context,
      filter: { region: (request?.lineup?.billing_address?.region ?? '') },
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.showsPerformanceQueryClient.browsePerformances(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { details: (reply2?.result?.details ?? {}) },
      },
      {
        lineup: {
          issued_at: request?.lineup?.issued_at,
          lineup_id: (request?.lineup_id ?? ''),
        },
      },
    );
  }

  /**
   * Suspends a lineup.
   */
  suspendLineup(call, callback) {
    this.executeSuspendLineup(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuspendLineup(request) {
    validateContext(request.context);
    if (!request.lineup_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'lineup_id is required');
    }
    await this.store.record(request.context.tenant_id, 'SuspendLineup', request);
    this.logger.info('suspend_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        owner_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    };
    const reply1 = await call((done) => this.clients.auditAuditEventQueryClient.browseAuditEvents(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        lineup: { lineup_id: (request?.lineup_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Terminates a lineup.
   */
  terminateLineup(call, callback) {
    this.executeTerminateLineup(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeTerminateLineup(request) {
    validateContext(request.context);
    if (!request.lineup_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'lineup_id is required');
    }
    await this.store.record(request.context.tenant_id, 'TerminateLineup', request);
    this.logger.info('terminate_lineup', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          email: (request?.details?.notify_contact?.email ?? ''),
          name: request?.details?.notify_contact?.name,
          preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.performersPerformerContractWorkflowClient.advancePerformerContractWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      details: { approver: { email: (request?.details?.notify_contact?.email ?? '') } },
      rider: {
        audit: { updated_by: (reply1?.performer_contract?.audit?.updated_by ?? '') },
        owner: {
          display_name: (reply1?.performer_contract?.counterparty?.display_name ?? ''),
          email: (request?.details?.new_owner?.email ?? ''),
        },
      },
    };
    const commentRiderApproval2 = promisify(this.clients.performersRiderApprovalClient.commentRiderApproval.bind(this.clients.performersRiderApprovalClient));
    const reply2 = await commentRiderApproval2(request2, metadataFor(request2.context), { deadline: deadline() });
    const request3 = { context: request?.context };
    const reply3 = await call((done) => this.clients.showsPerformanceQueryClient.resolvePerformances(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        lineup: {
          billing_address: {
            country_code: (reply1?.performer_contract?.billing_address?.country_code ?? ''),
          },
        },
      },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.outcome?.status ?? ''),
        },
      },
      {
        result: {
          correlation_id: (reply3?.result?.correlation_id ?? ''),
          message: (reply3?.result?.message ?? ''),
        },
      },
      {
        lineup: { lineup_id: (request?.lineup_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }
}

module.exports = { LineupLifecycleService };
