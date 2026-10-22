'use strict';

/**
 * Moves a seat hold between the states of its lifecycle.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { exchange } = require('../../../platform/streams');

/**
 * The handlers of SeatHoldLifecycleService, over the dependencies the seating team is given.
 */
function createSeatHoldLifecycleHandlers({ store, clients, bus, logger }) {
  /**
   * Activates a seat hold.
   */
  async function activateSeatHold(request) {
    validateContext(request.context);
    if (!request.seat_hold_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
    }
    await store.record(request.context.tenant_id, 'ActivateSeatHold', request);
    logger.info('activate_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(request?.annotations ?? {}) },
        owner_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.notificationsNotificationQueryClient.countNotifications(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') },
      },
    );
  }

  /**
   * Expires a seat hold.
   */
  async function expireSeatHold(request) {
    validateContext(request.context);
    if (!request.seat_hold_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
    }
    await store.record(request.context.tenant_id, 'ExpireSeatHold', request);
    logger.info('expire_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      seat_hold_id: (request?.seat_hold_id ?? ''),
    };
    const generateSeatHoldReport1 = promisify(clients.seatingSeatHoldReportingClient.generateSeatHoldReport.bind(clients.seatingSeatHoldReportingClient));
    const reply1 = await generateSeatHoldReport1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      details: { notify: { name: request?.details?.notify_contact?.name } },
      stream_key: { contact: { phone: (request?.details?.notify_contact?.phone ?? '') } },
    };
    const reply2 = await call((done) => clients.broadcastsStreamKeyWorkflowClient.advanceStreamKeyWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      change: {
        block: {
          notify_email: (request?.context?.actor?.email ?? ''),
          reason: (request?.reason ?? ''),
        },
        scopes: [...(request?.context?.actor?.roles ?? [])],
      },
      context: request?.context,
    };
    const messages3 = await exchange(clients.venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request3.context), { deadline: deadline() }), [request3]);
    const reply3 = messages3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.details?.notify_contact?.preferred_channel ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        seat_hold: {
          organizer: { email: (reply2?.stream_key?.contact?.email ?? '') },
          participants: [{ contact: { name: reply2?.stream_key?.contact?.name } }],
        },
      },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') },
      },
    );
  }

  /**
   * Returns a seat hold.
   */
  async function getSeatHoldLifecycle(request) {
    validateContext(request.context);
    if (!request.seat_hold_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
    }
    await store.record(request.context.tenant_id, 'GetSeatHoldLifecycle', request);
    logger.info('get_seat_hold_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.seat_hold_id ?? ''),
      },
    };
    const method1 = 'countSeatMaps';
    const reply1 = await call((done) => clients.seatingSeatMapQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (reply1?.result?.correlation_id ?? ''),
      },
    };
    const client2 = clients.registry.get('acme.venues.v1.VenueSectionExportService');
    const reply2 = await call((done) => client2.listVenueSectionExports(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      { seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') } },
    );
  }

  /**
   * Reinstates a seat hold.
   */
  async function reinstateSeatHold(request) {
    validateContext(request.context);
    if (!request.seat_hold_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
    }
    await store.record(request.context.tenant_id, 'ReinstateSeatHold', request);
    logger.info('reinstate_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
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
    const send1 = clients.seatingSeatAssignmentReportingClient.shareSeatAssignmentReport.bind(clients.seatingSeatAssignmentReportingClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.seat_assignment?.sla?.due_at,
        labels: { ...(reply1?.seat_assignment?.custom_fields ?? {}) },
        owner_id: (reply1?.seat_assignment?.seat_assignment_id ?? ''),
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.seatingSeatMapQueryClient.countSeatMaps(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      context: input3?.context,
      details: {
        notify: {
          preferred_channel: (reply1?.seat_assignment?.requester?.preferred_channel ?? ''),
        },
      },
      stream_key: {
        contact: {
          name: reply1?.seat_assignment?.requester?.name,
          preferred_channel: (input3?.details?.notify_contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply3 = await call((done) => clients.broadcastsStreamKeyWorkflowClient.pauseStreamKeyWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    let selected4_1;
    switch (reply1?.seat_assignment?.origin) {
      case 'customer':
        selected4_1 = ((reply1?.seat_assignment?.origin === 'customer' ? reply1?.seat_assignment.customer : undefined)?.display_name ?? '');
        break;
      default:
        selected4_1 = '';
    }
    const request4 = {
      access_policy: { owner: { display_name: selected4_1 } },
      context: request?.context,
    };
    const reply4 = await call((done) => clients.authorizationAccessPolicyClient.cloneAccessPolicy(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        outcome: { status: (reply2?.result?.status ?? '') },
        result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
      },
      {
        seat_hold: {
          participants: [{
            contact: {
              preferred_channel: (reply3?.stream_key?.contact?.preferred_channel ?? ''),
            },
          }],
        },
      },
      {
        seat_hold: {
          audit: { created_at: reply4?.access_policy?.audit?.created_at },
          participants: [{ contact: { email: (reply4?.access_policy?.owner?.email ?? '') } }],
        },
      },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') },
      },
    );
  }

  /**
   * Renews a seat hold.
   */
  async function renewSeatHold(request) {
    validateContext(request.context);
    if (!request.seat_hold_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
    }
    await store.record(request.context.tenant_id, 'RenewSeatHold', request);
    logger.info('renew_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            manager: {
              email: (request?.details?.notify_contact?.email ?? ''),
              user_id: (request?.details?.new_owner?.user_id ?? ''),
            },
          }],
          manager: { email: (request?.details?.new_owner?.email ?? '') },
        }],
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.seatingSeatMapCalculationClient.recalculateSeatMaps(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        destination: { country_code: (reply1?.seat_map?.location?.country_code ?? '') },
      },
      incident: {
        customer: { display_name: (reply1?.seat_map?.manager?.display_name ?? '') },
        requester: { email: (request?.details?.notify_contact?.email ?? '') },
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.itsmIncidentCalculationClient.calculateIncident(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: { region: (reply1?.seat_map?.location?.region ?? '') },
    };
    const reply3 = await withRetry(() => call((done) => clients.boxofficeGuestListReportingClient.listGuestListReports(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const request4 = {
      context: request?.context,
      seat_assignment: {
        audit: { etag: (reply1?.seat_map?.audit?.etag ?? '') },
        requester: {
          name: request?.details?.notify_contact?.name,
          preferred_channel: (reply2?.incident?.requester?.preferred_channel ?? ''),
        },
      },
    };
    const task4 = deferred(() => call((done) => clients.seatingSeatAssignmentValidationClient.scoreSeatAssignment(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    const reply4 = await task4();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          status: (reply2?.result?.status ?? ''),
        },
      },
      { result: { status: (reply3?.result?.status ?? '') } },
      {
        outcome: { message: (reply4?.result?.message ?? '') },
        seat_hold: {
          organizer: {
            display_name: ((reply4?.seat_assignment?.origin === 'customer' ? reply4?.seat_assignment.customer : undefined)?.display_name ?? ''),
          },
        },
      },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') },
      },
    );
  }

  /**
   * Schedules a seat hold.
   */
  async function scheduleSeatHoldExpiry(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await store.record(request.context.tenant_id, 'ScheduleSeatHoldExpiry', request);
    logger.info('schedule_seat_hold_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: {
        filters: { ...(request?.details?.parameters ?? {}) },
        period: request?.seat_hold?.window,
      },
      reason: (request?.reason ?? ''),
    };
    const reply1 = await bus.request('acme.seating.v1.SeatHoldLifecycleService/ScheduleSeatHoldExpiry:0', request1);
    const request2 = {
      context: request?.context,
      seat_hold_id: (request?.seat_hold?.seat_hold_id ?? ''),
    };
    const reply2 = await runInContext(request.context, () => call((done) => clients.seatingSeatHoldCalculationClient.getSeatHoldRates(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        result: { status: (reply2?.result?.status ?? '') },
        seat_hold: {
          location: { recipient: (reply2?.seat_hold?.location?.recipient ?? '') },
        },
      },
      {
        seat_hold: {
          metadata: (request?.seat_hold?.metadata ?? {}),
          seat_hold_id: (request?.seat_hold_id ?? ''),
        },
      },
    );
  }

  /**
   * Suspends a seat hold.
   */
  async function suspendSeatHold(request) {
    validateContext(request.context);
    if (!request.seat_hold_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
    }
    await store.record(request.context.tenant_id, 'SuspendSeatHold', request);
    logger.info('suspend_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { notify: { name: request?.details?.notify_contact?.name } },
      gate_scan: {
        subject: { display_name: (request?.details?.new_owner?.display_name ?? '') },
      },
    };
    const reply1 = await call((done) => clients.admissionsGateScanWorkflowClient.startGateScanWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply1?.result?.status ?? '') } },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') },
      },
    );
  }

  /**
   * Terminates a seat hold.
   */
  async function terminateSeatHold(request) {
    validateContext(request.context);
    if (!request.seat_hold_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
    }
    await store.record(request.context.tenant_id, 'TerminateSeatHold', request);
    logger.info('terminate_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          email: (request?.details?.new_owner?.email ?? ''),
          name: request?.details?.notify_contact?.name,
          phone: (request?.details?.notify_contact?.phone ?? ''),
        },
      },
      performer_contract: {
        counterparty: { display_name: (request?.details?.new_owner?.display_name ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.performersPerformerContractWorkflowClient.resumePerformerContractWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    let selected2_2;
    switch (reply1?.performer_contract?.settlement) {
      case 'card':
        selected2_2 = ((reply1?.performer_contract?.settlement === 'card' ? reply1?.performer_contract.card : undefined)?.last4 ?? '');
        break;
      default:
        selected2_2 = '';
    }
    const request2 = {
      context: request?.context,
      details: { notify: { name: request?.details?.notify_contact?.name } },
      stream_key: {
        card: { last4: selected2_2 },
        holder: {
          display_name: (reply1?.performer_contract?.counterparty?.display_name ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.broadcastsStreamKeyWorkflowClient.pauseStreamKeyWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') } },
      { outcome: { message: (reply2?.outcome?.message ?? '') } },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') },
      },
    );
  }

  return {
    activateSeatHold: unary(activateSeatHold),
    expireSeatHold: unary(expireSeatHold),
    getSeatHoldLifecycle: unary(getSeatHoldLifecycle),
    reinstateSeatHold: unary(reinstateSeatHold),
    renewSeatHold: unary(renewSeatHold),
    scheduleSeatHoldExpiry: unary(scheduleSeatHoldExpiry),
    suspendSeatHold: unary(suspendSeatHold),
    terminateSeatHold: unary(terminateSeatHold),
  };
}

module.exports = { createSeatHoldLifecycleHandlers };
