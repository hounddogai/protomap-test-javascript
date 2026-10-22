'use strict';

/**
 * Moves a seat assignment between the states of its lifecycle.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of SeatAssignmentLifecycleService, over the dependencies the seating team is given.
 */
function createSeatAssignmentLifecycleHandlers({ store, clients, bus, logger }) {
  /**
   * Activates a seat assignment.
   */
  async function activateSeatAssignment(request) {
    validateContext(request.context);
    if (!request.seat_assignment_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_assignment_id is required');
    }
    await store.record(request.context.tenant_id, 'ActivateSeatAssignment', request);
    logger.info('activate_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const method1 = 'batchGetSuppliers';
    const reply1 = await call((done) => clients.suppliersSupplierClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { email: (request?.details?.new_owner?.email ?? '') } },
      seat_assignment: {
        assignee: { display_name: (request?.details?.new_owner?.display_name ?? '') },
        requester: {
          preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
        },
      },
    };
    const client2 = clients.registry.get('acme.seating.v1.SeatAssignmentValidationService');
    const reply2 = await call((done) => client2.explainSeatAssignmentDecision(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      activation: {
        requester: {
          preferred_channel: (reply2?.seat_assignment?.requester?.preferred_channel ?? ''),
        },
      },
      context: request?.context,
      details: { approver: { email: (request?.details?.notify_contact?.email ?? '') } },
    };
    const send3 = clients.sponsorshipsActivationApprovalClient.commentActivationApproval.bind(clients.sponsorshipsActivationApprovalClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      yard: {
        owner: {
          display_name: (request?.details?.new_owner?.display_name ?? ''),
          email: (reply2?.seat_assignment?.requester?.email ?? ''),
        },
      },
    };
    const spread4 = {
      ...request4,
      context: request4.context ? { ...request4.context, baggage: { ...request4.context.baggage } } : null,
    };
    const reply4 = await call((done) => clients.yardsYardClient.cloneYard(spread4, metadataFor(spread4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        seat_assignment: { sla: { policy_id: (reply2?.seat_assignment?.sla?.policy_id ?? '') } },
      },
      { seat_assignment: { sla: { due_at: reply3?.activation?.sla?.due_at } } },
      {
        seat_assignment: { assignee: { display_name: (reply4?.yard?.owner?.display_name ?? '') } },
      },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_assignment: { seat_assignment_id: (request?.seat_assignment_id ?? '') },
      },
    );
  }

  /**
   * Expires a seat assignment.
   */
  async function expireSeatAssignment(request) {
    validateContext(request.context);
    if (!request.seat_assignment_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_assignment_id is required');
    }
    await store.record(request.context.tenant_id, 'ExpireSeatAssignment', request);
    logger.info('expire_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      performer: {
        contact: { name: input1?.details?.notify_contact?.name },
        relationship_manager: { display_name: (input1?.details?.new_owner?.display_name ?? '') },
      },
    };
    const reply1 = await call((done) => clients.performersPerformerApprovalClient.recallPerformer(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      seat_assignment: { requester: { phone: (reply1?.performer?.contact?.phone ?? '') } },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.seatingSeatAssignmentCalculationClient.recalculateSeatAssignments(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_assignment: { requester: { name: reply1?.performer?.contact?.name } },
      },
      {
        seat_assignment: { priority: (reply2?.seat_assignment?.priority ?? 0) },
      },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_assignment: { seat_assignment_id: (request?.seat_assignment_id ?? '') },
      },
    );
  }

  /**
   * Reinstates a seat assignment.
   */
  async function reinstateSeatAssignment(request) {
    validateContext(request.context);
    if (!request.seat_assignment_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_assignment_id is required');
    }
    await store.record(request.context.tenant_id, 'ReinstateSeatAssignment', request);
    logger.info('reinstate_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_assignment_id: (request?.seat_assignment_id ?? ''),
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.seatingSeatAssignmentReportingClient.fetchSeatAssignmentReport(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      subscriber: {
        email: (reply1?.seat_assignment?.requester?.email ?? ''),
        name: reply1?.seat_assignment?.requester?.name,
        preferred_channel: (reply1?.seat_assignment?.requester?.preferred_channel ?? ''),
      },
    };
    const messages2 = await collect(clients.suppliersSupplierClient.watchSuppliers(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { seat_assignment: { status: (reply1?.seat_assignment?.status ?? '') } },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_assignment: { seat_assignment_id: (request?.seat_assignment_id ?? '') },
      },
    );
  }

  /**
   * Renews a seat assignment.
   */
  async function renewSeatAssignment(request) {
    validateContext(request.context);
    if (!request.seat_assignment_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_assignment_id is required');
    }
    await store.record(request.context.tenant_id, 'RenewSeatAssignment', request);
    logger.info('renew_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{ manager: { user_id: (request?.details?.new_owner?.user_id ?? '') } }],
          manager: { email: (request?.details?.new_owner?.email ?? '') },
        }],
        manager: { email: (request?.details?.notify_contact?.email ?? '') },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.seatingSeatMapCalculationClient.explainSeatMapCalculation(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { next_steps: (reply1?.outcome?.next_steps ?? []) } },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_assignment: { seat_assignment_id: (request?.seat_assignment_id ?? '') },
      },
    );
  }

  /**
   * Schedules a seat assignment.
   */
  async function scheduleSeatAssignmentExpiry(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await store.record(request.context.tenant_id, 'ScheduleSeatAssignmentExpiry', request);
    logger.info('schedule_seat_assignment_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_hold: {
        participants: [{
          contact: {
            preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.seatingSeatHoldCalculationClient.compareSeatHoldOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_assignment: {
          audit: {
            updated_at: reply1?.seat_hold?.audit?.updated_at,
            updated_by: (reply1?.seat_hold?.audit?.updated_by ?? ''),
          },
        },
      },
      {
        seat_assignment: {
          assignee: request?.seat_assignment?.assignee,
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }

  /**
   * Suspends a seat assignment.
   */
  async function suspendSeatAssignment(request) {
    validateContext(request.context);
    if (!request.seat_assignment_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_assignment_id is required');
    }
    await store.record(request.context.tenant_id, 'SuspendSeatAssignment', request);
    logger.info('suspend_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: {
        recipient: {
          email: (request?.details?.notify_contact?.email ?? ''),
          phone: (request?.details?.notify_contact?.phone ?? ''),
          preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
        },
      },
    };
    const task1 = deferred(() => call((done) => clients.venuesVenueSectionExportClient.shareVenueSectionExport(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      venue: {
        address: { lines: [...(reply1?.venue_section?.location?.lines ?? [])] },
        audit: { created_at: reply1?.venue_section?.audit?.created_at },
        owner: {
          email: (request?.details?.notify_contact?.email ?? ''),
          user_id: (request?.details?.new_owner?.user_id ?? ''),
        },
      },
    };
    const reply2 = await bus.request('acme.seating.v1.SeatAssignmentLifecycleService/SuspendSeatAssignment:1', request2);
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.venue_section?.audit?.updated_at,
        region: ((reply2?.venue?.locator === 'address' ? reply2?.venue.address : undefined)?.region ?? ''),
      },
    };
    const reply3 = await runInContext(request.context, () => call((done) => clients.notificationsNotificationQueryClient.findNotifications(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.outcome?.message ?? '') },
        seat_assignment: { audit: { updated_at: reply1?.venue_section?.audit?.updated_at } },
      },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      { outcome: { message: (reply3?.result?.message ?? '') } },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_assignment: { seat_assignment_id: (request?.seat_assignment_id ?? '') },
      },
    );
  }

  /**
   * Terminates a seat assignment.
   */
  async function terminateSeatAssignment(request) {
    validateContext(request.context);
    if (!request.seat_assignment_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_assignment_id is required');
    }
    await store.record(request.context.tenant_id, 'TerminateSeatAssignment', request);
    logger.info('terminate_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(request?.annotations ?? {}) },
        owner_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    };
    const reply1 = await call((done) => clients.seatingSeatHoldReportingClient.countSeatHoldTotals(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
        },
      },
      seat_map: {
        children: [{
          manager: { display_name: (request?.details?.new_owner?.display_name ?? '') },
        }],
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      clients.seatingSeatMapValidationClient.lintSeatMaps(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        seat_assignment: { requester: { email: (reply2?.seat_map?.manager?.email ?? '') } },
      },
      {
        outcome: { effective_at: request?.details?.effective_at },
        seat_assignment: { seat_assignment_id: (request?.seat_assignment_id ?? '') },
      },
    );
  }

  return {
    activateSeatAssignment: unary(activateSeatAssignment),
    expireSeatAssignment: unary(expireSeatAssignment),
    reinstateSeatAssignment: unary(reinstateSeatAssignment),
    renewSeatAssignment: unary(renewSeatAssignment),
    scheduleSeatAssignmentExpiry: unary(scheduleSeatAssignmentExpiry),
    suspendSeatAssignment: unary(suspendSeatAssignment),
    terminateSeatAssignment: unary(terminateSeatAssignment),
  };
}

module.exports = { createSeatAssignmentLifecycleHandlers };
