'use strict';

/**
 * Runs the long-lived workflow that takes a seat assignment from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of SeatAssignmentWorkflowService, over the dependencies the seating team is given.
 */
function createSeatAssignmentWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a seat assignment.
   */
  async function advanceSeatAssignmentWorkflow(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await store.record(request.context.tenant_id, 'AdvanceSeatAssignmentWorkflow', request);
    logger.info('advance_seat_assignment_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.seat_assignment?.sla?.due_at,
        labels: { ...(request?.seat_assignment?.custom_fields ?? {}) },
        owner_id: ((request?.seat_assignment?.origin === 'customer' ? request?.seat_assignment.customer : undefined)?.seat_map_id ?? ''),
      },
    };
    const messages1 = await collect(clients.venuesVenuePolicyExportClient.streamVenuePolicyExport(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = { context: input2?.context };
    const reply2 = await call((done) => clients.seatingSeatMapClient.batchGetSeatMaps(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { details: (reply2?.result?.details ?? {}) },
      },
      {
        seat_assignment: {
          custom_fields: (request?.seat_assignment?.custom_fields ?? {}),
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }

  /**
   * Cancels a seat assignment.
   */
  async function cancelSeatAssignmentWorkflow(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await store.record(request.context.tenant_id, 'CancelSeatAssignmentWorkflow', request);
    logger.info('cancel_seat_assignment_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.seat_assignment?.comments ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        seat_hold: {
          audit: { created_at: item?.created_at },
          participants: [{ contact: { email: (item?.author_email ?? '') } }],
        },
      };
      const reply1 = await call((done) => clients.seatingSeatHoldWorkflowClient.startSeatHoldWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      wristband: {
        owner: {
          display_name: (reply1?.seat_hold?.organizer?.display_name ?? ''),
          user_id: (reply1?.seat_hold?.organizer?.user_id ?? ''),
        },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.admissionsWristbandWorkflowClient.pauseWristbandWorkflow(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { effective_at: reply2?.outcome?.effective_at } },
      {
        seat_assignment: {
          priority: (request?.seat_assignment?.priority ?? 0),
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }

  /**
   * Lists seat assignments a page at a time.
   */
  async function listSeatAssignmentWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListSeatAssignmentWorkflows', request);
    logger.info('list_seat_assignment_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Pauses a seat assignment.
   */
  async function pauseSeatAssignmentWorkflow(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await store.record(request.context.tenant_id, 'PauseSeatAssignmentWorkflow', request);
    logger.info('pause_seat_assignment_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_hold: {
        participants: [{ contact: { email: (request?.seat_assignment?.assignee?.email ?? '') } }],
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.seatingSeatHoldValidationClient.lintSeatHolds(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_assignment: { assignee: { email: (reply1?.seat_hold?.organizer?.email ?? '') } },
      },
      {
        seat_assignment: {
          custom_fields: (request?.seat_assignment?.custom_fields ?? {}),
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }

  /**
   * Retrys a seat assignment.
   */
  async function retrySeatAssignmentWorkflow(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await store.record(request.context.tenant_id, 'RetrySeatAssignmentWorkflow', request);
    logger.info('retry_seat_assignment_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.seat_assignment?.comments ?? [])) {
      const request1 = {
        context: request?.context,
        venue_policy: {
          audit: {
            created_at: item?.created_at,
            etag: (request?.seat_assignment?.audit?.etag ?? ''),
          },
          owner: { email: (item?.author_email ?? '') },
        },
      };
      const reply1 = await call((done) => clients.venuesVenuePolicyExportClient.retryVenuePolicyExport(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            children: [{
              seat_map_id: ((request?.seat_assignment?.origin === 'customer' ? request?.seat_assignment.customer : undefined)?.seat_map_id ?? ''),
            }],
          }],
        }],
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.seatingSeatMapApprovalClient.rejectSeatMap(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    const results3 = await Promise.all((request?.seat_assignment?.history ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        filter: {
          created_after: item?.changed_at,
          owner_id: (request?.seat_assignment?.assignee?.user_id ?? ''),
          region: (reply2?.seat_map?.location?.region ?? ''),
        },
      };
      const reply3 = await call((done) => clients.admissionsWristbandWorkflowClient.listWristbandWorkflows(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_assignment: { audit: { updated_by: (reply2?.seat_map?.audit?.updated_by ?? '') } },
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
   * Signals a seat assignment.
   */
  async function signalSeatAssignmentWorkflow(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await store.record(request.context.tenant_id, 'SignalSeatAssignmentWorkflow', request);
    logger.info('signal_seat_assignment_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.seat_assignment?.comments ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        details: { approver: { email: (item?.author_email ?? '') } },
        seat_hold: { audit: { created_at: item?.created_at } },
      };
      const reply1 = await call((done) => clients.seatingSeatHoldApprovalClient.commentSeatHoldApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      seat_hold: {
        participants: [{
          contact: {
            preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const reply2 = await withRetry(() => call((done) => clients.seatingSeatHoldApprovalClient.recallSeatHold(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const results3 = [];
    for (const item of (request?.seat_assignment?.comments ?? [])) {
      const request3 = {
        context: request?.context,
        filter: {
          created_after: item?.created_at,
          region: (reply2?.seat_hold?.location?.region ?? ''),
        },
      };
      const reply3 = await call((done) => clients.seatingSeatMapQueryClient.findSeatMaps(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      results3.push(reply3);
    }
    const reply3 = results3.at(-1) ?? {};
    const request4 = {
      context: request?.context,
      seat_hold: {
        participants: [{
          contact: { phone: (request?.seat_assignment?.requester?.phone ?? '') },
        }],
      },
    };
    const task4 = deferred(() => call((done) => clients.seatingSeatHoldCalculationClient.quoteSeatHold(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    const reply4 = await task4();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply2?.outcome?.message ?? '') },
        seat_assignment: {
          assignee: { display_name: (reply2?.seat_hold?.organizer?.display_name ?? '') },
        },
      },
      { result: { status: (reply4?.result?.status ?? '') } },
      {
        seat_assignment: {
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
          subject: (request?.seat_assignment?.subject ?? ''),
        },
      },
    );
  }

  /**
   * Starts a seat assignment.
   */
  async function startSeatAssignmentWorkflow(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await store.record(request.context.tenant_id, 'StartSeatAssignmentWorkflow', request);
    logger.info('start_seat_assignment_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.seat_assignment?.comments ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        details: {
          new_owner: { user_id: (request?.seat_assignment?.assignee?.user_id ?? '') },
          notify_contact: {
            email: (item?.author_email ?? ''),
            phone: (request?.seat_assignment?.requester?.phone ?? ''),
            preferred_channel: (request?.seat_assignment?.requester?.preferred_channel ?? ''),
          },
        },
        seat_assignment_id: (request?.seat_assignment_id ?? ''),
      };
      const reply1 = await call((done) => clients.seatingSeatAssignmentLifecycleClient.expireSeatAssignment(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      seat_hold: {
        participants: [{
          contact: {
            email: (reply1?.seat_assignment?.requester?.email ?? ''),
            name: request?.seat_assignment?.requester?.name,
          },
        }],
      },
    };
    const reply2 = await bus.request('acme.seating.v1.SeatAssignmentWorkflowService/StartSeatAssignmentWorkflow:1', request2);
    const request3 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: {
              created_by: (reply1?.seat_assignment?.audit?.created_by ?? ''),
              updated_at: reply2?.seat_hold?.audit?.updated_at,
            },
          }],
        }],
      },
    };
    const reply3 = await runInContext(request.context, () => call((done) => clients.seatingSeatMapApprovalClient.rejectSeatMap(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const request4 = {
      context: request?.context,
      seat_hold: {
        participants: [{ contact: { email: (reply1?.seat_assignment?.assignee?.email ?? '') } }],
      },
    };
    const reply4 = await call((done) => clients.seatingSeatHoldWorkflowClient.startSeatHoldWorkflow(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply2?.outcome?.status ?? '') } },
      { result: { reference_ids: (reply3?.result?.reference_ids ?? []) } },
      {
        seat_assignment: { assignee: { email: (reply4?.seat_hold?.organizer?.email ?? '') } },
      },
      {
        seat_assignment: {
          requester: request?.seat_assignment?.requester,
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }

  return {
    advanceSeatAssignmentWorkflow: unary(advanceSeatAssignmentWorkflow),
    cancelSeatAssignmentWorkflow: unary(cancelSeatAssignmentWorkflow),
    listSeatAssignmentWorkflows: unary(listSeatAssignmentWorkflows),
    pauseSeatAssignmentWorkflow: unary(pauseSeatAssignmentWorkflow),
    retrySeatAssignmentWorkflow: unary(retrySeatAssignmentWorkflow),
    signalSeatAssignmentWorkflow: unary(signalSeatAssignmentWorkflow),
    startSeatAssignmentWorkflow: unary(startSeatAssignmentWorkflow),
  };
}

module.exports = { createSeatAssignmentWorkflowHandlers };
