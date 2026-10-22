'use strict';

/**
 * Routes seat assignments through approval, with escalation and delegation. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsWristbandApprovalClient,
  documentsDocumentQueryClient,
  identityUserQueryClient,
  performersPerformerWorkflowClient,
  performersRiderWorkflowClient,
  seatingSeatAssignmentCalculationClient,
  seatingSeatAssignmentReportingClient,
  seatingSeatHoldApprovalClient,
  seatingSeatHoldReportingClient,
  venuesVenuePolicyNotificationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Approves a seat assignment.
 */
async function approveSeatAssignment(request) {
  validateContext(request.context);
  if (request.seat_assignment) {
    await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
  }
  await store.record(request.context.tenant_id, 'ApproveSeatAssignment', request);
  logger.info('approve_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = [];
  for (const item of (request?.seat_assignment?.history ?? [])) {
    const request1 = {
      context: request?.context,
      filter: {
        created_after: item?.changed_at,
        labels: { ...(request?.seat_assignment?.custom_fields ?? {}) },
        owner_id: ((request?.seat_assignment?.origin === 'customer' ? request?.seat_assignment.customer : undefined)?.seat_map_id ?? ''),
      },
    };
    const reply1 = await call((done) => documentsDocumentQueryClient.suggestDocuments(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    results1.push(reply1);
  }
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.seat_assignment?.audit?.updated_at,
      labels: { ...(request?.seat_assignment?.custom_fields ?? {}) },
      owner_id: (request?.details?.approver?.user_id ?? ''),
    },
  };
  const reply2 = await bus.request('acme.seating.v1.SeatAssignmentApprovalService/ApproveSeatAssignment:1', request2);
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { details: (reply2?.result?.details ?? {}) } },
    {
      outcome: { effective_at: request?.details?.effective_at },
      seat_assignment: { seat_assignment_id: (request?.seat_assignment_id ?? '') },
    },
  );
}

/**
 * Escalates a seat assignment.
 */
async function escalateSeatAssignment(request) {
  validateContext(request.context);
  if (request.seat_assignment) {
    await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
  }
  await store.record(request.context.tenant_id, 'EscalateSeatAssignment', request);
  logger.info('escalate_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = await Promise.all((request?.seat_assignment?.history ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      seat_assignment: {
        history: [{
          changed_at: item?.changed_at,
          from_status: (item?.from_status ?? ''),
          to_status: (item?.to_status ?? ''),
        }],
      },
    };
    const reply1 = await call((done) => seatingSeatAssignmentCalculationClient.estimateSeatAssignment(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return reply1;
  }));
  const reply1 = results1.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    seat_assignment: {
      priority: (request?.seat_assignment?.priority ?? 0),
      seat_assignment_id: (request?.seat_assignment_id ?? ''),
    },
  };
}

/**
 * Returns a seat assignment.
 */
async function getSeatAssignmentApprovalStatus(request) {
  validateContext(request.context);
  if (!request.seat_assignment_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_assignment_id is required');
  }
  await store.record(request.context.tenant_id, 'GetSeatAssignmentApprovalStatus', request);
  logger.info('get_seat_assignment_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return {
    seat_assignment: { seat_assignment_id: (request?.seat_assignment_id ?? '') },
  };
}

/**
 * Lists seat assignments a page at a time.
 */
async function listPendingSeatAssignments(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListPendingSeatAssignments', request);
  logger.info('list_pending_seat_assignments', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    rider: {
      audit: { updated_at: request?.filter?.created_after },
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => performersRiderWorkflowClient.signalRiderWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = { context: request?.context };
  const reply2 = await call((done) => clients.ratelimitingRateLimitQueryClient.resolveRateLimits(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return assemble(
    {
      seat_assignments: [{ audit: { updated_at: reply1?.rider?.audit?.updated_at } }],
    },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
  );
}

/**
 * Recalls a seat assignment.
 */
async function recallSeatAssignment(request) {
  validateContext(request.context);
  if (request.seat_assignment) {
    await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
  }
  await store.record(request.context.tenant_id, 'RecallSeatAssignment', request);
  logger.info('recall_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    reason: (request?.reason ?? ''),
  };
  const reply1 = await new Promise((resolve, reject) => {
    seatingSeatHoldReportingClient.generateSeatHoldReport(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    context: request?.context,
    subscriber: { phone: (request?.seat_assignment?.requester?.phone ?? '') },
  };
  const messages2 = await collect(seatingSeatAssignmentReportingClient.streamSeatAssignmentMetrics(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    {
      seat_assignment: {
        assignee: request?.seat_assignment?.assignee,
        seat_assignment_id: (request?.seat_assignment_id ?? ''),
      },
    },
  );
}

/**
 * Rejects a seat assignment.
 */
async function rejectSeatAssignment(request) {
  validateContext(request.context);
  if (request.seat_assignment) {
    await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
  }
  await store.record(request.context.tenant_id, 'RejectSeatAssignment', request);
  logger.info('reject_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const settled1 = await Promise.allSettled((request?.seat_assignment?.comments ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      details: {
        approver: { user_id: (request?.details?.approver?.user_id ?? '') },
        comment: (item?.comment_id ?? ''),
      },
      wristband: {
        audit: { created_at: item?.created_at },
        owner: { email: (item?.author_email ?? '') },
      },
    };
    const reply1 = await call((done) => admissionsWristbandApprovalClient.recallWristband(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return reply1;
  }));
  const results1 = settled1.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    performer: {
      audit: { updated_at: request?.seat_assignment?.audit?.updated_at },
      contact: { name: request?.seat_assignment?.requester?.name },
    },
  };
  const cancelPerformerWorkflow2 = promisify(performersPerformerWorkflowClient.cancelPerformerWorkflow.bind(performersPerformerWorkflowClient));
  const reply2 = await cancelPerformerWorkflow2(request2, metadataFor(request2.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      seat_assignment: {
        requester: {
          email: (reply2?.performer?.relationship_manager?.email ?? ''),
          phone: (reply2?.performer?.contact?.phone ?? ''),
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
 * Submits a seat assignment.
 */
async function submitSeatAssignment(request) {
  validateContext(request.context);
  if (request.seat_assignment) {
    await store.save(store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
  }
  await store.record(request.context.tenant_id, 'SubmitSeatAssignment', request);
  logger.info('submit_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = await Promise.all((request?.seat_assignment?.comments ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      subscriber: {
        email: (item?.author_email ?? ''),
        preferred_channel: (request?.seat_assignment?.requester?.preferred_channel ?? ''),
      },
    };
    const messages1 = await collect(venuesVenuePolicyNotificationClient.streamVenuePolicyNotices(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    return reply1;
  }));
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    seat_hold: {
      participants: [{ contact: { email: (request?.seat_assignment?.assignee?.email ?? '') } }],
    },
  };
  const reply2 = await call((done) => seatingSeatHoldApprovalClient.recallSeatHold(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { status: (reply2?.outcome?.status ?? '') },
      seat_assignment: { audit: { created_at: reply2?.seat_hold?.audit?.created_at } },
    },
    {
      seat_assignment: {
        seat_assignment_id: (request?.seat_assignment_id ?? ''),
        status: (request?.seat_assignment?.status ?? ''),
      },
    },
  );
}

module.exports = { approveSeatAssignment, escalateSeatAssignment, getSeatAssignmentApprovalStatus, listPendingSeatAssignments, recallSeatAssignment, rejectSeatAssignment, submitSeatAssignment };
