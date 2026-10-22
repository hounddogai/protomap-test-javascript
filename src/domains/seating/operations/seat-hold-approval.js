'use strict';

/**
 * Routes seat holds through approval, with escalation and delegation. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsWristbandApprovalClient,
  boxofficeCashDrawerWorkflowClient,
  catalogProductClient,
  corporateeventsCorporateEventCalculationClient,
  featureflagsFeatureFlagValidationClient,
  metricsMetricSeriesClient,
  performersPerformerWorkflowClient,
  seatingSeatAssignmentReportingClient,
  seatingSeatAssignmentValidationClient,
  seatingSeatHoldLifecycleClient,
  seatingSeatHoldReportingClient,
  seatingSeatMapClient,
  seatingSeatMapQueryClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Approves a seat hold.
 */
async function approveSeatHold(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'ApproveSeatHold', request);
  logger.info('approve_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { recipient: { email: (request?.details?.approver?.email ?? '') } },
  };
  const reply1 = await new Promise((resolve, reject) => {
    seatingSeatAssignmentReportingClient.shareSeatAssignmentReport(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const settled2 = await Promise.allSettled((request?.seat_hold?.participants ?? []).map(async (item) => {
    const request2 = {
      context: request?.context,
      details: {
        new_owner: { email: (item?.contact?.email ?? '') },
        notify_contact: {
          name: item?.contact?.name,
          phone: (item?.contact?.phone ?? ''),
          preferred_channel: (item?.contact?.preferred_channel ?? ''),
        },
      },
      seat_hold_id: (request?.seat_hold_id ?? ''),
    };
    const reply2 = await call((done) => seatingSeatHoldLifecycleClient.terminateSeatHold(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return reply2;
  }));
  const results2 = settled2.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply2 = results2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.outcome?.status ?? '') } },
    {
      seat_hold: {
        seat_hold_id: (request?.seat_hold_id ?? ''),
        title: (request?.seat_hold?.title ?? ''),
      },
    },
  );
}

/**
 * Comments a seat hold.
 */
async function commentSeatHoldApproval(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'CommentSeatHoldApproval', request);
  logger.info('comment_seat_hold_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      effective_at: request?.details?.effective_at,
      new_owner: {
        display_name: (request?.seat_hold?.organizer?.display_name ?? ''),
        user_id: (request?.seat_hold?.organizer?.user_id ?? ''),
      },
      notify_contact: { email: (request?.details?.approver?.email ?? '') },
    },
    seat_hold_id: (request?.seat_hold?.seat_hold_id ?? ''),
  };
  const reinstateSeatHold1 = promisify(seatingSeatHoldLifecycleClient.reinstateSeatHold.bind(seatingSeatHoldLifecycleClient));
  const reply1 = await reinstateSeatHold1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      labels: { ...(reply1?.result?.details ?? {}) },
      owner_id: (reply1?.seat_hold?.organizer?.user_id ?? ''),
      region: (reply1?.seat_hold?.location?.region ?? ''),
    },
  };
  const reply2 = await call((done) => seatingSeatAssignmentReportingClient.listSeatAssignmentReports(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    seat_hold_id: (request?.seat_hold?.seat_hold_id ?? ''),
  };
  const method3 = 'scheduleSeatHoldReport';
  const reply3 = await call((done) => seatingSeatHoldReportingClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { seat_hold: { location: reply1?.seat_hold?.location } },
    { outcome: { message: (reply2?.result?.message ?? '') } },
    {
      result: {
        correlation_id: (reply3?.result?.correlation_id ?? ''),
        status: (reply3?.result?.status ?? ''),
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
 * Delegates a seat hold.
 */
async function delegateSeatHoldApproval(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'DelegateSeatHoldApproval', request);
  logger.info('delegate_seat_hold_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { region: (request?.seat_hold?.location?.region ?? '') },
  };
  const client1 = clients.registry.get('acme.seating.v1.SeatHoldReportingService');
  const reply1 = await call((done) => client1.countSeatHoldTotals(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: { region: (request?.seat_hold?.location?.region ?? '') },
  };
  const send2 = performersPerformerWorkflowClient.listPerformerWorkflows.bind(performersPerformerWorkflowClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    corporate_event: {
      audit: { created_by: (request?.seat_hold?.audit?.created_by ?? '') },
      organizer: {
        display_name: (request?.seat_hold?.organizer?.display_name ?? ''),
        email: (request?.seat_hold?.organizer?.email ?? ''),
        user_id: (request?.seat_hold?.organizer?.user_id ?? ''),
      },
    },
  };
  const spread3 = {
    ...request3,
    context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
  };
  const reply3 = await call((done) => corporateeventsCorporateEventCalculationClient.explainCorporateEventCalculation(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { message: (reply1?.result?.message ?? '') } },
    {
      outcome: { status: (reply2?.result?.status ?? '') },
      result: { message: (reply2?.result?.message ?? '') },
    },
    {
      result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
      seat_hold: {
        location: { country_code: (reply3?.corporate_event?.location?.country_code ?? '') },
      },
    },
    {
      seat_hold: {
        organizer: request?.seat_hold?.organizer,
        seat_hold_id: (request?.seat_hold_id ?? ''),
      },
    },
  );
}

/**
 * Escalates a seat hold.
 */
async function escalateSeatHold(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'EscalateSeatHold', request);
  logger.info('escalate_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = [];
  for (const item of (request?.seat_hold?.participants ?? [])) {
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          email: (item?.contact?.email ?? ''),
          phone: (item?.contact?.phone ?? ''),
          preferred_channel: (item?.contact?.preferred_channel ?? ''),
        },
      },
      seat_assignment: { requester: { name: item?.contact?.name } },
    };
    const reply1 = await call((done) => seatingSeatAssignmentValidationClient.checkSeatAssignmentEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    results1.push(reply1);
  }
  const reply1 = results1.at(-1) ?? {};
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    cash_drawer: { audit: { updated_at: reply1?.seat_assignment?.audit?.updated_at } },
    context: input2?.context,
  };
  const reply2 = await call((done) => boxofficeCashDrawerWorkflowClient.signalCashDrawerWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      seat_hold: {
        participants: [{
          contact: {
            preferred_channel: (reply2?.cash_drawer?.contact?.preferred_channel ?? ''),
          },
        }],
      },
    },
    {
      seat_hold: {
        seat_hold_id: (request?.seat_hold_id ?? ''),
        status: (request?.seat_hold?.status ?? ''),
      },
    },
  );
}

/**
 * Returns a seat hold.
 */
async function getSeatHoldApprovalStatus(request) {
  validateContext(request.context);
  if (!request.seat_hold_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
  }
  await store.record(request.context.tenant_id, 'GetSeatHoldApprovalStatus', request);
  logger.info('get_seat_hold_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { created_after: request?.context?.deadline },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => metricsMetricSeriesClient.listMetricSerieses(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    product: {
      audit: { created_at: request?.context?.deadline },
      components: { primary: { component_id: (reply1?.result?.correlation_id ?? '') } },
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => catalogProductClient.createProduct(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    { seat_hold: { audit: { etag: (reply2?.product?.audit?.etag ?? '') } } },
    { seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') } },
  );
}

/**
 * Lists seat holds a page at a time.
 */
async function listPendingSeatHolds(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListPendingSeatHolds', request);
  logger.info('list_pending_seat_holds', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Recalls a seat hold.
 */
async function recallSeatHold(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'RecallSeatHold', request);
  logger.info('recall_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    wristband: {
      audit: {
        created_at: request?.seat_hold?.audit?.created_at,
        created_by: (request?.seat_hold?.audit?.created_by ?? ''),
        updated_by: (request?.seat_hold?.audit?.updated_by ?? ''),
      },
    },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => admissionsWristbandApprovalClient.approveWristband(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { notify: { email: (reply1?.wristband?.owner?.email ?? '') } },
    performer: {
      primary_address: {
        locality: ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.locality ?? ''),
      },
    },
  };
  const reply2 = await withRetry(() => call((done) => performersPerformerWorkflowClient.startPerformerWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    {
      seat_hold: {
        seat_hold_id: (request?.seat_hold_id ?? ''),
        window: request?.seat_hold?.window,
      },
    },
  );
}

/**
 * Rejects a seat hold.
 */
async function rejectSeatHold(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'RejectSeatHold', request);
  logger.info('reject_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    feature_flag: {
      audit: { updated_by: (request?.seat_hold?.audit?.updated_by ?? '') },
      owner: {
        display_name: (request?.seat_hold?.organizer?.display_name ?? ''),
        email: (request?.seat_hold?.organizer?.email ?? ''),
        user_id: (request?.seat_hold?.organizer?.user_id ?? ''),
      },
    },
  };
  const task1 = deferred(() => call((done) => featureflagsFeatureFlagValidationClient.normalizeFeatureFlag(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  const request2 = {
    context: request?.context,
    seat_map: {
      children: [{
        children: [{
          audit: { etag: (reply1?.feature_flag?.audit?.etag ?? '') },
          location: { lines: [...(request?.seat_hold?.location?.lines ?? [])] },
        }],
      }],
    },
  };
  const reply2 = await bus.request('acme.seating.v1.SeatHoldApprovalService/RejectSeatHold:1', request2);
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { message: (reply1?.outcome?.message ?? '') },
      seat_hold: { audit: { updated_at: reply1?.feature_flag?.audit?.updated_at } },
    },
    {
      seat_hold: {
        location: { recipient: (reply2?.seat_map?.location?.recipient ?? '') },
        organizer: { display_name: (reply2?.seat_map?.manager?.display_name ?? '') },
      },
    },
    {
      seat_hold: {
        seat_hold_id: (request?.seat_hold_id ?? ''),
        status: (request?.seat_hold?.status ?? ''),
      },
    },
  );
}

/**
 * Submits a seat hold.
 */
async function submitSeatHold(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'SubmitSeatHold', request);
  logger.info('submit_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { recipient: { email: (request?.details?.approver?.email ?? '') } },
  };
  const reply1 = await runInContext(request.context, () => call((done) => seatingSeatHoldReportingClient.shareSeatHoldReport(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.seat_hold?.audit?.created_at,
      region: (request?.seat_hold?.location?.region ?? ''),
    },
    since: request?.seat_hold?.audit?.updated_at,
    subscriber: { email: (request?.seat_hold?.organizer?.email ?? '') },
  };
  const messages2 = await collect(seatingSeatAssignmentReportingClient.streamSeatAssignmentMetrics(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  const request3 = {
    context: request?.context,
    venue: {
      audit: { updated_at: request?.seat_hold?.audit?.updated_at },
      owner: {
        display_name: (reply1?.seat_hold?.organizer?.display_name ?? ''),
        email: (request?.seat_hold?.organizer?.email ?? ''),
      },
    },
  };
  const reply3 = await call((done) => clients.venuesVenueHistoryClient.compareVenueVersions(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = { context: request?.context };
  const reply4 = await new Promise((resolve, reject) => {
    seatingSeatMapQueryClient.resolveSeatMaps(request4, metadataFor(request4.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      seat_hold: {
        audit: { created_at: reply1?.seat_hold?.audit?.created_at },
        location: { postal_code: (reply1?.seat_hold?.location?.postal_code ?? '') },
      },
    },
    {
      outcome: { next_steps: (reply3?.outcome?.next_steps ?? []) },
      seat_hold: { audit: { updated_by: (reply3?.venue?.audit?.updated_by ?? '') } },
    },
    { result: { status: (reply4?.result?.status ?? '') } },
    {
      seat_hold: {
        seat_hold_id: (request?.seat_hold_id ?? ''),
        status: (request?.seat_hold?.status ?? ''),
      },
    },
  );
}

module.exports = { approveSeatHold, commentSeatHoldApproval, delegateSeatHoldApproval, escalateSeatHold, getSeatHoldApprovalStatus, listPendingSeatHolds, recallSeatHold, rejectSeatHold, submitSeatHold };
