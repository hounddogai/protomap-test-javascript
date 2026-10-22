'use strict';

/**
 * Builds and schedules reports about seat assignments. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  boxofficeGuestListApprovalClient,
  concessionsConcessionOrderApprovalClient,
  concessionsMenuItemLifecycleClient,
  concessionsMenuItemReportingClient,
  featurestoreFeatureViewClient,
  seatingSeatAssignmentCalculationClient,
  seatingSeatAssignmentValidationClient,
  seatingSeatHoldValidationClient,
  seatingSeatMapCalculationClient,
  seatingSeatMapQueryClient,
  sponsorshipsSponsorshipDealWorkflowClient,
  timekeepingTimesheetQueryClient,
  venuesVenuePolicySyncClient,
  venuesVenueSectionExportClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Counts seat assignments by status.
 */
async function countSeatAssignmentTotals(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'CountSeatAssignmentTotals', request);
  logger.info('count_seat_assignment_totals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    concession_order: { billing_address: { region: (request?.filter?.region ?? '') } },
    context: request?.context,
    details: {
      approver: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const submitConcessionOrder1 = promisify(concessionsConcessionOrderApprovalClient.submitConcessionOrder.bind(concessionsConcessionOrderApprovalClient));
  const reply1 = await submitConcessionOrder1(request1, metadataFor(request1.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    result: {
      reference_ids: (reply1?.result?.reference_ids ?? []),
      status: (reply1?.outcome?.status ?? ''),
    },
  };
}

/**
 * Deletes a seat assignment.
 */
async function deleteSeatAssignmentReport(request) {
  validateContext(request.context);
  if (!request.seat_assignment_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_assignment_id is required');
  }
  await store.record(request.context.tenant_id, 'DeleteSeatAssignmentReport', request);
  logger.info('delete_seat_assignment_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsorship_deal: { audit: { etag: (request?.etag ?? '') } },
  };
  const reply1 = await call((done) => sponsorshipsSponsorshipDealWorkflowClient.retrySponsorshipDealWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return { result: { status: (reply1?.outcome?.status ?? '') } };
}

/**
 * Returns a seat assignment.
 */
async function fetchSeatAssignmentReport(request) {
  validateContext(request.context);
  if (!request.seat_assignment_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_assignment_id is required');
  }
  await store.record(request.context.tenant_id, 'FetchSeatAssignmentReport', request);
  logger.info('fetch_seat_assignment_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    feature_view: {
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const method1 = 'updateFeatureView';
  const reply1 = await call((done) => featurestoreFeatureViewClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { filters: { ...(reply1?.result?.details ?? {}) } },
  };
  const client2 = clients.registry.get('acme.seating.v1.SeatMapQueryService');
  const reply2 = await call((done) => client2.summarizeSeatMaps(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  return assemble(
    {
      result: {
        message: (reply1?.result?.message ?? ''),
        status: (reply1?.result?.status ?? ''),
      },
    },
    { result: { details: (reply2?.result?.details ?? {}) } },
    {
      seat_assignment: { seat_assignment_id: (request?.seat_assignment_id ?? '') },
    },
  );
}

/**
 * Generates a seat assignment.
 */
async function generateSeatAssignmentReport(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'GenerateSeatAssignmentReport', request);
  logger.info('generate_seat_assignment_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    subscriber: { email: (request?.context?.actor?.email ?? '') },
  };
  const messages1 = await collect(concessionsMenuItemReportingClient.streamMenuItemMetrics(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: {
      approver: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
    reason: (request?.reason ?? ''),
  };
  const send2 = boxofficeGuestListApprovalClient.delegateGuestListApproval.bind(boxofficeGuestListApprovalClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
    reason: (request?.reason ?? ''),
  };
  const spread3 = {
    ...request3,
    context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
  };
  const reply3 = await call((done) => venuesVenueSectionExportClient.shareVenueSectionExport(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.cron ?? '') });
  return assemble(
    { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
    { result: { message: (reply3?.result?.message ?? '') } },
    { period: request?.details?.period },
  );
}

/**
 * Lists seat assignments a page at a time.
 */
async function listSeatAssignmentReports(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListSeatAssignmentReports', request);
  logger.info('list_seat_assignment_reports', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Schedules a seat assignment.
 */
async function scheduleSeatAssignmentReport(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ScheduleSeatAssignmentReport', request);
  logger.info('schedule_seat_assignment_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    annotations: { ...(input1?.annotations ?? {}) },
    context: input1?.context,
    reason: (input1?.reason ?? ''),
    sponsorship_deal: {
      counterparty: { email: (input1?.context?.actor?.email ?? '') },
      metadata: { ...(input1?.details?.filters ?? {}) },
    },
  };
  const reply1 = await call((done) => sponsorshipsSponsorshipDealWorkflowClient.pauseSponsorshipDealWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.cron ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    { period: request?.details?.period },
  );
}

/**
 * Shares a seat assignment.
 */
async function shareSeatAssignmentReport(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ShareSeatAssignmentReport', request);
  logger.info('share_seat_assignment_report', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    seat_assignment: { assignee: { email: (request?.details?.recipient?.email ?? '') } },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => seatingSeatAssignmentCalculationClient.quoteSeatAssignment(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  let selected2_1;
  switch (reply1?.seat_assignment?.origin) {
    case 'customer':
      selected2_1 = ((reply1?.seat_assignment?.origin === 'customer' ? reply1?.seat_assignment.customer : undefined)?.display_name ?? '');
      break;
    default:
      selected2_1 = '';
  }
  const request2 = {
    context: request?.context,
    seat_map: {
      children: [{ children: [{ manager: { display_name: selected2_1 } }] }],
    },
  };
  const reply2 = await call((done) => seatingSeatMapCalculationClient.calculateSeatMap(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: { region: (reply2?.seat_map?.location?.region ?? '') },
  };
  const merged3 = Object.assign({}, request3, { context: request3.context });
  const reply3 = await call((done) => timekeepingTimesheetQueryClient.suggestTimesheets(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    menu_item: {
      audit: { updated_by: (reply1?.seat_assignment?.audit?.updated_by ?? '') },
    },
  };
  const reply4 = await withRetry(() => call((done) => concessionsMenuItemLifecycleClient.scheduleMenuItemExpiry(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      seat_assignment: { sla: { policy_id: (reply1?.seat_assignment?.sla?.policy_id ?? '') } },
    },
    { outcome: { status: (reply2?.result?.status ?? '') } },
    { result: { status: (reply3?.result?.status ?? '') } },
    { outcome: { message: (reply4?.outcome?.message ?? '') } },
    {
      seat_assignment: {
        requester: {
          preferred_channel: (request?.details?.recipient?.preferred_channel ?? ''),
        },
        seat_assignment_id: (request?.seat_assignment_id ?? ''),
      },
    },
  );
}

/**
 * Streams the changes of seat assignments as they happen.
 */
async function streamSeatAssignmentMetrics(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'StreamSeatAssignmentMetrics', request);
  logger.info('stream_seat_assignment_metrics', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_policy: {
      audit: { created_at: request?.filter?.created_after },
      block: { notify_email: (request?.context?.actor?.email ?? '') },
      owner: {
        email: (request?.subscriber?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const task1 = deferred(() => call((done) => venuesVenuePolicySyncClient.pushVenuePolicy(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  const request2 = {
    context: request?.context,
    seat_hold: {
      participants: [{ contact: { email: (reply1?.venue_policy?.owner?.email ?? '') } }],
    },
  };
  const reply2 = await bus.request('acme.seating.v1.SeatAssignmentReportingService/StreamSeatAssignmentMetrics:1', request2);
  const request3 = {
    context: request?.context,
    seat_assignment: {
      audit: {
        created_at: reply2?.seat_hold?.audit?.created_at,
        etag: (reply2?.seat_hold?.audit?.etag ?? ''),
      },
    },
  };
  const reply3 = await runInContext(request.context, () => call((done) => seatingSeatAssignmentValidationClient.checkSeatAssignmentEligibility(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.filter?.region ?? '') });
  return assemble(
    {
      result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      seat_assignment: {
        assignee: { display_name: (reply1?.venue_policy?.owner?.display_name ?? '') },
      },
    },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    {
      seat_assignment: { audit: { created_at: reply3?.seat_assignment?.audit?.created_at } },
    },
    {
      seat_assignment: {
        requester: {
          phone: (request?.subscriber?.phone ?? ''),
          preferred_channel: (request?.subscriber?.preferred_channel ?? ''),
        },
      },
    },
  );
}

module.exports = { countSeatAssignmentTotals, deleteSeatAssignmentReport, fetchSeatAssignmentReport, generateSeatAssignmentReport, listSeatAssignmentReports, scheduleSeatAssignmentReport, shareSeatAssignmentReport, streamSeatAssignmentMetrics };
