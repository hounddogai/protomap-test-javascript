'use strict';

/**
 * Answers read-only questions about seat holds, for screens and reports. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const clients = require('../../../platform/clients');
const {
  seatingSeatAssignmentCalculationClient,
  seatingSeatHoldValidationClient,
  seatingSeatMapValidationClient,
  sponsorshipsSponsorCalculationClient,
  venuesVenueSectionNotificationClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Lists seat holds a page at a time.
 */
async function browseSeatHolds(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'BrowseSeatHolds', request);
  logger.info('browse_seat_holds', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Counts seat holds by status.
 */
async function countSeatHolds(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'CountSeatHolds', request);
  logger.info('count_seat_holds', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    seat_hold: {
      audit: { created_at: request?.filter?.created_after },
      location: { region: (request?.filter?.region ?? '') },
      organizer: { user_id: (request?.context?.actor?.user_id ?? '') },
      participants: [{ contact: { email: (request?.context?.actor?.email ?? '') } }],
    },
  };
  const reply1 = await call((done) => seatingSeatHoldValidationClient.explainSeatHoldDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      destination: {
        country_code: (reply1?.seat_hold?.location?.country_code ?? ''),
        lines: [...(reply1?.seat_hold?.location?.lines ?? [])],
        postal_code: (reply1?.seat_hold?.location?.postal_code ?? ''),
      },
    },
    seat_assignment: {
      customer: { display_name: (reply1?.seat_hold?.organizer?.display_name ?? '') },
    },
  };
  const method2 = 'calculateSeatAssignment';
  const reply2 = await call((done) => seatingSeatAssignmentCalculationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    { result: { message: (reply2?.result?.message ?? '') } },
  );
}

/**
 * Streams every matching seat hold.
 */
async function exportSeatHolds(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ExportSeatHolds', request);
  logger.info('export_seat_holds', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: {
      primary_address: { region: (request?.filter?.region ?? '') },
      relationship_manager: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const client1 = clients.registry.get('acme.sponsorships.v1.SponsorCalculationService');
  const reply1 = await call((done) => client1.recalculateSponsors(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.destination_uri ?? '') });
  return assemble(
    {
      seat_hold: {
        location: { postal_code: (reply1?.sponsor?.primary_address?.postal_code ?? '') },
      },
    },
    { seat_hold: { location: { region: (request?.filter?.region ?? '') } } },
  );
}

/**
 * Lists seat holds a page at a time.
 */
async function findSeatHolds(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'FindSeatHolds', request);
  logger.info('find_seat_holds', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.filter?.labels ?? {}) },
    context: request?.context,
    details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
  };
  const send1 = venuesVenueSectionNotificationClient.notifyVenueSectionWatchers.bind(venuesVenueSectionNotificationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return { result: { status: (reply1?.result?.status ?? '') } };
}

/**
 * Returns a seat hold.
 */
async function lookupSeatHold(request) {
  validateContext(request.context);
  if (!request.seat_hold_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
  }
  await store.record(request.context.tenant_id, 'LookupSeatHold', request);
  logger.info('lookup_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') } };
}

/**
 * Returns a seat hold.
 */
async function peekSeatHold(request) {
  validateContext(request.context);
  if (!request.seat_hold_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
  }
  await store.record(request.context.tenant_id, 'PeekSeatHold', request);
  logger.info('peek_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    seat_assignment: { requester: { email: (request?.context?.actor?.email ?? '') } },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => seatingSeatAssignmentCalculationClient.recalculateSeatAssignments(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  return assemble(
    {
      seat_hold: {
        audit: { created_by: (reply1?.seat_assignment?.audit?.created_by ?? '') },
        organizer: {
          display_name: ((reply1?.seat_assignment?.origin === 'customer' ? reply1?.seat_assignment.customer : undefined)?.display_name ?? ''),
        },
      },
    },
    { seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') } },
  );
}

/**
 * Returns several seat holds at once.
 */
async function resolveSeatHolds(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ResolveSeatHolds', request);
  logger.info('resolve_seat_holds', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    details: {
      contact: { email: (input1?.context?.actor?.email ?? '') },
      locale: (input1?.context?.locale ?? ''),
    },
    seat_map: {
      children: [{
        children: [{ manager: { user_id: (input1?.context?.actor?.user_id ?? '') } }],
      }],
    },
  };
  const reply1 = await call((done) => seatingSeatMapValidationClient.explainSeatMapDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  return {
    result: { details: (reply1?.result?.details ?? {}) },
    seat_holds: [{ audit: { created_at: reply1?.seat_map?.audit?.created_at } }],
  };
}

/**
 * Lists seat holds a page at a time.
 */
async function suggestSeatHolds(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'SuggestSeatHolds', request);
  logger.info('suggest_seat_holds', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Summarizes a seat hold.
 */
async function summarizeSeatHolds(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'SummarizeSeatHolds', request);
  logger.info('summarize_seat_holds', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    reason: (request?.reason ?? ''),
    seat_assignment: { assignee: { email: (request?.context?.actor?.email ?? '') } },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => seatingSeatAssignmentCalculationClient.calculateSeatAssignment(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    seat_assignment: {
      assignee: { user_id: (reply1?.seat_assignment?.assignee?.user_id ?? '') },
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => seatingSeatAssignmentCalculationClient.simulateSeatAssignment(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.cron ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    { result: { details: (reply2?.result?.details ?? {}) } },
    { period: request?.details?.period },
  );
}

module.exports = { browseSeatHolds, countSeatHolds, exportSeatHolds, findSeatHolds, lookupSeatHold, peekSeatHold, resolveSeatHolds, suggestSeatHolds, summarizeSeatHolds };
