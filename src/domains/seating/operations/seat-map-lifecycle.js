'use strict';

/**
 * Moves a seat map between the states of its lifecycle. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  auditAuditEventQueryClient,
  boxofficeGuestListApprovalClient,
  concessionsMenuItemReportingClient,
  ledgerLedgerAccountClient,
  seatingSeatAssignmentCalculationClient,
  seatingSeatAssignmentValidationClient,
  seatingSeatHoldCalculationClient,
  seatingSeatHoldQueryClient,
  seatingSeatHoldReportingClient,
  seatingSeatHoldValidationClient,
  seatingSeatMapValidationClient,
  sponsorshipsSponsorshipDealApprovalClient,
  venuesVenueQueryClient,
  venuesVenueSectionNotificationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Activates a seat map.
 */
async function activateSeatMap(request) {
  validateContext(request.context);
  if (!request.seat_map_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
  }
  await store.record(request.context.tenant_id, 'ActivateSeatMap', request);
  logger.info('activate_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => seatingSeatHoldReportingClient.listSeatHoldReports(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    seat_assignment: { assignee: { email: (request?.details?.notify_contact?.email ?? '') } },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => seatingSeatAssignmentValidationClient.normalizeSeatAssignment(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    details: {
      approver: {
        display_name: (reply2?.seat_assignment?.assignee?.display_name ?? ''),
        user_id: (reply2?.seat_assignment?.assignee?.user_id ?? ''),
      },
    },
    guest_list: {
      audit: { etag: (reply2?.seat_assignment?.audit?.etag ?? '') },
      counterparty: { display_name: (request?.details?.new_owner?.display_name ?? '') },
    },
  };
  const merged3 = Object.assign({}, request3, { context: request3.context });
  const reply3 = await call((done) => boxofficeGuestListApprovalClient.recallGuestList(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    details: { recipient: { phone: (request?.details?.notify_contact?.phone ?? '') } },
  };
  const reply4 = await withRetry(() => call((done) => venuesVenueSectionNotificationClient.sendVenueSectionTestNotice(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
    },
    {
      seat_map: {
        children: [{
          audit: { updated_by: (reply2?.seat_assignment?.audit?.updated_by ?? '') },
        }],
      },
    },
    {
      result: { correlation_id: (reply3?.result?.correlation_id ?? '') },
      seat_map: {
        children: [{
          location: { region: (reply3?.guest_list?.billing_address?.region ?? '') },
        }],
      },
    },
    {
      seat_map: {
        children: [{ manager: { email: (reply4?.venue_section?.manager?.email ?? '') } }],
        manager: { email: (reply4?.venue_section?.manager?.email ?? '') },
      },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      seat_map: { seat_map_id: (request?.seat_map_id ?? '') },
    },
  );
}

/**
 * Expires a seat map.
 */
async function expireSeatMap(request) {
  validateContext(request.context);
  if (!request.seat_map_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
  }
  await store.record(request.context.tenant_id, 'ExpireSeatMap', request);
  logger.info('expire_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const task1 = deferred(() => call((done) => seatingSeatHoldQueryClient.resolveSeatHolds(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  const request2 = {
    context: request?.context,
    details: { contact: { email: (request?.details?.notify_contact?.email ?? '') } },
  };
  const reply2 = await bus.request('acme.seating.v1.SeatMapLifecycleService/ExpireSeatMap:1', request2);
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    {
      seat_map: {
        audit: { created_at: reply2?.seat_assignment?.audit?.created_at },
        manager: { email: (reply2?.seat_assignment?.assignee?.email ?? '') },
      },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      seat_map: { seat_map_id: (request?.seat_map_id ?? '') },
    },
  );
}

/**
 * Returns a seat map.
 */
async function getSeatMapLifecycle(request) {
  validateContext(request.context);
  if (!request.seat_map_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
  }
  await store.record(request.context.tenant_id, 'GetSeatMapLifecycle', request);
  logger.info('get_seat_map_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { seat_map: { seat_map_id: (request?.seat_map_id ?? '') } };
}

/**
 * Reinstates a seat map.
 */
async function reinstateSeatMap(request) {
  validateContext(request.context);
  if (!request.seat_map_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
  }
  await store.record(request.context.tenant_id, 'ReinstateSeatMap', request);
  logger.info('reinstate_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.seat_map_id ?? ''),
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => ledgerLedgerAccountClient.listLedgerAccounts(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      result: { status: (reply1?.result?.status ?? '') },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      seat_map: { seat_map_id: (request?.seat_map_id ?? '') },
    },
  );
}

/**
 * Renews a seat map.
 */
async function renewSeatMap(request) {
  validateContext(request.context);
  if (!request.seat_map_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
  }
  await store.record(request.context.tenant_id, 'RenewSeatMap', request);
  logger.info('renew_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
    },
  };
  const reply1 = await call((done) => clients.seatingSeatMapClient.listSeatMaps(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { approver: { email: (request?.details?.notify_contact?.email ?? '') } },
    sponsorship_deal: {
      counterparty: { display_name: (request?.details?.new_owner?.display_name ?? '') },
    },
  };
  const reply2 = await new Promise((resolve, reject) => {
    sponsorshipsSponsorshipDealApprovalClient.delegateSponsorshipDealApproval(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request3 = {
    context: request?.context,
    seat_hold: {
      participants: [{
        contact: {
          phone: (request?.details?.notify_contact?.phone ?? ''),
          preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
        },
      }],
    },
  };
  const explainSeatHoldDecision3 = promisify(seatingSeatHoldValidationClient.explainSeatHoldDecision.bind(seatingSeatHoldValidationClient));
  const reply3 = await explainSeatHoldDecision3(request3, metadataFor(request3.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { status: (reply1?.result?.status ?? '') },
      result: { message: (reply1?.result?.message ?? '') },
    },
    {
      result: { status: (reply2?.outcome?.status ?? '') },
      seat_map: {
        children: [{
          children: [{
            location: { locality: (reply2?.sponsorship_deal?.billing_address?.locality ?? '') },
          }],
        }],
      },
    },
    {
      seat_map: {
        children: [{ audit: { created_by: (reply3?.seat_hold?.audit?.created_by ?? '') } }],
      },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      seat_map: { seat_map_id: (request?.seat_map_id ?? '') },
    },
  );
}

/**
 * Schedules a seat map.
 */
async function scheduleSeatMapExpiry(request) {
  validateContext(request.context);
  if (request.seat_map) {
    await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
  }
  await store.record(request.context.tenant_id, 'ScheduleSeatMapExpiry', request);
  logger.info('schedule_seat_map_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.seat_map?.audit?.updated_at,
      region: (request?.seat_map?.location?.region ?? ''),
    },
  };
  const reply1 = await call((done) => venuesVenueQueryClient.countVenues(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.seat_map?.audit?.created_at,
      owner_id: (request?.seat_map?.seat_map_id ?? ''),
      region: (request?.seat_map?.location?.region ?? ''),
    },
  };
  const method2 = 'countSeatHoldTotals';
  const reply2 = await call((done) => seatingSeatHoldReportingClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    ledger_account: {
      audit: { updated_by: (request?.seat_map?.audit?.updated_by ?? '') },
      contact: {
        name: request?.details?.notify?.name,
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
  };
  const client3 = clients.registry.get('acme.ledger.v1.LedgerAccountService');
  const reply3 = await call((done) => client3.cloneLedgerAccount(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    seat_assignment: {
      audit: {
        created_by: (request?.seat_map?.audit?.created_by ?? ''),
        updated_at: reply3?.ledger_account?.audit?.updated_at,
      },
    },
  };
  const send4 = seatingSeatAssignmentCalculationClient.calculateSeatAssignment.bind(seatingSeatAssignmentCalculationClient);
  const reply4 = await call((done) => send4(request4, metadataFor(request4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    {
      result: {
        reference_ids: (reply2?.result?.reference_ids ?? []),
        status: (reply2?.result?.status ?? ''),
      },
    },
    {
      seat_map: {
        children: [{
          children: [{
            audit: { created_by: (reply3?.ledger_account?.audit?.created_by ?? '') },
          }],
        }],
      },
    },
    {
      seat_map: {
        children: [{
          audit: {
            created_at: reply4?.seat_assignment?.audit?.created_at,
            created_by: (reply4?.seat_assignment?.audit?.created_by ?? ''),
          },
        }],
      },
    },
    {
      seat_map: {
        attributes: (request?.seat_map?.attributes ?? {}),
        seat_map_id: (request?.seat_map_id ?? ''),
      },
    },
  );
}

/**
 * Suspends a seat map.
 */
async function suspendSeatMap(request) {
  validateContext(request.context);
  if (!request.seat_map_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
  }
  await store.record(request.context.tenant_id, 'SuspendSeatMap', request);
  logger.info('suspend_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    seat_hold: {
      participants: [{
        contact: {
          email: (request?.details?.new_owner?.email ?? ''),
          preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
        },
      }],
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => seatingSeatHoldCalculationClient.simulateSeatHold(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    context: input2?.context,
    filter: { region: (reply1?.seat_hold?.location?.region ?? '') },
  };
  const reply2 = await call((done) => concessionsMenuItemReportingClient.listMenuItemReports(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    seat_map: {
      children: [{
        children: [{
          audit: { created_at: reply1?.seat_hold?.audit?.created_at },
          location: { region: (reply1?.seat_hold?.location?.region ?? '') },
          manager: {
            display_name: (reply1?.seat_hold?.organizer?.display_name ?? ''),
            email: (reply1?.seat_hold?.organizer?.email ?? ''),
          },
        }],
      }],
    },
  };
  const cloned3 = JSON.parse(JSON.stringify(request3));
  const reply3 = await call((done) => seatingSeatMapValidationClient.previewSeatMap(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      seat_map: { audit: { created_at: reply1?.seat_hold?.audit?.created_at } },
    },
    { result: { details: (reply2?.result?.details ?? {}) } },
    {
      outcome: { reference_id: (reply3?.outcome?.reference_id ?? '') },
      seat_map: { children: [{ children: [{ name: (reply3?.seat_map?.name ?? '') }] }] },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      seat_map: { seat_map_id: (request?.seat_map_id ?? '') },
    },
  );
}

/**
 * Terminates a seat map.
 */
async function terminateSeatMap(request) {
  validateContext(request.context);
  if (!request.seat_map_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
  }
  await store.record(request.context.tenant_id, 'TerminateSeatMap', request);
  logger.info('terminate_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { labels: { ...(request?.annotations ?? {}) } },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => auditAuditEventQueryClient.findAuditEvents(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    {
      outcome: { effective_at: request?.details?.effective_at },
      seat_map: { seat_map_id: (request?.seat_map_id ?? '') },
    },
  );
}

module.exports = { activateSeatMap, expireSeatMap, getSeatMapLifecycle, reinstateSeatMap, renewSeatMap, scheduleSeatMapExpiry, suspendSeatMap, terminateSeatMap };
