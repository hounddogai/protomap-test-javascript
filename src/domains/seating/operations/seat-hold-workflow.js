'use strict';

/**
 * Runs the long-lived workflow that takes a seat hold from start to finish. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionApprovalClient,
  broadcastsStreamKeyApprovalClient,
  metricsMetricSeriesQueryClient,
  performersPerformerApprovalClient,
  searchSearchIndexQueryClient,
  seatingSeatHoldLifecycleClient,
  seatingSeatHoldValidationClient,
  seatingSeatMapApprovalClient,
  travelTripCalculationClient,
  venuesVenueClient,
  venuesVenueSectionNotificationClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Advances a seat hold.
 */
async function advanceSeatHoldWorkflow(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'AdvanceSeatHoldWorkflow', request);
  logger.info('advance_seat_hold_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      notify_contact: { email: (request?.seat_hold?.organizer?.email ?? '') },
    },
    seat_hold_id: (request?.seat_hold_id ?? ''),
  };
  const reply1 = await new Promise((resolve, reject) => {
    seatingSeatHoldLifecycleClient.activateSeatHold(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.seat_hold?.audit?.created_at,
      owner_id: (reply1?.seat_hold?.organizer?.user_id ?? ''),
      region: (request?.seat_hold?.location?.region ?? ''),
    },
  };
  const countMetricSerieses2 = promisify(metricsMetricSeriesQueryClient.countMetricSerieses.bind(metricsMetricSeriesQueryClient));
  const reply2 = await countMetricSerieses2(request2, metadataFor(request2.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    {
      result: {
        correlation_id: (reply2?.result?.correlation_id ?? ''),
        details: (reply2?.result?.details ?? {}),
      },
    },
    {
      seat_hold: {
        location: request?.seat_hold?.location,
        seat_hold_id: (request?.seat_hold_id ?? ''),
      },
    },
  );
}

/**
 * Cancels a seat hold.
 */
async function cancelSeatHoldWorkflow(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'CancelSeatHoldWorkflow', request);
  logger.info('cancel_seat_hold_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    performer: {
      audit: { created_by: (request?.seat_hold?.audit?.created_by ?? '') },
      primary_address: { recipient: (request?.seat_hold?.location?.recipient ?? '') },
      relationship_manager: { email: (request?.details?.notify?.email ?? '') },
    },
  };
  const reply1 = await call((done) => performersPerformerApprovalClient.delegatePerformerApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const results2 = [];
  for (const item of (request?.seat_hold?.participants ?? [])) {
    const request2 = {
      context: request?.context,
      details: { contact: { phone: (item?.contact?.phone ?? '') } },
      seat_hold: {
        participants: [{
          contact: {
            email: (item?.contact?.email ?? ''),
            name: item?.contact?.name,
          },
          participant_id: (item?.participant_id ?? ''),
        }],
      },
    };
    const reply2 = await call((done) => seatingSeatHoldValidationClient.explainSeatHoldDecision(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    results2.push(reply2);
  }
  const reply2 = results2.at(-1) ?? {};
  const results3 = await Promise.all((request?.seat_hold?.participants ?? []).map(async (item) => {
    const request3 = {
      context: request?.context,
      stream_key: {
        contact: {
          name: item?.contact?.name,
          phone: (item?.contact?.phone ?? ''),
          preferred_channel: (item?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply3 = await call((done) => broadcastsStreamKeyApprovalClient.commentStreamKeyApproval(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    return reply3;
  }));
  const reply3 = results3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') },
      seat_hold: {
        participants: [{
          contact: {
            preferred_channel: (reply1?.performer?.contact?.preferred_channel ?? ''),
          },
        }],
      },
    },
    {
      seat_hold: {
        seat_hold_id: (request?.seat_hold_id ?? ''),
        title: (request?.seat_hold?.title ?? ''),
      },
    },
  );
}

/**
 * Lists seat holds a page at a time.
 */
async function listSeatHoldWorkflows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListSeatHoldWorkflows', request);
  logger.info('list_seat_hold_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.filter?.created_after,
      owner_id: (request?.filter?.owner_id ?? ''),
      query: (request?.filter?.query ?? ''),
    },
    page: { page_token: (request?.page?.page_token ?? '') },
  };
  const method1 = 'listPendingAdmissions';
  const reply1 = await call((done) => admissionsAdmissionApprovalClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return { result: { message: (reply1?.result?.message ?? '') } };
}

/**
 * Resumes a seat hold.
 */
async function resumeSeatHoldWorkflow(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'ResumeSeatHoldWorkflow', request);
  logger.info('resume_seat_hold_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    seat_map: {
      children: [{
        children: [{
          audit: { updated_at: request?.seat_hold?.audit?.updated_at },
          manager: { display_name: (request?.seat_hold?.organizer?.display_name ?? '') },
        }],
      }],
    },
  };
  const client1 = clients.registry.get('acme.seating.v1.SeatMapApprovalService');
  const reply1 = await call((done) => client1.recallSeatMap(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.seat_hold?.audit?.created_at,
      labels: { ...(reply1?.seat_map?.attributes ?? {}) },
      owner_id: (request?.seat_hold?.organizer?.user_id ?? ''),
      region: (request?.seat_hold?.location?.region ?? ''),
    },
  };
  const messages2 = await collect(searchSearchIndexQueryClient.exportSearchIndexes(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      seat_hold: { location: { locality: (reply1?.seat_map?.location?.locality ?? '') } },
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
 * Signals a seat hold.
 */
async function signalSeatHoldWorkflow(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'SignalSeatHoldWorkflow', request);
  logger.info('signal_seat_hold_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      destination: { country_code: (request?.seat_hold?.location?.country_code ?? '') },
    },
    trip: {
      counterparty: { display_name: (request?.seat_hold?.organizer?.display_name ?? '') },
    },
  };
  const send1 = travelTripCalculationClient.compareTripOptions.bind(travelTripCalculationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const settled2 = await Promise.allSettled((request?.seat_hold?.participants ?? []).map(async (item) => {
    const request2 = {
      context: request?.context,
      venue: {
        address: { recipient: (reply1?.trip?.billing_address?.recipient ?? '') },
        audit: { created_at: reply1?.trip?.audit?.created_at },
        owner: { email: (item?.contact?.email ?? '') },
      },
    };
    const reply2 = await call((done) => venuesVenueClient.cloneVenue(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return reply2;
  }));
  const results2 = settled2.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply2 = results2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      seat_hold: {
        audit: { created_at: reply1?.trip?.audit?.created_at },
        metadata: (reply1?.trip?.metadata ?? {}),
      },
    },
    {
      seat_hold: {
        seat_hold_id: (request?.seat_hold_id ?? ''),
        title: (request?.seat_hold?.title ?? ''),
      },
    },
  );
}

/**
 * Starts a seat hold.
 */
async function startSeatHoldWorkflow(request) {
  validateContext(request.context);
  if (request.seat_hold) {
    await store.save(store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
  }
  await store.record(request.context.tenant_id, 'StartSeatHoldWorkflow', request);
  logger.info('start_seat_hold_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.seat_hold?.organizer?.email ?? ''),
        name: request?.details?.notify?.name,
        phone: (request?.details?.notify?.phone ?? ''),
      },
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => venuesVenueSectionNotificationClient.unsubscribeVenueSectionUpdates(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const results2 = [];
  for (const item of (request?.seat_hold?.participants ?? [])) {
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.seat_hold?.audit?.updated_at,
        owner_id: (item?.participant_id ?? ''),
        region: (request?.seat_hold?.location?.region ?? ''),
      },
    };
    const reply2 = await call((done) => searchSearchIndexQueryClient.findSearchIndexes(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    results2.push(reply2);
  }
  const reply2 = results2.at(-1) ?? {};
  const { context: context3, ...body3 } = request;
  const input3 = { ...body3, context: context3 };
  const request3 = {
    context: input3?.context,
    seat_map: {
      children: [{
        children: [{
          location: {
            locality: (reply1?.venue_section?.location?.locality ?? ''),
            region: (input3?.seat_hold?.location?.region ?? ''),
          },
        }],
      }],
    },
  };
  const reply3 = await call((done) => seatingSeatMapApprovalClient.recallSeatMap(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: {
        message: (reply1?.result?.message ?? ''),
        next_steps: (reply1?.outcome?.next_steps ?? []),
      },
    },
    { result: { status: (reply3?.outcome?.status ?? '') } },
    {
      seat_hold: {
        seat_hold_id: (request?.seat_hold_id ?? ''),
        title: (request?.seat_hold?.title ?? ''),
      },
    },
  );
}

module.exports = { advanceSeatHoldWorkflow, cancelSeatHoldWorkflow, listSeatHoldWorkflows, resumeSeatHoldWorkflow, signalSeatHoldWorkflow, startSeatHoldWorkflow };
