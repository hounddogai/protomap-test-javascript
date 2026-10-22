'use strict';

/**
 * Runs the long-lived workflow that takes a tour from start to finish. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const clients = require('../../../platform/clients');
const {
  admissionsWristbandReportingClient,
  budgetingBudgetQueryClient,
  concessionsConcessionStandApprovalClient,
  identityUserClient,
  searchSearchIndexQueryClient,
  searchSearchIndexValidationClient,
  toursTourClient,
  toursTourQueryClient,
  toursTourStopApprovalClient,
  toursTourStopQueryClient,
  venuesVenuePolicySyncClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const { collect, exchange } = require('../../../platform/streams');
const store = require('../store');

/**
 * Advances a tour.
 */
async function advanceTourWorkflow(request) {
  validateContext(request.context);
  if (request.tour) {
    await store.save(store.toTourRecord(request.context.tenant_id, request.tour));
  }
  await store.record(request.context.tenant_id, 'AdvanceTourWorkflow', request);
  logger.info('advance_tour_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = await Promise.all((request?.tour?.participants ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      filter: { owner_id: (item?.participant_id ?? '') },
    };
    const messages1 = await collect(searchSearchIndexQueryClient.exportSearchIndexes(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    return reply1;
  }));
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    concession_stand: {
      address: { postal_code: (request?.tour?.location?.postal_code ?? '') },
    },
    context: request?.context,
    details: { approver: { email: (request?.tour?.organizer?.email ?? '') } },
  };
  const reply2 = await call((done) => clients.concessionsConcessionStandApprovalClient.recallConcessionStand(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      tour: {
        location: {
          country_code: ((reply2?.concession_stand?.locator === 'address' ? reply2?.concession_stand.address : undefined)?.country_code ?? ''),
        },
      },
    },
    {
      tour: {
        status: (request?.tour?.status ?? ''),
        tour_id: (request?.tour_id ?? ''),
      },
    },
  );
}

/**
 * Cancels a tour.
 */
async function cancelTourWorkflow(request) {
  validateContext(request.context);
  if (request.tour) {
    await store.save(store.toTourRecord(request.context.tenant_id, request.tour));
  }
  await store.record(request.context.tenant_id, 'CancelTourWorkflow', request);
  logger.info('cancel_tour_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.tour?.audit?.updated_at,
      region: (request?.tour?.location?.region ?? ''),
    },
  };
  const messages1 = await collect(toursTourQueryClient.exportTours(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const results2 = [];
  for (const item of (request?.tour?.participants ?? [])) {
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.tour?.audit?.created_at,
        labels: { ...(request?.details?.parameters ?? {}) },
        owner_id: (item?.participant_id ?? ''),
        region: (request?.tour?.location?.region ?? ''),
      },
    };
    const reply2 = await call((done) => budgetingBudgetQueryClient.countBudgets(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    results2.push(reply2);
  }
  const reply2 = results2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    tour: {
      location: request?.tour?.location,
      tour_id: (request?.tour_id ?? ''),
    },
  };
}

/**
 * Returns a tour.
 */
async function inspectTourWorkflow(request) {
  validateContext(request.context);
  if (!request.tour_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tour_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectTourWorkflow', request);
  logger.info('inspect_tour_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    user_id: (request?.context?.actor?.user_id ?? ''),
  };
  const reply1 = await new Promise((resolve, reject) => {
    identityUserClient.getUser(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  return assemble(
    {
      tour: {
        audit: {
          created_at: reply1?.user?.audit?.created_at,
          etag: (reply1?.user?.audit?.etag ?? ''),
        },
      },
    },
    { tour: { tour_id: (request?.tour_id ?? '') } },
  );
}

/**
 * Pauses a tour.
 */
async function pauseTourWorkflow(request) {
  validateContext(request.context);
  if (request.tour) {
    await store.save(store.toTourRecord(request.context.tenant_id, request.tour));
  }
  await store.record(request.context.tenant_id, 'PauseTourWorkflow', request);
  logger.info('pause_tour_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = await Promise.all((request?.tour?.participants ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.tour?.audit?.created_at,
        owner_id: (item?.participant_id ?? ''),
        region: (request?.tour?.location?.region ?? ''),
      },
    };
    const messages1 = await collect(toursTourStopQueryClient.exportTourStops(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    return reply1;
  }));
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    tour_stop: {
      participants: [{
        contact: {
          name: request?.details?.notify?.name,
          phone: (request?.details?.notify?.phone ?? ''),
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      }],
    },
  };
  const submitTourStop2 = promisify(toursTourStopApprovalClient.submitTourStop.bind(toursTourStopApprovalClient));
  const reply2 = await submitTourStop2(request2, metadataFor(request2.context), { deadline: deadline() });
  const results3 = await Promise.all((request?.tour?.participants ?? []).map(async (item) => {
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.tour?.audit?.created_at,
        owner_id: (item?.participant_id ?? ''),
        region: (request?.tour?.location?.region ?? ''),
      },
    };
    const reply3 = await call((done) => toursTourQueryClient.browseTours(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    return reply3;
  }));
  const reply3 = results3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      tour: { audit: { updated_by: (reply2?.tour_stop?.audit?.updated_by ?? '') } },
    },
    {
      tour: {
        title: (request?.tour?.title ?? ''),
        tour_id: (request?.tour_id ?? ''),
      },
    },
  );
}

/**
 * Resumes a tour.
 */
async function resumeTourWorkflow(request) {
  validateContext(request.context);
  if (request.tour) {
    await store.save(store.toTourRecord(request.context.tenant_id, request.tour));
  }
  await store.record(request.context.tenant_id, 'ResumeTourWorkflow', request);
  logger.info('resume_tour_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    reason: (request?.reason ?? ''),
  };
  const reply1 = await call((done) => budgetingBudgetQueryClient.summarizeBudgets(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.tour?.audit?.created_at,
      labels: { ...(reply1?.summary ?? {}) },
      owner_id: (request?.tour?.organizer?.user_id ?? ''),
      region: (request?.tour?.location?.region ?? ''),
    },
  };
  const method2 = 'listUsers';
  const reply2 = await call((done) => identityUserClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        status: (reply1?.result?.status ?? ''),
      },
    },
    { result: { message: (reply2?.result?.message ?? '') } },
    {
      tour: {
        location: request?.tour?.location,
        tour_id: (request?.tour_id ?? ''),
      },
    },
  );
}

/**
 * Signals a tour.
 */
async function signalTourWorkflow(request) {
  validateContext(request.context);
  if (request.tour) {
    await store.save(store.toTourRecord(request.context.tenant_id, request.tour));
  }
  await store.record(request.context.tenant_id, 'SignalTourWorkflow', request);
  logger.info('signal_tour_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    change: {
      audit: { created_by: (request?.tour?.audit?.created_by ?? '') },
      owner: {
        display_name: (request?.tour?.organizer?.display_name ?? ''),
        user_id: (request?.tour?.organizer?.user_id ?? ''),
      },
    },
    context: request?.context,
  };
  const messages1 = await exchange(venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request1.context), { deadline: deadline() }), [request1]);
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    tour: {
      audit: { created_at: request?.tour?.audit?.created_at },
      location: { recipient: (request?.tour?.location?.recipient ?? '') },
      participants: [{
        contact: {
          email: (request?.tour?.organizer?.email ?? ''),
          phone: (request?.details?.notify?.phone ?? ''),
        },
      }],
    },
  };
  const client2 = clients.registry.get('acme.tours.v1.TourService');
  const reply2 = await call((done) => client2.updateTour(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      tour: {
        participants: [{ contact: { email: (reply2?.tour?.organizer?.email ?? '') } }],
      },
    },
    {
      tour: {
        status: (request?.tour?.status ?? ''),
        tour_id: (request?.tour_id ?? ''),
      },
    },
  );
}

/**
 * Starts a tour.
 */
async function startTourWorkflow(request) {
  validateContext(request.context);
  if (request.tour) {
    await store.save(store.toTourRecord(request.context.tenant_id, request.tour));
  }
  await store.record(request.context.tenant_id, 'StartTourWorkflow', request);
  logger.info('start_tour_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.details?.notify?.email ?? ''),
        name: request?.details?.notify?.name,
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
  };
  const send1 = admissionsWristbandReportingClient.shareWristbandReport.bind(admissionsWristbandReportingClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    search_index: {
      address: {
        country_code: ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.country_code ?? ''),
      },
      audit: {
        updated_at: request?.tour?.audit?.updated_at,
        updated_by: (reply1?.wristband?.audit?.updated_by ?? ''),
      },
      owner: { user_id: (reply1?.wristband?.owner?.user_id ?? '') },
    },
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => searchSearchIndexValidationClient.checkSearchIndexEligibility(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  const settled3 = await Promise.allSettled((request?.tour?.participants ?? []).map(async (item) => {
    const request3 = {
      concession_stand: {
        address: { postal_code: (request?.tour?.location?.postal_code ?? '') },
      },
      context: request?.context,
      details: {
        approver: {
          display_name: (request?.tour?.organizer?.display_name ?? ''),
          email: (item?.contact?.email ?? ''),
        },
      },
    };
    const reply3 = await call((done) => concessionsConcessionStandApprovalClient.approveConcessionStand(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    return reply3;
  }));
  const results3 = settled3.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply3 = results3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { status: (reply1?.outcome?.status ?? '') },
      tour: { audit: { updated_by: (reply1?.wristband?.audit?.updated_by ?? '') } },
    },
    {
      tour: {
        location: {
          region: ((reply2?.search_index?.locator === 'address' ? reply2?.search_index.address : undefined)?.region ?? ''),
        },
      },
    },
    {
      tour: {
        metadata: (request?.tour?.metadata ?? {}),
        tour_id: (request?.tour_id ?? ''),
      },
    },
  );
}

module.exports = { advanceTourWorkflow, cancelTourWorkflow, inspectTourWorkflow, pauseTourWorkflow, resumeTourWorkflow, signalTourWorkflow, startTourWorkflow };
