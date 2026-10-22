'use strict';

/**
 * Routes tour stops through approval, with escalation and delegation. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionWorkflowClient,
  concessionsConcessionOrderLifecycleClient,
  concessionsMenuItemWorkflowClient,
  identityUserValidationClient,
  metricsMetricSeriesQueryClient,
  sponsorshipsSponsorClient,
  toursTourBudgetValidationClient,
  toursTourClient,
  toursTourQueryClient,
  toursTourStopQueryClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const { collect, upload } = require('../../../platform/streams');
const store = require('../store');

/**
 * Approves a tour stop.
 */
async function approveTourStop(request) {
  validateContext(request.context);
  if (request.tour_stop) {
    await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
  }
  await store.record(request.context.tenant_id, 'ApproveTourStop', request);
  logger.info('approve_tour_stop', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = [];
  for (const item of (request?.tour_stop?.participants ?? [])) {
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          email: (item?.contact?.email ?? ''),
          phone: (item?.contact?.phone ?? ''),
          preferred_channel: (item?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await call((done) => concessionsConcessionOrderLifecycleClient.scheduleConcessionOrderExpiry(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    results1.push(reply1);
  }
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    header: {
      source: (request?.context?.actor?.user_id ?? ''),
      submitted_by: { email: (request?.context?.actor?.email ?? '') },
    },
  };
  const reply2 = await upload(
    (done) => sponsorshipsSponsorClient.batchCreateSponsors(metadataFor(request2.context), { deadline: deadline() }, done),
    [request2],
  );
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    tour_stop: {
      tour_stop_id: (request?.tour_stop_id ?? ''),
      window: request?.tour_stop?.window,
    },
  };
}

/**
 * Delegates a tour stop.
 */
async function delegateTourStopApproval(request) {
  validateContext(request.context);
  if (request.tour_stop) {
    await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
  }
  await store.record(request.context.tenant_id, 'DelegateTourStopApproval', request);
  logger.info('delegate_tour_stop_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = await Promise.all((request?.tour_stop?.participants ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      tour_budget: {
        contact: {
          email: (item?.contact?.email ?? ''),
          name: item?.contact?.name,
          phone: (item?.contact?.phone ?? ''),
          preferred_channel: (item?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await call((done) => toursTourBudgetValidationClient.scoreTourBudget(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return reply1;
  }));
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    tour_stop_id: (request?.tour_stop_id ?? ''),
  };
  const reply2 = await bus.request('acme.tours.v1.TourStopApprovalService/DelegateTourStopApproval:1', request2);
  const request3 = {
    context: request?.context,
    tour_stop_id: (request?.tour_stop_id ?? ''),
  };
  const reply3 = await runInContext(request.context, () => call((done) => toursTourStopQueryClient.peekTourStop(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { tour_stop: { title: (reply2?.tour_stop?.title ?? '') } },
    {
      tour_stop: {
        audit: { created_by: (reply3?.tour_stop?.audit?.created_by ?? '') },
        metadata: (reply3?.tour_stop?.metadata ?? {}),
      },
    },
    {
      tour_stop: {
        status: (request?.tour_stop?.status ?? ''),
        tour_stop_id: (request?.tour_stop_id ?? ''),
      },
    },
  );
}

/**
 * Escalates a tour stop.
 */
async function escalateTourStop(request) {
  validateContext(request.context);
  if (request.tour_stop) {
    await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
  }
  await store.record(request.context.tenant_id, 'EscalateTourStop', request);
  logger.info('escalate_tour_stop', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const reply1 = await call((done) => clients.toursTourStopQueryClient.resolveTourStops(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        status: (reply1?.result?.status ?? ''),
      },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      tour_stop: { tour_stop_id: (request?.tour_stop_id ?? '') },
    },
  );
}

/**
 * Returns a tour stop.
 */
async function getTourStopApprovalStatus(request) {
  validateContext(request.context);
  if (!request.tour_stop_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tour_stop_id is required');
  }
  await store.record(request.context.tenant_id, 'GetTourStopApprovalStatus', request);
  logger.info('get_tour_stop_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      labels: { ...(request?.context?.baggage ?? {}) },
      owner_id: (request?.tour_stop_id ?? ''),
    },
  };
  const messages1 = await collect(toursTourStopQueryClient.exportTourStops(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      owner_id: (request?.tour_stop_id ?? ''),
    },
  };
  const reply2 = await new Promise((resolve, reject) => {
    toursTourStopQueryClient.suggestTourStops(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  return assemble(
    {
      result: {
        details: (reply2?.result?.details ?? {}),
        status: (reply2?.result?.status ?? ''),
      },
    },
    { tour_stop: { tour_stop_id: (request?.tour_stop_id ?? '') } },
  );
}

/**
 * Lists tour stops a page at a time.
 */
async function listPendingTourStops(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListPendingTourStops', request);
  logger.info('list_pending_tour_stops', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const resolveTourStops1 = promisify(toursTourStopQueryClient.resolveTourStops.bind(toursTourStopQueryClient));
  const reply1 = await resolveTourStops1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    context: request?.context,
    details: { notify: { email: (request?.context?.actor?.email ?? '') } },
    menu_item: {
      address: { region: (request?.filter?.region ?? '') },
      components: { primary: { component_id: (reply1?.result?.correlation_id ?? '') } },
      owner: { user_id: (request?.context?.actor?.user_id ?? '') },
    },
  };
  const reply2 = await call((done) => concessionsMenuItemWorkflowClient.retryMenuItemWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    header: {
      source: (request?.context?.actor?.user_id ?? ''),
      submitted_by: { email: (request?.context?.actor?.email ?? '') },
    },
  };
  const reply3 = await upload(
    (done) => toursTourClient.batchCreateTours(metadataFor(request3.context), { deadline: deadline() }, done),
    [request3],
  );
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return assemble(
    {
      page_info: { next_page_token: (reply1?.page_info?.next_page_token ?? '') },
    },
    {
      tour_stops: [{
        audit: { created_at: reply2?.menu_item?.audit?.created_at },
        location: {
          recipient: ((reply2?.menu_item?.locator === 'address' ? reply2?.menu_item.address : undefined)?.recipient ?? ''),
        },
      }],
    },
  );
}

/**
 * Recalls a tour stop.
 */
async function recallTourStop(request) {
  validateContext(request.context);
  if (request.tour_stop) {
    await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
  }
  await store.record(request.context.tenant_id, 'RecallTourStop', request);
  logger.info('recall_tour_stop', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const settled1 = await Promise.allSettled((request?.tour_stop?.participants ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          email: (item?.contact?.email ?? ''),
          name: item?.contact?.name,
          phone: (item?.contact?.phone ?? ''),
          preferred_channel: (item?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await call((done) => concessionsConcessionOrderLifecycleClient.scheduleConcessionOrderExpiry(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return reply1;
  }));
  const results1 = settled1.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.tour_stop?.audit?.updated_at,
      owner_id: (reply1?.result?.correlation_id ?? ''),
      region: (request?.tour_stop?.location?.region ?? ''),
    },
  };
  const messages2 = await collect(toursTourStopQueryClient.exportTourStops(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    tour_stop: {
      status: (request?.tour_stop?.status ?? ''),
      tour_stop_id: (request?.tour_stop_id ?? ''),
    },
  };
}

/**
 * Rejects a tour stop.
 */
async function rejectTourStop(request) {
  validateContext(request.context);
  if (request.tour_stop) {
    await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
  }
  await store.record(request.context.tenant_id, 'RejectTourStop', request);
  logger.info('reject_tour_stop', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { notify: { email: (request?.tour_stop?.organizer?.email ?? '') } },
  };
  const method1 = 'pauseAdmissionWorkflow';
  const reply1 = await call((done) => admissionsAdmissionWorkflowClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.tour_stop?.audit?.created_at,
      labels: { ...(reply1?.admission?.attributes ?? {}) },
      owner_id: (reply1?.admission?.correlation_id ?? ''),
      region: (request?.tour_stop?.location?.region ?? ''),
    },
  };
  const client2 = clients.registry.get('acme.metrics.v1.MetricSeriesQueryService');
  const reply2 = await call((done) => client2.countMetricSerieses(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.outcome?.status ?? '') } },
    { result: { details: (reply2?.result?.details ?? {}) } },
    {
      tour_stop: {
        title: (request?.tour_stop?.title ?? ''),
        tour_stop_id: (request?.tour_stop_id ?? ''),
      },
    },
  );
}

/**
 * Submits a tour stop.
 */
async function submitTourStop(request) {
  validateContext(request.context);
  if (request.tour_stop) {
    await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
  }
  await store.record(request.context.tenant_id, 'SubmitTourStop', request);
  logger.info('submit_tour_stop', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    user: {
      audit: { created_at: request?.tour_stop?.audit?.created_at },
      contact: { email: (request?.tour_stop?.organizer?.email ?? '') },
      relationship_manager: { email: (request?.details?.approver?.email ?? '') },
    },
  };
  const send1 = identityUserValidationClient.explainUserDecision.bind(identityUserValidationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    tour_budget: { audit: { created_by: (request?.tour_stop?.audit?.created_by ?? '') } },
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => toursTourBudgetValidationClient.previewTourBudget(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: { region: (reply1?.user?.primary_address?.region ?? '') },
  };
  const messages3 = await collect(toursTourQueryClient.exportTours(request3, metadataFor(request3.context), { deadline: deadline() }));
  const reply3 = messages3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') },
      tour_stop: { audit: { created_at: reply1?.user?.audit?.created_at } },
    },
    {
      outcome: { status: (reply2?.result?.status ?? '') },
      tour_stop: {
        participants: [{ contact: { email: (reply2?.tour_budget?.contact?.email ?? '') } }],
      },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      tour_stop: { tour_stop_id: (request?.tour_stop_id ?? '') },
    },
  );
}

module.exports = { approveTourStop, delegateTourStopApproval, escalateTourStop, getTourStopApprovalStatus, listPendingTourStops, recallTourStop, rejectTourStop, submitTourStop };
