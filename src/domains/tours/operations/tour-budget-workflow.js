'use strict';

/**
 * Runs the long-lived workflow that takes a tour budget from start to finish. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  broadcastsBroadcastApprovalClient,
  broadcastsStreamKeyApprovalClient,
  campaignsMarketingCampaignQueryClient,
  searchSearchIndexQueryClient,
  sponsorshipsActivationApprovalClient,
  toursTourBudgetApprovalClient,
  toursTourBudgetValidationClient,
  toursTourStopQueryClient,
  venuesVenueExportClient,
  venuesVenuePolicySyncClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Advances a tour budget.
 */
async function advanceTourBudgetWorkflow(request) {
  validateContext(request.context);
  if (request.tour_budget) {
    await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
  }
  await store.record(request.context.tenant_id, 'AdvanceTourBudgetWorkflow', request);
  logger.info('advance_tour_budget_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    broadcast: {
      audit: { updated_by: (request?.tour_budget?.audit?.updated_by ?? '') },
      participants: [{
        contact: {
          email: (request?.details?.notify?.email ?? ''),
          phone: (request?.details?.notify?.phone ?? ''),
        },
      }],
    },
    context: request?.context,
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => broadcastsBroadcastApprovalClient.rejectBroadcast(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    annotations: { ...(input2?.annotations ?? {}) },
    context: input2?.context,
    details: { period: reply1?.broadcast?.window },
    reason: (input2?.reason ?? ''),
  };
  const reply2 = await call((done) => campaignsMarketingCampaignQueryClient.summarizeMarketingCampaigns(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    tour_budget: {
      audit: {
        updated_at: reply1?.broadcast?.audit?.updated_at,
        updated_by: (request?.tour_budget?.audit?.updated_by ?? ''),
      },
    },
  };
  const cloned3 = JSON.parse(JSON.stringify(request3));
  const reply3 = await call((done) => toursTourBudgetValidationClient.lintTourBudgets(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    { result: { message: (reply2?.result?.message ?? '') } },
    {
      tour_budget: { holder: { code: (reply3?.tour_budget?.holder?.code ?? '') } },
    },
    {
      tour_budget: {
        balance: request?.tour_budget?.balance,
        tour_budget_id: (request?.tour_budget_id ?? ''),
      },
    },
  );
}

/**
 * Cancels a tour budget.
 */
async function cancelTourBudgetWorkflow(request) {
  validateContext(request.context);
  if (request.tour_budget) {
    await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
  }
  await store.record(request.context.tenant_id, 'CancelTourBudgetWorkflow', request);
  logger.info('cancel_tour_budget_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.tour_budget?.audit?.created_at,
      labels: { ...(request?.tour_budget?.labels ?? {}) },
    },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => toursTourStopQueryClient.findTourStops(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { status: (reply1?.result?.status ?? '') },
      result: { message: (reply1?.result?.message ?? '') },
    },
    {
      tour_budget: {
        status: (request?.tour_budget?.status ?? ''),
        tour_budget_id: (request?.tour_budget_id ?? ''),
      },
    },
  );
}

/**
 * Returns a tour budget.
 */
async function inspectTourBudgetWorkflow(request) {
  validateContext(request.context);
  if (!request.tour_budget_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tour_budget_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectTourBudgetWorkflow', request);
  logger.info('inspect_tour_budget_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_policy: { owner: { email: (request?.context?.actor?.email ?? '') } },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => venuesVenuePolicySyncClient.pushVenuePolicy(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  return assemble(
    { result: { message: (reply1?.outcome?.message ?? '') } },
    { tour_budget: { tour_budget_id: (request?.tour_budget_id ?? '') } },
  );
}

/**
 * Lists tour budgets a page at a time.
 */
async function listTourBudgetWorkflows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListTourBudgetWorkflows', request);
  logger.info('list_tour_budget_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { created_after: request?.filter?.created_after },
    page: {
      page_size: (request?.page?.page_size ?? 0),
      page_token: (request?.page?.page_token ?? ''),
    },
  };
  const reply1 = await withRetry(() => call((done) => searchSearchIndexQueryClient.suggestSearchIndexes(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return { result: { details: (reply1?.result?.details ?? {}) } };
}

/**
 * Pauses a tour budget.
 */
async function pauseTourBudgetWorkflow(request) {
  validateContext(request.context);
  if (request.tour_budget) {
    await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
  }
  await store.record(request.context.tenant_id, 'PauseTourBudgetWorkflow', request);
  logger.info('pause_tour_budget_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    tour_budget: {
      audit: { created_by: (request?.tour_budget?.audit?.created_by ?? '') },
      contact: {
        preferred_channel: (request?.tour_budget?.contact?.preferred_channel ?? ''),
      },
    },
  };
  const task1 = deferred(() => call((done) => toursTourBudgetApprovalClient.approveTourBudget(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      tour_budget: { audit: { updated_by: (reply1?.tour_budget?.audit?.updated_by ?? '') } },
    },
    {
      tour_budget: {
        balance: request?.tour_budget?.balance,
        tour_budget_id: (request?.tour_budget_id ?? ''),
      },
    },
  );
}

/**
 * Resumes a tour budget.
 */
async function resumeTourBudgetWorkflow(request) {
  validateContext(request.context);
  if (request.tour_budget) {
    await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
  }
  await store.record(request.context.tenant_id, 'ResumeTourBudgetWorkflow', request);
  logger.info('resume_tour_budget_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.tour_budget?.opened_at,
      labels: { ...(request?.tour_budget?.labels ?? {}) },
      owner_id: (request?.tour_budget?.holder?.venue_id ?? ''),
    },
  };
  const reply1 = await bus.request('acme.tours.v1.TourBudgetWorkflowService/ResumeTourBudgetWorkflow:0', request1);
  const request2 = {
    activation: {
      assignee: { display_name: (request?.tour_budget?.holder?.display_name ?? '') },
      customer: { venue_id: (request?.tour_budget?.holder?.venue_id ?? '') },
      requester: {
        email: (request?.details?.notify?.email ?? ''),
        name: request?.details?.notify?.name,
      },
    },
    context: request?.context,
  };
  const reply2 = await runInContext(request.context, () => call((done) => sponsorshipsActivationApprovalClient.escalateActivation(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        message: (reply1?.result?.message ?? ''),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    },
    {
      outcome: { message: (reply2?.result?.message ?? '') },
      tour_budget: { audit: { created_at: reply2?.activation?.audit?.created_at } },
    },
    {
      tour_budget: {
        labels: (request?.tour_budget?.labels ?? {}),
        tour_budget_id: (request?.tour_budget_id ?? ''),
      },
    },
  );
}

/**
 * Retrys a tour budget.
 */
async function retryTourBudgetWorkflow(request) {
  validateContext(request.context);
  if (request.tour_budget) {
    await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
  }
  await store.record(request.context.tenant_id, 'RetryTourBudgetWorkflow', request);
  logger.info('retry_tour_budget_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_policy: {
      owner: {
        display_name: (request?.tour_budget?.holder?.display_name ?? ''),
        email: (request?.details?.notify?.email ?? ''),
      },
    },
  };
  const reply1 = await call((done) => clients.venuesVenuePolicySyncClient.syncVenuePolicies(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: reply1?.venue_policy?.audit?.created_at,
      labels: { ...(request?.tour_budget?.labels ?? {}) },
      owner_id: (reply1?.venue_policy?.owner?.user_id ?? ''),
    },
  };
  const reply2 = await new Promise((resolve, reject) => {
    toursTourBudgetApprovalClient.listPendingTourBudgets(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: {
        message: (reply1?.result?.message ?? ''),
        reference_id: (reply1?.outcome?.reference_id ?? ''),
      },
    },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    {
      tour_budget: {
        labels: (request?.tour_budget?.labels ?? {}),
        tour_budget_id: (request?.tour_budget_id ?? ''),
      },
    },
  );
}

/**
 * Signals a tour budget.
 */
async function signalTourBudgetWorkflow(request) {
  validateContext(request.context);
  if (request.tour_budget) {
    await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
  }
  await store.record(request.context.tenant_id, 'SignalTourBudgetWorkflow', request);
  logger.info('signal_tour_budget_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = [];
  for (const item of (request?.tour_budget?.limits ?? [])) {
    const request1 = {
      context: request?.context,
      stream_key: {
        limits: [{
          amount: item?.amount,
          name: (item?.name ?? ''),
          period: item?.period,
        }],
      },
    };
    const reply1 = await call((done) => broadcastsStreamKeyApprovalClient.recallStreamKey(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    results1.push(reply1);
  }
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    tour_budget_id: (request?.tour_budget_id ?? ''),
  };
  const getTourBudgetRules2 = promisify(toursTourBudgetValidationClient.getTourBudgetRules.bind(toursTourBudgetValidationClient));
  const reply2 = await getTourBudgetRules2(request2, metadataFor(request2.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      tour_budget: { contact: { email: (reply2?.tour_budget?.contact?.email ?? '') } },
    },
    {
      tour_budget: {
        balance: request?.tour_budget?.balance,
        tour_budget_id: (request?.tour_budget_id ?? ''),
      },
    },
  );
}

/**
 * Starts a tour budget.
 */
async function startTourBudgetWorkflow(request) {
  validateContext(request.context);
  if (request.tour_budget) {
    await store.save(store.toTourBudgetRecord(request.context.tenant_id, request.tour_budget));
  }
  await store.record(request.context.tenant_id, 'StartTourBudgetWorkflow', request);
  logger.info('start_tour_budget_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.tour_budget?.audit?.updated_at,
      labels: { ...(request?.tour_budget?.labels ?? {}) },
    },
  };
  const reply1 = await call((done) => toursTourStopQueryClient.countTourStops(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { status: (reply1?.result?.status ?? '') },
      result: { message: (reply1?.result?.message ?? '') },
    },
    {
      tour_budget: {
        labels: (request?.tour_budget?.labels ?? {}),
        tour_budget_id: (request?.tour_budget_id ?? ''),
      },
    },
  );
}

module.exports = { advanceTourBudgetWorkflow, cancelTourBudgetWorkflow, inspectTourBudgetWorkflow, listTourBudgetWorkflows, pauseTourBudgetWorkflow, resumeTourBudgetWorkflow, retryTourBudgetWorkflow, signalTourBudgetWorkflow, startTourBudgetWorkflow };
