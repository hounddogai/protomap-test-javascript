'use strict';

/**
 * Runs the long-lived workflow that takes a sponsorship deal from start to finish. Each operation validates, records,
 * and calls.
 */

const grpc = require('@grpc/grpc-js');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  metricsMetricSeriesValidationClient,
  sponsorshipsActivationLifecycleClient,
  sponsorshipsSponsorClient,
  sponsorshipsSponsorQueryClient,
  sponsorshipsSponsorValidationClient,
  sponsorshipsSponsorWorkflowClient,
  sponsorshipsSponsorshipDealApprovalClient,
  sponsorshipsSponsorshipDealQueryClient,
  sponsorshipsSponsorshipDealValidationClient,
  venuesVenuePolicySyncClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { exchange } = require('../../../platform/streams');
const store = require('../store');

/**
 * Advances a sponsorship deal.
 */
async function advanceSponsorshipDealWorkflow(request) {
  validateContext(request.context);
  if (request.sponsorship_deal) {
    await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
  }
  await store.record(request.context.tenant_id, 'AdvanceSponsorshipDealWorkflow', request);
  logger.info('advance_sponsorship_deal_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { approver: { email: (request?.details?.notify?.email ?? '') } },
    sponsorship_deal: {
      billing_address: {
        country_code: (request?.sponsorship_deal?.billing_address?.country_code ?? ''),
        lines: [...(request?.sponsorship_deal?.billing_address?.lines ?? [])],
      },
      card: {
        last4: ((request?.sponsorship_deal?.settlement === 'card' ? request?.sponsorship_deal.card : undefined)?.last4 ?? ''),
      },
    },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => sponsorshipsSponsorshipDealApprovalClient.rejectSponsorshipDeal(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        status: (reply1?.outcome?.status ?? ''),
      },
    },
    {
      sponsorship_deal: {
        notes: (request?.sponsorship_deal?.notes ?? ''),
        sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
      },
    },
  );
}

/**
 * Cancels a sponsorship deal.
 */
async function cancelSponsorshipDealWorkflow(request) {
  validateContext(request.context);
  if (request.sponsorship_deal) {
    await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
  }
  await store.record(request.context.tenant_id, 'CancelSponsorshipDealWorkflow', request);
  logger.info('cancel_sponsorship_deal_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: {
      contact: { name: request?.details?.notify?.name },
      primary_address: {
        country_code: (request?.sponsorship_deal?.billing_address?.country_code ?? ''),
        recipient: (request?.sponsorship_deal?.billing_address?.recipient ?? ''),
      },
      relationship_manager: { email: (request?.sponsorship_deal?.counterparty?.email ?? '') },
    },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => sponsorshipsSponsorClient.cloneSponsor(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { contact: { name: request?.details?.notify?.name } },
    sponsor: {
      audit: { updated_by: (request?.sponsorship_deal?.audit?.updated_by ?? '') },
      contact: { email: (request?.sponsorship_deal?.counterparty?.email ?? '') },
    },
  };
  const reply2 = await withRetry(() => call((done) => sponsorshipsSponsorValidationClient.explainSponsorDecision(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        message: (reply1?.result?.message ?? ''),
      },
    },
    { outcome: { status: (reply2?.outcome?.status ?? '') } },
    {
      sponsorship_deal: {
        number: (request?.sponsorship_deal?.number ?? ''),
        sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
      },
    },
  );
}

/**
 * Returns a sponsorship deal.
 */
async function inspectSponsorshipDealWorkflow(request) {
  validateContext(request.context);
  if (!request.sponsorship_deal_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsorship_deal_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectSponsorshipDealWorkflow', request);
  logger.info('inspect_sponsorship_deal_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      owner_id: (request?.sponsorship_deal_id ?? ''),
    },
  };
  const task1 = deferred(() => call((done) => sponsorshipsSponsorQueryClient.countSponsors(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        message: (reply1?.result?.message ?? ''),
      },
    },
    {
      sponsorship_deal: { sponsorship_deal_id: (request?.sponsorship_deal_id ?? '') },
    },
  );
}

/**
 * Lists sponsorship deals a page at a time.
 */
async function listSponsorshipDealWorkflows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListSponsorshipDealWorkflows', request);
  logger.info('list_sponsorship_deal_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: { email: (request?.context?.actor?.email ?? '') },
      locale: (request?.context?.locale ?? ''),
    },
    sponsorship_deal: { billing_address: { region: (request?.filter?.region ?? '') } },
  };
  const reply1 = await bus.request('acme.sponsorships.v1.SponsorshipDealWorkflowService/ListSponsorshipDealWorkflows:0', request1);
  const request2 = {
    change: {
      block: { notify_email: (request?.context?.actor?.email ?? '') },
      parameters: { ...(request?.context?.baggage ?? {}) },
      scopes: [...(request?.context?.actor?.roles ?? [])],
    },
    context: request?.context,
  };
  const messages2 = await exchange(venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request2.context), { deadline: deadline() }), [request2]);
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return { result: { details: (reply1?.result?.details ?? {}) } };
}

/**
 * Pauses a sponsorship deal.
 */
async function pauseSponsorshipDealWorkflow(request) {
  validateContext(request.context);
  if (request.sponsorship_deal) {
    await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
  }
  await store.record(request.context.tenant_id, 'PauseSponsorshipDealWorkflow', request);
  logger.info('pause_sponsorship_deal_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const reply1 = await runInContext(request.context, () => call((done) => sponsorshipsSponsorshipDealQueryClient.resolveSponsorshipDeals(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    details: {
      approver: { email: (request?.sponsorship_deal?.counterparty?.email ?? '') },
    },
  };
  const reply2 = await call((done) => clients.broadcastsReplayClipApprovalClient.delegateReplayClipApproval(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    {
      sponsorship_deal: {
        billing_address: {
          locality: ((reply2?.replay_clip?.locator === 'address' ? reply2?.replay_clip.address : undefined)?.locality ?? ''),
        },
      },
    },
    {
      sponsorship_deal: {
        notes: (request?.sponsorship_deal?.notes ?? ''),
        sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
      },
    },
  );
}

/**
 * Resumes a sponsorship deal.
 */
async function resumeSponsorshipDealWorkflow(request) {
  validateContext(request.context);
  if (request.sponsorship_deal) {
    await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
  }
  await store.record(request.context.tenant_id, 'ResumeSponsorshipDealWorkflow', request);
  logger.info('resume_sponsorship_deal_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: {
        email: (request?.details?.notify?.email ?? ''),
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    metricsMetricSeriesValidationClient.validateMetricSeries(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { effective_at: reply1?.outcome?.effective_at },
      sponsorship_deal: {
        billing_address: {
          lines: ((reply1?.metric_series?.locator === 'address' ? reply1?.metric_series.address : undefined)?.lines ?? []),
        },
      },
    },
    {
      sponsorship_deal: {
        sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
        status: (request?.sponsorship_deal?.status ?? ''),
      },
    },
  );
}

/**
 * Retrys a sponsorship deal.
 */
async function retrySponsorshipDealWorkflow(request) {
  validateContext(request.context);
  if (request.sponsorship_deal) {
    await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
  }
  await store.record(request.context.tenant_id, 'RetrySponsorshipDealWorkflow', request);
  logger.info('retry_sponsorship_deal_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  let selected1_3;
  switch (request?.sponsorship_deal?.settlement) {
    case 'card':
      selected1_3 = ((request?.sponsorship_deal?.settlement === 'card' ? request?.sponsorship_deal.card : undefined)?.network ?? '');
      break;
    default:
      selected1_3 = '';
  }
  const request1 = {
    context: request?.context,
    sponsorship_deal: {
      audit: {
        created_at: request?.sponsorship_deal?.audit?.created_at,
        updated_at: request?.sponsorship_deal?.audit?.updated_at,
      },
      card: { network: selected1_3 },
      counterparty: {
        display_name: (request?.sponsorship_deal?.counterparty?.display_name ?? ''),
      },
    },
  };
  const reply1 = await call((done) => sponsorshipsSponsorshipDealApprovalClient.approveSponsorshipDeal(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    {
      sponsorship_deal: {
        sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
        total: request?.sponsorship_deal?.total,
      },
    },
  );
}

/**
 * Starts a sponsorship deal.
 */
async function startSponsorshipDealWorkflow(request) {
  validateContext(request.context);
  if (request.sponsorship_deal) {
    await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
  }
  await store.record(request.context.tenant_id, 'StartSponsorshipDealWorkflow', request);
  logger.info('start_sponsorship_deal_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: {
      audit: { updated_at: request?.sponsorship_deal?.audit?.updated_at },
      primary_address: {
        locality: (request?.sponsorship_deal?.billing_address?.locality ?? ''),
      },
    },
  };
  const reply1 = await call((done) => sponsorshipsSponsorWorkflowClient.resumeSponsorWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    activation: {
      assignee: {
        display_name: (request?.sponsorship_deal?.counterparty?.display_name ?? ''),
      },
      audit: { updated_at: request?.sponsorship_deal?.audit?.updated_at },
      customer: {
        display_name: (reply1?.sponsor?.relationship_manager?.display_name ?? ''),
      },
    },
    context: request?.context,
  };
  const method2 = 'scheduleActivationExpiry';
  const reply2 = await call((done) => sponsorshipsActivationLifecycleClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      sponsorship_deal: {
        audit: { created_by: (reply1?.sponsor?.audit?.created_by ?? '') },
        counterparty: { email: (reply1?.sponsor?.relationship_manager?.email ?? '') },
      },
    },
    {
      result: {
        correlation_id: (reply2?.result?.correlation_id ?? ''),
        status: (reply2?.result?.status ?? ''),
      },
    },
    {
      sponsorship_deal: {
        sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
        status: (request?.sponsorship_deal?.status ?? ''),
      },
    },
  );
}

module.exports = { advanceSponsorshipDealWorkflow, cancelSponsorshipDealWorkflow, inspectSponsorshipDealWorkflow, listSponsorshipDealWorkflows, pauseSponsorshipDealWorkflow, resumeSponsorshipDealWorkflow, retrySponsorshipDealWorkflow, startSponsorshipDealWorkflow };
