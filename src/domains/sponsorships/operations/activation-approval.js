'use strict';

/**
 * Routes activations through approval, with escalation and delegation. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  broadcastsBroadcastValidationClient,
  broadcastsBroadcastWorkflowClient,
  broadcastsReplayClipWorkflowClient,
  featureflagsFeatureFlagValidationClient,
  identityUserValidationClient,
  metricsMetricSeriesClient,
  seatingSeatMapClient,
  sponsorshipsActivationLifecycleClient,
  sponsorshipsActivationValidationClient,
  sponsorshipsSponsorApprovalClient,
  sponsorshipsSponsorValidationClient,
  sponsorshipsSponsorshipDealApprovalClient,
  sponsorshipsSponsorshipDealCalculationClient,
  venuesVenuePolicyNotificationClient,
  venuesVenuePolicySyncClient,
  venuesVenueSectionSyncClient,
  webhooksWebhookEndpointQueryClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const { collect, exchange, upload } = require('../../../platform/streams');
const store = require('../store');

/**
 * Approves an activation.
 */
async function approveActivation(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'ApproveActivation', request);
  logger.info('approve_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsorship_deal: {
      audit: { etag: (request?.activation?.audit?.etag ?? '') },
      counterparty: { email: (request?.activation?.assignee?.email ?? '') },
    },
  };
  const reply1 = await bus.request('acme.sponsorships.v1.ActivationApprovalService/ApproveActivation:0', request1);
  const request2 = {
    context: request?.context,
    subscriber: { email: (request?.activation?.requester?.email ?? '') },
  };
  const messages2 = await collect(venuesVenuePolicyNotificationClient.streamVenuePolicyNotices(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        priority: (request?.activation?.priority ?? 0),
      },
    },
  );
}

/**
 * Comments an activation.
 */
async function commentActivationApproval(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'CommentActivationApproval', request);
  logger.info('comment_activation_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const settled1 = await Promise.allSettled((request?.activation?.comments ?? []).map(async (item) => {
    const request1 = {
      broadcast: {
        audit: {
          created_at: item?.created_at,
          created_by: (request?.activation?.audit?.created_by ?? ''),
        },
        participants: [{ contact: { email: (request?.activation?.requester?.email ?? '') } }],
      },
      context: request?.context,
      details: { notify: { email: (item?.author_email ?? '') } },
    };
    const reply1 = await call((done) => broadcastsBroadcastWorkflowClient.pauseBroadcastWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return reply1;
  }));
  const results1 = settled1.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    change: {
      block: {
        notify_email: (request?.activation?.assignee?.email ?? ''),
        reason: (request?.reason ?? ''),
      },
      priority: (request?.activation?.priority ?? 0),
    },
    context: request?.context,
  };
  const messages2 = await exchange(venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request2.context), { deadline: deadline() }), [request2]);
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
  return {
    activation: {
      activation_id: (request?.activation_id ?? ''),
      assignee: request?.activation?.assignee,
    },
  };
}

/**
 * Delegates an activation.
 */
async function delegateActivationApproval(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'DelegateActivationApproval', request);
  logger.info('delegate_activation_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: {
      relationship_manager: { email: (request?.activation?.requester?.email ?? '') },
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => sponsorshipsSponsorValidationClient.previewSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const results2 = [];
  for (const item of (request?.activation?.comments ?? [])) {
    const request2 = {
      context: request?.context,
      sponsorship_deal: {
        audit: { created_at: item?.created_at },
        counterparty: { email: (item?.author_email ?? '') },
      },
    };
    const reply2 = await call((done) => sponsorshipsSponsorshipDealCalculationClient.recalculateSponsorshipDeals(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    results2.push(reply2);
  }
  const reply2 = results2.at(-1) ?? {};
  const request3 = {
    context: request?.context,
    vehicle: {
      address: { locality: (reply1?.sponsor?.primary_address?.locality ?? '') },
      owner: {
        display_name: (reply2?.sponsorship_deal?.counterparty?.display_name ?? ''),
        email: (reply1?.sponsor?.contact?.email ?? ''),
      },
    },
  };
  const reply3 = await call((done) => clients.fleetVehicleClient.createVehicle(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    sponsor: {
      audit: { etag: (reply1?.sponsor?.audit?.etag ?? '') },
      contact: {
        preferred_channel: (request?.activation?.requester?.preferred_channel ?? ''),
      },
    },
  };
  const reply4 = await new Promise((resolve, reject) => {
    sponsorshipsSponsorValidationClient.normalizeSponsor(request4, metadataFor(request4.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      activation: {
        assignee: {
          display_name: (reply1?.sponsor?.relationship_manager?.display_name ?? ''),
        },
        requester: { email: (reply1?.sponsor?.relationship_manager?.email ?? '') },
      },
    },
    {
      activation: { assignee: { email: (reply3?.vehicle?.owner?.email ?? '') } },
      result: { message: (reply3?.result?.message ?? '') },
    },
    { result: { message: (reply4?.outcome?.message ?? '') } },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        status: (request?.activation?.status ?? ''),
      },
    },
  );
}

/**
 * Escalates an activation.
 */
async function escalateActivation(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'EscalateActivation', request);
  logger.info('escalate_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    metric_series: { owner: { user_id: (request?.activation?.assignee?.user_id ?? '') } },
  };
  const createMetricSeries1 = promisify(metricsMetricSeriesClient.createMetricSeries.bind(metricsMetricSeriesClient));
  const reply1 = await createMetricSeries1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    broadcast: {
      participants: [{
        contact: {
          email: (reply1?.metric_series?.owner?.email ?? ''),
          name: request?.activation?.requester?.name,
        },
      }],
    },
    context: request?.context,
  };
  const reply2 = await call((done) => broadcastsBroadcastValidationClient.normalizeBroadcast(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      activation: {
        audit: { updated_by: (reply1?.metric_series?.audit?.updated_by ?? '') },
      },
    },
    {
      activation: { audit: { updated_by: (reply2?.broadcast?.audit?.updated_by ?? '') } },
      result: { message: (reply2?.outcome?.message ?? '') },
    },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        requester: request?.activation?.requester,
      },
    },
  );
}

/**
 * Returns an activation.
 */
async function getActivationApprovalStatus(request) {
  validateContext(request.context);
  if (!request.activation_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'activation_id is required');
  }
  await store.record(request.context.tenant_id, 'GetActivationApprovalStatus', request);
  logger.info('get_activation_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    activation: { activation_id: (request?.activation_id ?? '') },
    context: request?.context,
  };
  const method1 = 'lintActivations';
  const reply1 = await call((done) => sponsorshipsActivationValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { contact: { name: reply1?.activation?.requester?.name } },
  };
  const client2 = clients.registry.get('acme.identity.v1.UserValidationService');
  const reply2 = await call((done) => client2.checkUserEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  return assemble(
    {
      activation: {
        requester: {
          preferred_channel: (reply1?.activation?.requester?.preferred_channel ?? ''),
        },
        sla: { due_at: reply1?.activation?.sla?.due_at },
      },
    },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    { activation: { activation_id: (request?.activation_id ?? '') } },
  );
}

/**
 * Lists activations a page at a time.
 */
async function listPendingActivations(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListPendingActivations', request);
  logger.info('list_pending_activations', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.filter?.created_after,
      owner_id: (request?.filter?.owner_id ?? ''),
      region: (request?.filter?.region ?? ''),
    },
  };
  const messages1 = await collect(webhooksWebhookEndpointQueryClient.exportWebhookEndpoints(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    header: {
      source: (request?.context?.actor?.user_id ?? ''),
      submitted_by: { email: (request?.context?.actor?.email ?? '') },
    },
  };
  const reply2 = await upload(
    (done) => seatingSeatMapClient.batchCreateSeatMaps(metadataFor(request2.context), { deadline: deadline() }, done),
    [request2],
  );
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Recalls an activation.
 */
async function recallActivation(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'RecallActivation', request);
  logger.info('recall_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsorship_deal: {
      counterparty: {
        display_name: ((request?.activation?.origin === 'customer' ? request?.activation.customer : undefined)?.display_name ?? ''),
        email: (request?.details?.approver?.email ?? ''),
      },
    },
  };
  const send1 = sponsorshipsSponsorshipDealCalculationClient.calculateSponsorshipDeal.bind(sponsorshipsSponsorshipDealCalculationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    change: {
      children: [{
        children: [{ parent_id: (request?.activation?.activation_id ?? '') }],
        parent_id: (request?.activation?.activation_id ?? ''),
      }],
      parent_id: (request?.activation?.activation_id ?? ''),
    },
    context: request?.context,
  };
  const messages2 = await exchange(venuesVenueSectionSyncClient.streamVenueSectionChanges(metadataFor(request2.context), { deadline: deadline() }), [request2]);
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      activation: {
        audit: { created_by: (reply1?.sponsorship_deal?.audit?.created_by ?? '') },
      },
    },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        custom_fields: (request?.activation?.custom_fields ?? {}),
      },
    },
  );
}

/**
 * Rejects an activation.
 */
async function rejectActivation(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'RejectActivation', request);
  logger.info('reject_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { approver: { email: (request?.activation?.assignee?.email ?? '') } },
    sponsor: { contact: { name: request?.activation?.requester?.name } },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => sponsorshipsSponsorApprovalClient.submitSponsor(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const results2 = await Promise.all((request?.activation?.comments ?? []).map(async (item) => {
    const request2 = {
      context: request?.context,
      feature_flag: {
        audit: {
          created_at: item?.created_at,
          updated_at: reply1?.sponsor?.audit?.updated_at,
        },
        owner: { email: (item?.author_email ?? '') },
      },
    };
    const reply2 = await call((done) => featureflagsFeatureFlagValidationClient.scoreFeatureFlag(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return reply2;
  }));
  const reply2 = results2.at(-1) ?? {};
  const settled3 = await Promise.allSettled((request?.activation?.comments ?? []).map(async (item) => {
    const request3 = {
      context: request?.context,
      replay_clip: {
        audit: { created_at: item?.created_at },
        owner: { email: (item?.author_email ?? '') },
      },
    };
    const reply3 = await call((done) => broadcastsReplayClipWorkflowClient.startReplayClipWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, done));
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
      activation: { requester: { phone: (reply1?.sponsor?.contact?.phone ?? '') } },
    },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        subject: (request?.activation?.subject ?? ''),
      },
    },
  );
}

/**
 * Submits an activation.
 */
async function submitActivation(request) {
  validateContext(request.context);
  if (request.activation) {
    await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
  }
  await store.record(request.context.tenant_id, 'SubmitActivation', request);
  logger.info('submit_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    activation: {
      assignee: { user_id: (input1?.activation?.assignee?.user_id ?? '') },
      audit: { updated_at: input1?.activation?.audit?.updated_at },
      channel: {
        address: ((input1?.activation?.origin === 'channel' ? input1?.activation.channel : undefined)?.address ?? ''),
      },
      sla: { due_at: input1?.activation?.sla?.due_at },
    },
    context: input1?.context,
  };
  const reply1 = await call((done) => sponsorshipsActivationLifecycleClient.scheduleActivationExpiry(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    activation_id: (request?.activation_id ?? ''),
    context: request?.context,
    details: {
      notify_contact: { phone: (request?.activation?.requester?.phone ?? '') },
    },
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => sponsorshipsActivationLifecycleClient.suspendActivation(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    details: { approver: { email: (reply2?.activation?.requester?.email ?? '') } },
    sponsorship_deal: {
      audit: { updated_by: (request?.activation?.audit?.updated_by ?? '') },
      counterparty: { display_name: (request?.details?.approver?.display_name ?? '') },
    },
  };
  const copied3 = structuredClone(request3);
  const reply3 = await call((done) => sponsorshipsSponsorshipDealApprovalClient.submitSponsorshipDeal(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    sponsor: {
      audit: { created_by: (reply2?.activation?.audit?.created_by ?? '') },
      contact: { phone: (reply1?.activation?.requester?.phone ?? '') },
      relationship_manager: { display_name: (reply2?.activation?.assignee?.display_name ?? '') },
    },
  };
  const merged4 = Object.assign({}, request4, { context: request4.context });
  const reply4 = await call((done) => sponsorshipsSponsorValidationClient.scoreSponsor(merged4, metadataFor(merged4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      activation: { sla: { due_at: reply1?.activation?.sla?.due_at } },
      outcome: { status: (reply1?.outcome?.status ?? '') },
    },
    {
      result: {
        details: (reply2?.result?.details ?? {}),
        status: (reply2?.result?.status ?? ''),
      },
    },
    { result: { correlation_id: (reply3?.result?.correlation_id ?? '') } },
    { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
    {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        requester: request?.activation?.requester,
      },
    },
  );
}

module.exports = { approveActivation, commentActivationApproval, delegateActivationApproval, escalateActivation, getActivationApprovalStatus, listPendingActivations, recallActivation, rejectActivation, submitActivation };
