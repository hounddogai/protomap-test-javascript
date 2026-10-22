'use strict';

/**
 * Routes riders through approval, with escalation and delegation. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  boxofficeBoxOfficeSaleClient,
  documentsDocumentQueryClient,
  performersPerformerApprovalClient,
  performersPerformerContractApprovalClient,
  performersRiderValidationClient,
  searchSearchIndexQueryClient,
  toursTourStopApprovalClient,
  venuesVenueHistoryClient,
  venuesVenuePolicySyncClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect, upload } = require('../../../platform/streams');
const store = require('../store');

/**
 * Approves a rider.
 */
async function approveRider(request) {
  validateContext(request.context);
  if (request.rider) {
    await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
  }
  await store.record(request.context.tenant_id, 'ApproveRider', request);
  logger.info('approve_rider', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { owner_id: (request?.rider?.owner?.user_id ?? '') },
  };
  const messages1 = await collect(documentsDocumentQueryClient.exportDocuments(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: {
      filters: { ...(request?.rider?.parameters ?? {}) },
      period: request?.rider?.effective,
    },
    reason: (request?.reason ?? ''),
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => searchSearchIndexQueryClient.summarizeSearchIndexes(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
  return assemble(
    { outcome: { status: (reply2?.result?.status ?? '') } },
    {
      rider: {
        rider_id: (request?.rider_id ?? ''),
        version: (request?.rider?.version ?? '0'),
      },
    },
  );
}

/**
 * Comments a rider.
 */
async function commentRiderApproval(request) {
  validateContext(request.context);
  if (request.rider) {
    await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
  }
  await store.record(request.context.tenant_id, 'CommentRiderApproval', request);
  logger.info('comment_rider_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    venue_policy: {
      audit: {
        created_at: input1?.rider?.audit?.created_at,
        updated_at: input1?.rider?.audit?.updated_at,
        updated_by: (input1?.rider?.audit?.updated_by ?? ''),
      },
      route: {
        weight: ((input1?.rider?.action === 'route' ? input1?.rider.route : undefined)?.weight ?? 0),
      },
    },
  };
  const reply1 = await call((done) => venuesVenuePolicySyncClient.reconcileVenuePolicies(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    header: {
      source: (request?.context?.actor?.user_id ?? ''),
      submitted_by: { email: (request?.context?.actor?.email ?? '') },
    },
  };
  const reply2 = await upload(
    (done) => boxofficeBoxOfficeSaleClient.batchCreateBoxOfficeSales(metadataFor(request2.context), { deadline: deadline() }, done),
    [request2],
  );
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { effective_at: reply1?.outcome?.effective_at },
      rider: { priority: (reply1?.venue_policy?.priority ?? 0) },
    },
    {
      rider: {
        owner: request?.rider?.owner,
        rider_id: (request?.rider_id ?? ''),
      },
    },
  );
}

/**
 * Delegates a rider.
 */
async function delegateRiderApproval(request) {
  validateContext(request.context);
  if (request.rider) {
    await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
  }
  await store.record(request.context.tenant_id, 'DelegateRiderApproval', request);
  logger.info('delegate_rider_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    performer: { contact: { email: (request?.details?.approver?.email ?? '') } },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => performersPerformerApprovalClient.submitPerformer(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    venue: {
      address: { recipient: (reply1?.performer?.primary_address?.recipient ?? '') },
      audit: {
        created_at: reply1?.performer?.audit?.created_at,
        updated_by: (reply1?.performer?.audit?.updated_by ?? ''),
      },
      owner: { user_id: (request?.rider?.owner?.user_id ?? '') },
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => venuesVenueHistoryClient.recordVenueVersion(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { effective_at: reply1?.outcome?.effective_at },
      rider: { audit: { updated_at: reply1?.performer?.audit?.updated_at } },
    },
    { outcome: { status: (reply2?.result?.status ?? '') } },
    {
      rider: {
        priority: (request?.rider?.priority ?? 0),
        rider_id: (request?.rider_id ?? ''),
      },
    },
  );
}

/**
 * Escalates a rider.
 */
async function escalateRider(request) {
  validateContext(request.context);
  if (request.rider) {
    await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
  }
  await store.record(request.context.tenant_id, 'EscalateRider', request);
  logger.info('escalate_rider', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = await Promise.all((request?.rider?.conditions ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      rider: {
        conditions: [{
          attribute: (item?.attribute ?? ''),
          negate: (item?.negate ?? false),
          operator: (item?.operator ?? ''),
        }],
      },
    };
    const reply1 = await call((done) => performersRiderValidationClient.previewRider(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return reply1;
  }));
  const reply1 = results1.at(-1) ?? {};
  const settled2 = await Promise.allSettled((request?.rider?.conditions ?? []).map(async (item) => {
    const request2 = {
      context: request?.context,
      rider: { conditions: [{ value: (item?.value ?? '') }] },
    };
    const reply2 = await call((done) => performersRiderValidationClient.lintRiders(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return reply2;
  }));
  const results2 = settled2.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply2 = results2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    rider: {
      description: (request?.rider?.description ?? ''),
      rider_id: (request?.rider_id ?? ''),
    },
  };
}

/**
 * Returns a rider.
 */
async function getRiderApprovalStatus(request) {
  validateContext(request.context);
  if (!request.rider_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'rider_id is required');
  }
  await store.record(request.context.tenant_id, 'GetRiderApprovalStatus', request);
  logger.info('get_rider_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { locale: (request?.context?.locale ?? '') },
    rider: {
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
      rider_id: (request?.rider_id ?? ''),
    },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => performersRiderValidationClient.normalizeRider(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  return { rider: { rider_id: (reply1?.rider?.rider_id ?? '') } };
}

/**
 * Lists riders a page at a time.
 */
async function listPendingRiders(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListPendingRiders', request);
  logger.info('list_pending_riders', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      approver: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
    performer_contract: { billing_address: { region: (request?.filter?.region ?? '') } },
  };
  const reply1 = await withRetry(() => call((done) => performersPerformerContractApprovalClient.approvePerformerContract(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    result: { status: (reply1?.result?.status ?? '') },
    riders: [{ audit: { created_at: reply1?.performer_contract?.audit?.created_at } }],
  };
}

/**
 * Recalls a rider.
 */
async function recallRider(request) {
  validateContext(request.context);
  if (request.rider) {
    await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
  }
  await store.record(request.context.tenant_id, 'RecallRider', request);
  logger.info('recall_rider', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { approver: request?.details?.approver },
    tour_stop: {
      audit: { updated_by: (request?.rider?.audit?.updated_by ?? '') },
      organizer: { user_id: (request?.rider?.owner?.user_id ?? '') },
      participants: [{ contact: { email: (request?.rider?.owner?.email ?? '') } }],
    },
  };
  const task1 = deferred(() => call((done) => toursTourStopApprovalClient.recallTourStop(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  const request2 = {
    context: request?.context,
    rider: {
      discount: {
        amount: (request?.rider?.action === 'discount' ? request?.rider.discount : undefined)?.amount,
      },
      owner: { display_name: (request?.rider?.owner?.display_name ?? '') },
    },
  };
  const reply2 = await bus.request('acme.performers.v1.RiderApprovalService/RecallRider:1', request2);
  const request3 = {
    context: request?.context,
    tour_stop: {
      participants: [{ contact: { email: (request?.details?.approver?.email ?? '') } }],
    },
  };
  const reply3 = await runInContext(request.context, () => call((done) => toursTourStopApprovalClient.approveTourStop(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.outcome?.status ?? '') } },
    { result: { status: (reply2?.outcome?.status ?? '') } },
    {
      rider: {
        audit: { created_by: (reply3?.tour_stop?.audit?.created_by ?? '') },
        owner: { email: (reply3?.tour_stop?.organizer?.email ?? '') },
      },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      rider: { rider_id: (request?.rider_id ?? '') },
    },
  );
}

/**
 * Submits a rider.
 */
async function submitRider(request) {
  validateContext(request.context);
  if (request.rider) {
    await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
  }
  await store.record(request.context.tenant_id, 'SubmitRider', request);
  logger.info('submit_rider', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { approver: { user_id: (request?.rider?.owner?.user_id ?? '') } },
    tour_stop: {
      audit: { created_by: (request?.rider?.audit?.created_by ?? '') },
      organizer: { user_id: (request?.details?.approver?.user_id ?? '') },
      participants: [{ contact: { email: (request?.rider?.owner?.email ?? '') } }],
    },
  };
  const reply1 = await call((done) => clients.toursTourStopApprovalClient.submitTourStop(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { approver: { email: (request?.details?.approver?.email ?? '') } },
    performer_contract: { audit: { updated_by: (request?.rider?.audit?.updated_by ?? '') } },
  };
  const reply2 = await new Promise((resolve, reject) => {
    performersPerformerContractApprovalClient.commentPerformerContractApproval(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') } },
    {
      outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) },
      result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
    },
    {
      rider: {
        rider_id: (request?.rider_id ?? ''),
        version: (request?.rider?.version ?? '0'),
      },
    },
  );
}

module.exports = { approveRider, commentRiderApproval, delegateRiderApproval, escalateRider, getRiderApprovalStatus, listPendingRiders, recallRider, submitRider };
