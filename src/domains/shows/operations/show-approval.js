'use strict';

/**
 * Routes shows through approval, with escalation and delegation. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { bus } = require('../../../platform/bus');
const {
  concessionsMenuItemApprovalClient,
  identityUserClient,
  ledgerLedgerAccountQueryClient,
  lineageLineageNodeClient,
  seatingSeatHoldApprovalClient,
  showsLineupCalculationClient,
  showsPerformanceCalculationClient,
  showsPerformanceQueryClient,
  sponsorshipsSponsorWorkflowClient,
  venuesVenuePolicyHistoryClient,
  venuesVenuePolicySyncClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred } = require('../../../platform/rpc');
const { collect, exchange } = require('../../../platform/streams');
const store = require('../store');

/**
 * Approves a show.
 */
async function approveShow(request) {
  validateContext(request.context);
  if (request.show) {
    await store.save(store.toShowRecord(request.context.tenant_id, request.show));
  }
  await store.record(request.context.tenant_id, 'ApproveShow', request);
  logger.info('approve_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.show?.audit?.created_at,
      owner_id: (request?.details?.approver?.user_id ?? ''),
      region: (request?.show?.location?.region ?? ''),
    },
  };
  const messages1 = await collect(ledgerLedgerAccountQueryClient.exportLedgerAccounts(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    performance: { audit: { updated_at: request?.show?.audit?.updated_at } },
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => showsPerformanceCalculationClient.simulatePerformance(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  const { context: context3, ...body3 } = request;
  const input3 = { ...body3, context: context3 };
  const request3 = {
    context: input3?.context,
    user: {
      primary_address: { postal_code: (input3?.show?.location?.postal_code ?? '') },
      relationship_manager: { display_name: (input3?.details?.approver?.display_name ?? '') },
    },
  };
  const reply3 = await call((done) => identityUserClient.createUser(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      show: { audit: { created_at: reply2?.performance?.audit?.created_at } },
    },
    {
      show: {
        location: { postal_code: (reply3?.user?.primary_address?.postal_code ?? '') },
        organizer: {
          display_name: (reply3?.user?.relationship_manager?.display_name ?? ''),
        },
      },
    },
    {
      show: {
        metadata: (request?.show?.metadata ?? {}),
        show_id: (request?.show_id ?? ''),
      },
    },
  );
}

/**
 * Delegates a show.
 */
async function delegateShowApproval(request) {
  validateContext(request.context);
  if (request.show) {
    await store.save(store.toShowRecord(request.context.tenant_id, request.show));
  }
  await store.record(request.context.tenant_id, 'DelegateShowApproval', request);
  logger.info('delegate_show_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.show?.audit?.updated_at,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.show?.organizer?.user_id ?? ''),
      region: (request?.show?.location?.region ?? ''),
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => ledgerLedgerAccountQueryClient.browseLedgerAccounts(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    seat_hold: {
      location: { country_code: (request?.show?.location?.country_code ?? '') },
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => seatingSeatHoldApprovalClient.approveSeatHold(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.result?.status ?? '') } },
    {
      outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) },
      show: { organizer: reply2?.seat_hold?.organizer },
    },
    {
      show: {
        show_id: (request?.show_id ?? ''),
        title: (request?.show?.title ?? ''),
      },
    },
  );
}

/**
 * Escalates a show.
 */
async function escalateShow(request) {
  validateContext(request.context);
  if (request.show) {
    await store.save(store.toShowRecord(request.context.tenant_id, request.show));
  }
  await store.record(request.context.tenant_id, 'EscalateShow', request);
  logger.info('escalate_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = [];
  for (const item of (request?.show?.participants ?? [])) {
    const request1 = {
      context: request?.context,
      sponsor: {
        contact: {
          email: (item?.contact?.email ?? ''),
          preferred_channel: (item?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await call((done) => sponsorshipsSponsorWorkflowClient.pauseSponsorWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    results1.push(reply1);
  }
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    change: {
      block: {
        notify_email: (request?.context?.actor?.email ?? ''),
        reason: (request?.reason ?? ''),
      },
      effective: request?.show?.window,
    },
    context: request?.context,
  };
  const messages2 = await exchange(venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request2.context), { deadline: deadline() }), [request2]);
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
  return {
    show: {
      organizer: request?.show?.organizer,
      show_id: (request?.show_id ?? ''),
    },
  };
}

/**
 * Returns a show.
 */
async function getShowApprovalStatus(request) {
  validateContext(request.context);
  if (!request.show_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'show_id is required');
  }
  await store.record(request.context.tenant_id, 'GetShowApprovalStatus', request);
  logger.info('get_show_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { show: { show_id: (request?.show_id ?? '') } };
}

/**
 * Lists shows a page at a time.
 */
async function listPendingShows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListPendingShows', request);
  logger.info('list_pending_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { approver: { email: (request?.context?.actor?.email ?? '') } },
    menu_item: { address: { region: (request?.filter?.region ?? '') } },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => concessionsMenuItemApprovalClient.commentMenuItemApproval(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    shows: [{
      location: {
        locality: ((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.locality ?? ''),
      },
      organizer: { email: (reply1?.menu_item?.owner?.email ?? '') },
    }],
  };
}

/**
 * Recalls a show.
 */
async function recallShow(request) {
  validateContext(request.context);
  if (request.show) {
    await store.save(store.toShowRecord(request.context.tenant_id, request.show));
  }
  await store.record(request.context.tenant_id, 'RecallShow', request);
  logger.info('recall_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  let selected1_3;
  switch (request?.show?.recurrence) {
    case 'daily':
      selected1_3 = ((request?.show?.recurrence === 'daily' ? request?.show.daily : undefined)?.interval ?? 0);
      break;
    default:
      selected1_3 = 0;
  }
  const request1 = {
    context: request?.context,
    performance: {
      daily: { interval: selected1_3 },
      location: { recipient: (request?.show?.location?.recipient ?? '') },
      participants: [{ contact: { email: (request?.show?.organizer?.email ?? '') } }],
    },
  };
  const reply1 = await call((done) => showsPerformanceCalculationClient.comparePerformanceOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { contact: { email: (request?.show?.organizer?.email ?? '') } },
    lineup: {
      audit: { updated_by: (request?.show?.audit?.updated_by ?? '') },
      billing_address: {
        country_code: (request?.show?.location?.country_code ?? ''),
        locality: (request?.show?.location?.locality ?? ''),
      },
    },
  };
  const task2 = deferred(() => call((done) => showsLineupCalculationClient.explainLineupCalculation(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
    {
      show: {
        show_id: (request?.show_id ?? ''),
        title: (request?.show?.title ?? ''),
      },
    },
  );
}

/**
 * Rejects a show.
 */
async function rejectShow(request) {
  validateContext(request.context);
  if (request.show) {
    await store.save(store.toShowRecord(request.context.tenant_id, request.show));
  }
  await store.record(request.context.tenant_id, 'RejectShow', request);
  logger.info('reject_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      labels: { ...(request?.show?.metadata ?? {}) },
      owner_id: (request?.show?.show_id ?? ''),
      region: (request?.show?.location?.region ?? ''),
    },
  };
  const reply1 = await bus.request('acme.shows.v1.ShowApprovalService/RejectShow:0', request1);
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    {
      show: {
        show_id: (request?.show_id ?? ''),
        title: (request?.show?.title ?? ''),
      },
    },
  );
}

/**
 * Submits a show.
 */
async function submitShow(request) {
  validateContext(request.context);
  if (request.show) {
    await store.save(store.toShowRecord(request.context.tenant_id, request.show));
  }
  await store.record(request.context.tenant_id, 'SubmitShow', request);
  logger.info('submit_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    lineage_node: {
      children: [{
        children: [{
          location: {
            lines: [...(request?.show?.location?.lines ?? [])],
            recipient: (request?.show?.location?.recipient ?? ''),
          },
          manager: { email: (request?.details?.approver?.email ?? '') },
        }],
      }],
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => lineageLineageNodeClient.updateLineageNode(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const results2 = await Promise.all((request?.show?.participants ?? []).map(async (item) => {
    const request2 = {
      context: request?.context,
      details: { author_email: (item?.contact?.email ?? '') },
      venue_policy: {
        owner: { display_name: (request?.show?.organizer?.display_name ?? '') },
      },
    };
    const reply2 = await call((done) => venuesVenuePolicyHistoryClient.annotateVenuePolicyVersion(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return reply2;
  }));
  const reply2 = results2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      show: {
        audit: { updated_by: (reply1?.lineage_node?.audit?.updated_by ?? '') },
      },
    },
    {
      show: {
        metadata: (request?.show?.metadata ?? {}),
        show_id: (request?.show_id ?? ''),
      },
    },
  );
}

module.exports = { approveShow, delegateShowApproval, escalateShow, getShowApprovalStatus, listPendingShows, recallShow, rejectShow, submitShow };
