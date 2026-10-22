'use strict';

/**
 * Routes ticket transfers through approval, with escalation and delegation. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  broadcastsReplayClipWorkflowClient,
  concessionsConcessionStandApprovalClient,
  ticketingTicketClient,
  ticketingTicketQueryClient,
  ticketingTicketTierQueryClient,
  ticketingTicketWorkflowClient,
  venuesVenuePolicyHistoryClient,
  venuesVenuePolicyValidationClient,
  venuesVenueSectionValidationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Approves a ticket transfer.
 */
async function approveTicketTransfer(request) {
  validateContext(request.context);
  if (request.ticket_transfer) {
    await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
  }
  await store.record(request.context.tenant_id, 'ApproveTicketTransfer', request);
  logger.info('approve_ticket_transfer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.ticket_transfer?.sla?.due_at,
      owner_id: (request?.ticket_transfer?.assignee?.user_id ?? ''),
    },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => ticketingTicketClient.listTickets(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
    },
    {
      ticket_transfer: {
        status: (request?.ticket_transfer?.status ?? ''),
        ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
      },
    },
  );
}

/**
 * Comments a ticket transfer.
 */
async function commentTicketTransferApproval(request) {
  validateContext(request.context);
  if (request.ticket_transfer) {
    await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
  }
  await store.record(request.context.tenant_id, 'CommentTicketTransferApproval', request);
  logger.info('comment_ticket_transfer_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = await Promise.all((request?.ticket_transfer?.history ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: {
          created_at: item?.changed_at,
          etag: (request?.ticket_transfer?.audit?.etag ?? ''),
        },
        owner: {
          email: (request?.details?.approver?.email ?? ''),
          user_id: (request?.ticket_transfer?.assignee?.user_id ?? ''),
        },
      },
    };
    const reply1 = await call((done) => venuesVenuePolicyHistoryClient.compareVenuePolicyVersions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return reply1;
  }));
  const reply1 = results1.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    ticket_transfer: {
      custom_fields: (request?.ticket_transfer?.custom_fields ?? {}),
      ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
    },
  };
}

/**
 * Delegates a ticket transfer.
 */
async function delegateTicketTransferApproval(request) {
  validateContext(request.context);
  if (request.ticket_transfer) {
    await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
  }
  await store.record(request.context.tenant_id, 'DelegateTicketTransferApproval', request);
  logger.info('delegate_ticket_transfer_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    ticket: {
      counterparty: {
        display_name: (request?.ticket_transfer?.assignee?.display_name ?? ''),
      },
    },
  };
  const reply1 = await withRetry(() => call((done) => ticketingTicketClient.updateTicket(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.result?.status ?? '') } },
    {
      ticket_transfer: {
        status: (request?.ticket_transfer?.status ?? ''),
        ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
      },
    },
  );
}

/**
 * Escalates a ticket transfer.
 */
async function escalateTicketTransfer(request) {
  validateContext(request.context);
  if (request.ticket_transfer) {
    await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
  }
  await store.record(request.context.tenant_id, 'EscalateTicketTransfer', request);
  logger.info('escalate_ticket_transfer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const settled1 = await Promise.allSettled((request?.ticket_transfer?.comments ?? []).map(async (item) => {
    const request1 = {
      concession_stand: { audit: { created_at: item?.created_at } },
      context: request?.context,
    };
    const reply1 = await call((done) => concessionsConcessionStandApprovalClient.recallConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done));
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
      created_after: reply1?.concession_stand?.audit?.updated_at,
      region: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.region ?? ''),
    },
  };
  const task2 = deferred(() => call((done) => broadcastsReplayClipWorkflowClient.listReplayClipWorkflows(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        message: (reply2?.result?.message ?? ''),
        reference_ids: (reply2?.result?.reference_ids ?? []),
      },
    },
    {
      ticket_transfer: {
        status: (request?.ticket_transfer?.status ?? ''),
        ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
      },
    },
  );
}

/**
 * Returns a ticket transfer.
 */
async function getTicketTransferApprovalStatus(request) {
  validateContext(request.context);
  if (!request.ticket_transfer_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_transfer_id is required');
  }
  await store.record(request.context.tenant_id, 'GetTicketTransferApprovalStatus', request);
  logger.info('get_ticket_transfer_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_policy: { owner: { user_id: (request?.context?.actor?.user_id ?? '') } },
  };
  const reply1 = await bus.request('acme.ticketing.v1.TicketTransferApprovalService/GetTicketTransferApprovalStatus:0', request1);
  return assemble(
    {
      result: { message: (reply1?.result?.message ?? '') },
      ticket_transfer: {
        assignee: { display_name: (reply1?.venue_policy?.owner?.display_name ?? '') },
      },
    },
    {
      ticket_transfer: { ticket_transfer_id: (request?.ticket_transfer_id ?? '') },
    },
  );
}

/**
 * Lists ticket transfers a page at a time.
 */
async function listPendingTicketTransfers(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListPendingTicketTransfers', request);
  logger.info('list_pending_ticket_transfers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_section: {
      children: [{
        audit: { updated_at: request?.filter?.created_after },
        children: [{ location: { region: (request?.filter?.region ?? '') } }],
        manager: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      }],
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => venuesVenueSectionValidationClient.scoreVenueSection(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    details: { contact: { email: (reply1?.venue_section?.manager?.email ?? '') } },
  };
  const reply2 = await call((done) => clients.ticketingTicketTransferValidationClient.checkTicketTransferEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return assemble(
    {
      ticket_transfers: [{
        assignee: { display_name: (reply1?.venue_section?.manager?.display_name ?? '') },
      }],
    },
    {
      ticket_transfers: [{
        assignee: { email: (reply2?.ticket_transfer?.requester?.email ?? '') },
        status: (reply2?.ticket_transfer?.status ?? ''),
      }],
    },
  );
}

/**
 * Recalls a ticket transfer.
 */
async function recallTicketTransfer(request) {
  validateContext(request.context);
  if (request.ticket_transfer) {
    await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
  }
  await store.record(request.context.tenant_id, 'RecallTicketTransfer', request);
  logger.info('recall_ticket_transfer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  let selected1_2;
  switch (request?.ticket_transfer?.origin) {
    case 'customer':
      selected1_2 = ((request?.ticket_transfer?.origin === 'customer' ? request?.ticket_transfer.customer : undefined)?.ticket_tier_id ?? '');
      break;
    default:
      selected1_2 = '';
  }
  const request1 = {
    context: request?.context,
    reason: (request?.reason ?? ''),
    ticket_tier_id: selected1_2,
  };
  const reply1 = await call((done) => ticketingTicketTierQueryClient.summarizeTicketTiers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    ticket: { audit: { created_at: request?.ticket_transfer?.audit?.created_at } },
  };
  const startTicketWorkflow2 = promisify(ticketingTicketWorkflowClient.startTicketWorkflow.bind(ticketingTicketWorkflowClient));
  const reply2 = await startTicketWorkflow2(request2, metadataFor(request2.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    {
      result: { status: (reply2?.outcome?.status ?? '') },
      ticket_transfer: { audit: { etag: (reply2?.ticket?.audit?.etag ?? '') } },
    },
    {
      ticket_transfer: {
        priority: (request?.ticket_transfer?.priority ?? 0),
        ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
      },
    },
  );
}

/**
 * Rejects a ticket transfer.
 */
async function rejectTicketTransfer(request) {
  validateContext(request.context);
  if (request.ticket_transfer) {
    await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
  }
  await store.record(request.context.tenant_id, 'RejectTicketTransfer', request);
  logger.info('reject_ticket_transfer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const reply1 = await call((done) => ticketingTicketTierQueryClient.resolveTicketTiers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const results2 = [];
  for (const item of (request?.ticket_transfer?.comments ?? [])) {
    const request2 = {
      context: request?.context,
      ticket: { audit: { created_at: item?.created_at } },
    };
    const reply2 = await call((done) => ticketingTicketWorkflowClient.cancelTicketWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    results2.push(reply2);
  }
  const reply2 = results2.at(-1) ?? {};
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.ticket_transfer?.sla?.due_at,
      owner_id: (request?.ticket_transfer?.sla?.policy_id ?? ''),
      region: (reply2?.ticket?.billing_address?.region ?? ''),
    },
  };
  const method3 = 'suggestTicketTiers';
  const reply3 = await call((done) => ticketingTicketTierQueryClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    venue_policy: {
      audit: { created_at: reply2?.ticket?.audit?.created_at },
      owner: {
        display_name: ((request?.ticket_transfer?.origin === 'customer' ? request?.ticket_transfer.customer : undefined)?.display_name ?? ''),
      },
    },
  };
  const client4 = clients.registry.get('acme.venues.v1.VenuePolicyValidationService');
  const reply4 = await call((done) => client4.normalizeVenuePolicy(request4, metadataFor(request4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.result?.status ?? '') } },
    { result: { reference_ids: (reply3?.result?.reference_ids ?? []) } },
    {
      result: { message: (reply4?.result?.message ?? '') },
      ticket_transfer: { comments: [{ created_at: reply4?.venue_policy?.audit?.created_at }] },
    },
    {
      ticket_transfer: {
        priority: (request?.ticket_transfer?.priority ?? 0),
        ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
      },
    },
  );
}

/**
 * Submits a ticket transfer.
 */
async function submitTicketTransfer(request) {
  validateContext(request.context);
  if (request.ticket_transfer) {
    await store.save(store.toTicketTransferRecord(request.context.tenant_id, request.ticket_transfer));
  }
  await store.record(request.context.tenant_id, 'SubmitTicketTransfer', request);
  logger.info('submit_ticket_transfer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { created_after: request?.ticket_transfer?.audit?.created_at },
  };
  const send1 = ticketingTicketQueryClient.suggestTickets.bind(ticketingTicketQueryClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    {
      ticket_transfer: {
        assignee: request?.ticket_transfer?.assignee,
        ticket_transfer_id: (request?.ticket_transfer_id ?? ''),
      },
    },
  );
}

module.exports = { approveTicketTransfer, commentTicketTransferApproval, delegateTicketTransferApproval, escalateTicketTransfer, getTicketTransferApprovalStatus, listPendingTicketTransfers, recallTicketTransfer, rejectTicketTransfer, submitTicketTransfer };
