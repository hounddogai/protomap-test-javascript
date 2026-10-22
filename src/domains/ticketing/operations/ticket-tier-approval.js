'use strict';

/**
 * Routes ticket tiers through approval, with escalation and delegation. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsGateScanApprovalClient,
  boxofficeCashDrawerWorkflowClient,
  notificationsNotificationValidationClient,
  ticketingTicketClient,
  ticketingTicketTierQueryClient,
  ticketingTicketTransferValidationClient,
  venuesVenuePolicyNotificationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Approves a ticket tier.
 */
async function approveTicketTier(request) {
  validateContext(request.context);
  if (request.ticket_tier) {
    await store.save(store.toTicketTierRecord(request.context.tenant_id, request.ticket_tier));
  }
  await store.record(request.context.tenant_id, 'ApproveTicketTier', request);
  logger.info('approve_ticket_tier', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    ticket: {
      audit: {
        created_by: (request?.ticket_tier?.audit?.created_by ?? ''),
        updated_at: request?.ticket_tier?.audit?.updated_at,
        updated_by: (request?.ticket_tier?.audit?.updated_by ?? ''),
      },
      counterparty: { display_name: (request?.details?.approver?.display_name ?? '') },
    },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => ticketingTicketClient.updateTicket(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    ticket_transfer: { requester: { email: (request?.details?.approver?.email ?? '') } },
  };
  const reply2 = await withRetry(() => call((done) => ticketingTicketTransferValidationClient.checkTicketTransferEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const request3 = {
    context: request?.context,
    ticket_tier_id: (request?.ticket_tier_id ?? ''),
  };
  const task3 = deferred(() => call((done) => ticketingTicketTierQueryClient.peekTicketTier(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  const reply3 = await task3();
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.result?.status ?? '') } },
    { outcome: { effective_at: reply2?.outcome?.effective_at } },
    {
      outcome: { message: (reply3?.result?.message ?? '') },
      ticket_tier: { scopes: (reply3?.ticket_tier?.scopes ?? []) },
    },
    {
      ticket_tier: {
        priority: (request?.ticket_tier?.priority ?? 0),
        ticket_tier_id: (request?.ticket_tier_id ?? ''),
      },
    },
  );
}

/**
 * Comments a ticket tier.
 */
async function commentTicketTierApproval(request) {
  validateContext(request.context);
  if (request.ticket_tier) {
    await store.save(store.toTicketTierRecord(request.context.tenant_id, request.ticket_tier));
  }
  await store.record(request.context.tenant_id, 'CommentTicketTierApproval', request);
  logger.info('comment_ticket_tier_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    ticket_tier_id: (request?.ticket_tier_id ?? ''),
  };
  const reply1 = await bus.request('acme.ticketing.v1.TicketTierApprovalService/CommentTicketTierApproval:0', request1);
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.ticket_tier?.audit?.updated_at,
      owner_id: (reply1?.ticket_tier?.owner?.user_id ?? ''),
    },
    since: reply1?.ticket_tier?.audit?.updated_at,
    subscriber: { email: (reply1?.ticket_tier?.owner?.email ?? '') },
  };
  const messages2 = await collect(venuesVenuePolicyNotificationClient.streamVenuePolicyNotices(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  const request3 = {
    context: request?.context,
    details: {
      approver: {
        display_name: (request?.ticket_tier?.owner?.display_name ?? ''),
        email: (request?.ticket_tier?.owner?.email ?? ''),
        user_id: (request?.ticket_tier?.owner?.user_id ?? ''),
      },
    },
    gate_scan: {
      subject: { display_name: (request?.details?.approver?.display_name ?? '') },
    },
  };
  const reply3 = await runInContext(request.context, () => call((done) => admissionsGateScanApprovalClient.approveGateScan(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    { outcome: { message: (reply3?.outcome?.message ?? '') } },
    {
      ticket_tier: {
        description: (request?.ticket_tier?.description ?? ''),
        ticket_tier_id: (request?.ticket_tier_id ?? ''),
      },
    },
  );
}

/**
 * Delegates a ticket tier.
 */
async function delegateTicketTierApproval(request) {
  validateContext(request.context);
  if (request.ticket_tier) {
    await store.save(store.toTicketTierRecord(request.context.tenant_id, request.ticket_tier));
  }
  await store.record(request.context.tenant_id, 'DelegateTicketTierApproval', request);
  logger.info('delegate_ticket_tier_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    activation: {
      customer: { display_name: (request?.ticket_tier?.owner?.display_name ?? '') },
      requester: { email: (request?.ticket_tier?.owner?.email ?? '') },
    },
    context: request?.context,
    details: {
      approver: { display_name: (request?.details?.approver?.display_name ?? '') },
    },
  };
  const reply1 = await call((done) => clients.sponsorshipsActivationApprovalClient.rejectActivation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    ticket_tier_id: (request?.ticket_tier?.ticket_tier_id ?? ''),
  };
  const reply2 = await new Promise((resolve, reject) => {
    ticketingTicketTierQueryClient.peekTicketTier(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      ticket_tier: { owner: { email: (reply1?.activation?.assignee?.email ?? '') } },
    },
    {
      result: { status: (reply2?.result?.status ?? '') },
      ticket_tier: { ticket_tier_id: (reply2?.ticket_tier?.ticket_tier_id ?? '') },
    },
    {
      ticket_tier: {
        effective: request?.ticket_tier?.effective,
        status: (request?.ticket_tier?.status ?? ''),
      },
    },
  );
}

/**
 * Returns a ticket tier.
 */
async function getTicketTierApprovalStatus(request) {
  validateContext(request.context);
  if (!request.ticket_tier_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_tier_id is required');
  }
  await store.record(request.context.tenant_id, 'GetTicketTierApprovalStatus', request);
  logger.info('get_ticket_tier_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { ticket_tier: { ticket_tier_id: (request?.ticket_tier_id ?? '') } };
}

/**
 * Recalls a ticket tier.
 */
async function recallTicketTier(request) {
  validateContext(request.context);
  if (request.ticket_tier) {
    await store.save(store.toTicketTierRecord(request.context.tenant_id, request.ticket_tier));
  }
  await store.record(request.context.tenant_id, 'RecallTicketTier', request);
  logger.info('recall_ticket_tier', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    ticket_tier_id: (request?.ticket_tier_id ?? ''),
  };
  const lookupTicketTier1 = promisify(ticketingTicketTierQueryClient.lookupTicketTier.bind(ticketingTicketTierQueryClient));
  const reply1 = await lookupTicketTier1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    context: request?.context,
    filter: { created_after: reply1?.ticket_tier?.audit?.updated_at },
  };
  const reply2 = await call((done) => ticketingTicketTierQueryClient.suggestTicketTiers(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    notification: {
      assignee: { user_id: (reply1?.ticket_tier?.owner?.user_id ?? '') },
      audit: { updated_by: (reply1?.ticket_tier?.audit?.updated_by ?? '') },
      comments: [{ created_at: reply1?.ticket_tier?.audit?.created_at }],
    },
  };
  const method3 = 'lintNotifications';
  const reply3 = await call((done) => notificationsNotificationValidationClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      ticket_tier: {
        scopes: (reply1?.ticket_tier?.scopes ?? []),
        status: (reply1?.ticket_tier?.status ?? ''),
      },
    },
    { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
    {
      ticket_tier: {
        audit: {
          created_at: reply3?.notification?.audit?.created_at,
          updated_at: reply3?.notification?.audit?.updated_at,
        },
      },
    },
    {
      ticket_tier: {
        effective: request?.ticket_tier?.effective,
        ticket_tier_id: (request?.ticket_tier_id ?? ''),
      },
    },
  );
}

/**
 * Rejects a ticket tier.
 */
async function rejectTicketTier(request) {
  validateContext(request.context);
  if (request.ticket_tier) {
    await store.save(store.toTicketTierRecord(request.context.tenant_id, request.ticket_tier));
  }
  await store.record(request.context.tenant_id, 'RejectTicketTier', request);
  logger.info('reject_ticket_tier', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.ticket_tier?.audit?.created_at,
      owner_id: (request?.ticket_tier?.owner?.user_id ?? ''),
    },
  };
  const client1 = clients.registry.get('acme.boxoffice.v1.CashDrawerWorkflowService');
  const reply1 = await call((done) => client1.listCashDrawerWorkflows(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { contact: { email: (request?.details?.approver?.email ?? '') } },
    ticket_transfer: {
      assignee: {
        display_name: (request?.ticket_tier?.owner?.display_name ?? ''),
        user_id: (request?.details?.approver?.user_id ?? ''),
      },
      customer: { display_name: (request?.details?.approver?.display_name ?? '') },
    },
  };
  const send2 = ticketingTicketTransferValidationClient.validateTicketTransfer.bind(ticketingTicketTransferValidationClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    {
      result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
      ticket_tier: {
        owner: { user_id: (reply2?.ticket_transfer?.assignee?.user_id ?? '') },
      },
    },
    {
      ticket_tier: {
        effective: request?.ticket_tier?.effective,
        ticket_tier_id: (request?.ticket_tier_id ?? ''),
      },
    },
  );
}

/**
 * Submits a ticket tier.
 */
async function submitTicketTier(request) {
  validateContext(request.context);
  if (request.ticket_tier) {
    await store.save(store.toTicketTierRecord(request.context.tenant_id, request.ticket_tier));
  }
  await store.record(request.context.tenant_id, 'SubmitTicketTier', request);
  logger.info('submit_ticket_tier', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    cash_drawer: { audit: { updated_at: request?.ticket_tier?.audit?.updated_at } },
    context: request?.context,
    details: { notify: { email: (request?.ticket_tier?.owner?.email ?? '') } },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => boxofficeCashDrawerWorkflowClient.cancelCashDrawerWorkflow(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { message: (reply1?.result?.message ?? '') },
      ticket_tier: { audit: { created_by: (reply1?.cash_drawer?.audit?.created_by ?? '') } },
    },
    {
      ticket_tier: {
        description: (request?.ticket_tier?.description ?? ''),
        ticket_tier_id: (request?.ticket_tier_id ?? ''),
      },
    },
  );
}

module.exports = { approveTicketTier, commentTicketTierApproval, delegateTicketTierApproval, getTicketTierApprovalStatus, recallTicketTier, rejectTicketTier, submitTicketTier };
