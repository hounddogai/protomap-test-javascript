'use strict';

/**
 * Routes performer contracts through approval, with escalation and delegation. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  auditAuditEventValidationClient,
  broadcastsReplayClipWorkflowClient,
  concessionsConcessionOrderReportingClient,
  helpdeskTicketCalculationClient,
  notificationsNotificationValidationClient,
  performersPerformerClient,
  performersPerformerContractQueryClient,
  performersRiderValidationClient,
  sponsorshipsSponsorWorkflowClient,
  surveysSurveyQueryClient,
  venuesVenueHistoryClient,
  venuesVenuePolicyAdminClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Approves a performer contract.
 */
async function approvePerformerContract(request) {
  validateContext(request.context);
  if (request.performer_contract) {
    await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
  }
  await store.record(request.context.tenant_id, 'ApprovePerformerContract', request);
  logger.info('approve_performer_contract', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const reply1 = await withRetry(() => call((done) => performersPerformerContractQueryClient.resolvePerformerContracts(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    filter: { region: (request?.performer_contract?.billing_address?.region ?? '') },
  };
  const task2 = deferred(() => call((done) => surveysSurveyQueryClient.countSurveys(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.performer_contract?.audit?.updated_at,
      region: (request?.performer_contract?.billing_address?.region ?? ''),
    },
  };
  const reply3 = await bus.request('acme.performers.v1.PerformerContractApprovalService/ApprovePerformerContract:2', request3);
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    {
      result: {
        correlation_id: (reply3?.result?.correlation_id ?? ''),
        reference_ids: (reply3?.result?.reference_ids ?? []),
      },
    },
    {
      performer_contract: {
        notes: (request?.performer_contract?.notes ?? ''),
        performer_contract_id: (request?.performer_contract_id ?? ''),
      },
    },
  );
}

/**
 * Comments a performer contract.
 */
async function commentPerformerContractApproval(request) {
  validateContext(request.context);
  if (request.performer_contract) {
    await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
  }
  await store.record(request.context.tenant_id, 'CommentPerformerContractApproval', request);
  logger.info('comment_performer_contract_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { region: (request?.performer_contract?.billing_address?.region ?? '') },
  };
  const reply1 = await runInContext(request.context, () => call((done) => performersPerformerClient.listPerformers(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    rider: {
      owner: {
        display_name: (request?.details?.approver?.display_name ?? ''),
        email: (request?.details?.approver?.email ?? ''),
      },
    },
  };
  const reply2 = await call((done) => clients.performersRiderValidationClient.normalizeRider(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    notification: {
      assignee: { email: (reply2?.rider?.owner?.email ?? '') },
      audit: {
        created_by: (reply2?.rider?.audit?.created_by ?? ''),
        updated_by: (request?.performer_contract?.audit?.updated_by ?? ''),
      },
      comments: [{ created_at: reply2?.rider?.audit?.created_at }],
    },
  };
  const reply3 = await new Promise((resolve, reject) => {
    notificationsNotificationValidationClient.checkNotificationEligibility(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  let selected4_1;
  switch (reply2?.rider?.action) {
    case 'block':
      selected4_1 = ((reply2?.rider?.action === 'block' ? reply2?.rider.block : undefined)?.reason ?? '');
      break;
    default:
      selected4_1 = '';
  }
  const request4 = {
    context: request?.context,
    performer_contract_id: (request?.performer_contract?.performer_contract_id ?? ''),
    reason: selected4_1,
  };
  const reply4 = await call((done) => performersPerformerContractQueryClient.summarizePerformerContracts(request4, metadataFor(request4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      result: { message: (reply1?.result?.message ?? '') },
    },
    { outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) } },
    { outcome: { status: (reply3?.outcome?.status ?? '') } },
    {
      outcome: { status: (reply4?.result?.status ?? '') },
      result: { details: (reply4?.result?.details ?? {}) },
    },
    {
      performer_contract: {
        issued_at: request?.performer_contract?.issued_at,
        performer_contract_id: (request?.performer_contract_id ?? ''),
      },
    },
  );
}

/**
 * Escalates a performer contract.
 */
async function escalatePerformerContract(request) {
  validateContext(request.context);
  if (request.performer_contract) {
    await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
  }
  await store.record(request.context.tenant_id, 'EscalatePerformerContract', request);
  logger.info('escalate_performer_contract', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    audit_event: {
      subject: {
        display_name: (request?.performer_contract?.counterparty?.display_name ?? ''),
        number: (request?.performer_contract?.number ?? ''),
      },
    },
    context: request?.context,
  };
  const reply1 = await call((done) => auditAuditEventValidationClient.normalizeAuditEvent(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: reply1?.outcome?.effective_at,
      owner_id: (request?.performer_contract?.performer_contract_id ?? ''),
      region: (request?.performer_contract?.billing_address?.region ?? ''),
    },
  };
  const method2 = 'countConcessionOrderTotals';
  const reply2 = await call((done) => concessionsConcessionOrderReportingClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    details: { contact: { email: (request?.details?.approver?.email ?? '') } },
    rider: {
      audit: { updated_by: (request?.performer_contract?.audit?.updated_by ?? '') },
      block: { reason: (request?.reason ?? '') },
      owner: { display_name: (request?.details?.approver?.display_name ?? '') },
    },
  };
  const client3 = clients.registry.get('acme.performers.v1.RiderValidationService');
  const reply3 = await call((done) => client3.previewRider(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = { context: request?.context };
  const send4 = surveysSurveyQueryClient.resolveSurveys.bind(surveysSurveyQueryClient);
  const reply4 = await call((done) => send4(request4, metadataFor(request4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.approver?.user_id ?? '') });
  return assemble(
    {
      result: {
        message: (reply1?.result?.message ?? ''),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    },
    {
      outcome: { status: (reply2?.result?.status ?? '') },
      result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
    },
    { result: { reference_ids: (reply3?.result?.reference_ids ?? []) } },
    {
      outcome: { status: (reply4?.result?.status ?? '') },
      result: { details: (reply4?.result?.details ?? {}) },
    },
    {
      performer_contract: {
        metadata: (request?.performer_contract?.metadata ?? {}),
        performer_contract_id: (request?.performer_contract_id ?? ''),
      },
    },
  );
}

/**
 * Lists performer contracts a page at a time.
 */
async function listPendingPerformerContracts(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListPendingPerformerContracts', request);
  logger.info('list_pending_performer_contracts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Recalls a performer contract.
 */
async function recallPerformerContract(request) {
  validateContext(request.context);
  if (request.performer_contract) {
    await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
  }
  await store.record(request.context.tenant_id, 'RecallPerformerContract', request);
  logger.info('recall_performer_contract', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: {
      audit: { updated_at: request?.performer_contract?.audit?.updated_at },
      national_id: { number: (request?.performer_contract?.number ?? '') },
      relationship_manager: {
        display_name: (request?.performer_contract?.counterparty?.display_name ?? ''),
      },
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => sponsorshipsSponsorWorkflowClient.startSponsorWorkflow(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    context: input2?.context,
    replay_clip: {
      address: {
        postal_code: (input2?.performer_contract?.billing_address?.postal_code ?? ''),
      },
      owner: { email: (reply1?.sponsor?.contact?.email ?? '') },
    },
  };
  const reply2 = await call((done) => broadcastsReplayClipWorkflowClient.startReplayClipWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.outcome?.status ?? '') } },
    {
      performer_contract: {
        counterparty: { display_name: (reply2?.replay_clip?.owner?.display_name ?? '') },
      },
      result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
    },
    {
      performer_contract: {
        billing_address: request?.performer_contract?.billing_address,
        performer_contract_id: (request?.performer_contract_id ?? ''),
      },
    },
  );
}

/**
 * Rejects a performer contract.
 */
async function rejectPerformerContract(request) {
  validateContext(request.context);
  if (request.performer_contract) {
    await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
  }
  await store.record(request.context.tenant_id, 'RejectPerformerContract', request);
  logger.info('reject_performer_contract', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_id: (request?.performer_contract?.counterparty?.venue_id ?? ''),
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => venuesVenueHistoryClient.getVenueVersion(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.result?.status ?? '') } },
    {
      outcome: { effective_at: request?.details?.effective_at },
      performer_contract: { performer_contract_id: (request?.performer_contract_id ?? '') },
    },
  );
}

/**
 * Submits a performer contract.
 */
async function submitPerformerContract(request) {
  validateContext(request.context);
  if (request.performer_contract) {
    await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
  }
  await store.record(request.context.tenant_id, 'SubmitPerformerContract', request);
  logger.info('submit_performer_contract', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    ticket: { assignee: { email: (request?.details?.approver?.email ?? '') } },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => helpdeskTicketCalculationClient.compareTicketOptions(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    venue_policy: {
      audit: {
        etag: (reply1?.ticket?.audit?.etag ?? ''),
        updated_at: reply1?.ticket?.audit?.updated_at,
        updated_by: (request?.performer_contract?.audit?.updated_by ?? ''),
      },
    },
  };
  const merged2 = Object.assign({}, request2, { context: request2.context });
  const reply2 = await call((done) => venuesVenuePolicyAdminClient.repairVenuePolicy(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
  const request3 = { context: request?.context };
  const reply3 = await withRetry(() => call((done) => surveysSurveyQueryClient.resolveSurveys(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      performer_contract: { due_at: reply1?.ticket?.sla?.due_at },
      result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
    },
    {
      performer_contract: { audit: { updated_at: reply2?.venue_policy?.audit?.updated_at } },
    },
    {
      result: {
        correlation_id: (reply3?.result?.correlation_id ?? ''),
        status: (reply3?.result?.status ?? ''),
      },
    },
    {
      performer_contract: {
        issued_at: request?.performer_contract?.issued_at,
        performer_contract_id: (request?.performer_contract_id ?? ''),
      },
    },
  );
}

module.exports = { approvePerformerContract, commentPerformerContractApproval, escalatePerformerContract, listPendingPerformerContracts, recallPerformerContract, rejectPerformerContract, submitPerformerContract };
