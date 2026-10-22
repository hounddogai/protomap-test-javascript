'use strict';

/**
 * Runs the long-lived workflow that takes a ticket from start to finish. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  boxofficeBoxOfficeSaleApprovalClient,
  concessionsMenuItemReportingClient,
  documentsDocumentQueryClient,
  metricsMetricSeriesValidationClient,
  notificationsNotificationQueryClient,
  sponsorshipsSponsorApprovalClient,
  ticketingTicketTierQueryClient,
  ticketingTicketTransferValidationClient,
  venuesVenuePolicyNotificationClient,
  venuesVenuePolicySyncClient,
  yardsYardQueryClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect, exchange } = require('../../../platform/streams');
const store = require('../store');

/**
 * Advances a ticket.
 */
async function advanceTicketWorkflow(request) {
  validateContext(request.context);
  if (request.ticket) {
    await store.save(store.toTicketRecord(request.context.tenant_id, request.ticket));
  }
  await store.record(request.context.tenant_id, 'AdvanceTicketWorkflow', request);
  logger.info('advance_ticket_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { contact: { email: (request?.details?.notify?.email ?? '') } },
    metric_series: {
      address: { locality: (request?.ticket?.billing_address?.locality ?? '') },
    },
  };
  const send1 = metricsMetricSeriesValidationClient.previewMetricSeries.bind(metricsMetricSeriesValidationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.details?.notify?.email ?? ''),
        name: request?.details?.notify?.name,
        phone: (request?.details?.notify?.phone ?? ''),
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => venuesVenuePolicyNotificationClient.notifyVenuePolicyWatchers(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      ticket: {
        billing_address: {
          locality: ((reply1?.metric_series?.locator === 'address' ? reply1?.metric_series.address : undefined)?.locality ?? ''),
        },
        counterparty: { code: (reply1?.metric_series?.code ?? '') },
      },
    },
    { result: { status: (reply2?.outcome?.status ?? '') } },
    {
      ticket: {
        metadata: (request?.ticket?.metadata ?? {}),
        ticket_id: (request?.ticket_id ?? ''),
      },
    },
  );
}

/**
 * Cancels a ticket.
 */
async function cancelTicketWorkflow(request) {
  validateContext(request.context);
  if (request.ticket) {
    await store.save(store.toTicketRecord(request.context.tenant_id, request.ticket));
  }
  await store.record(request.context.tenant_id, 'CancelTicketWorkflow', request);
  logger.info('cancel_ticket_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    details: {
      contact: {
        email: (input1?.details?.notify?.email ?? ''),
        phone: (input1?.details?.notify?.phone ?? ''),
      },
    },
    ticket_transfer: { comments: [{ created_at: input1?.ticket?.audit?.created_at }] },
  };
  const reply1 = await call((done) => ticketingTicketTransferValidationClient.lintTicketTransfers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      ticket: {
        counterparty: { display_name: (reply1?.ticket_transfer?.assignee?.display_name ?? '') },
      },
    },
    {
      ticket: {
        notes: (request?.ticket?.notes ?? ''),
        ticket_id: (request?.ticket_id ?? ''),
      },
    },
  );
}

/**
 * Returns a ticket.
 */
async function inspectTicketWorkflow(request) {
  validateContext(request.context);
  if (!request.ticket_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'ticket_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectTicketWorkflow', request);
  logger.info('inspect_ticket_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { owner_id: (request?.ticket_id ?? '') },
  };
  const messages1 = await collect(notificationsNotificationQueryClient.exportNotifications(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    filter: { owner_id: (request?.ticket_id ?? '') },
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => documentsDocumentQueryClient.browseDocuments(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  return assemble(
    { result: { message: (reply2?.result?.message ?? '') } },
    { ticket: { ticket_id: (request?.ticket_id ?? '') } },
  );
}

/**
 * Lists tickets a page at a time.
 */
async function listTicketWorkflows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListTicketWorkflows', request);
  logger.info('list_ticket_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      owner_id: (request?.filter?.owner_id ?? ''),
      query: (request?.filter?.query ?? ''),
    },
  };
  const messages1 = await collect(notificationsNotificationQueryClient.exportNotifications(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Pauses a ticket.
 */
async function pauseTicketWorkflow(request) {
  validateContext(request.context);
  if (request.ticket) {
    await store.save(store.toTicketRecord(request.context.tenant_id, request.ticket));
  }
  await store.record(request.context.tenant_id, 'PauseTicketWorkflow', request);
  logger.info('pause_ticket_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    ticket_transfer: {
      audit: { updated_at: request?.ticket?.audit?.updated_at },
      requester: {
        email: (request?.details?.notify?.email ?? ''),
        phone: (request?.details?.notify?.phone ?? ''),
      },
    },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => ticketingTicketTransferValidationClient.lintTicketTransfers(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.ticket?.audit?.updated_at,
      region: (request?.ticket?.billing_address?.region ?? ''),
    },
  };
  const merged2 = Object.assign({}, request2, { context: request2.context });
  const reply2 = await call((done) => yardsYardQueryClient.findYards(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    {
      result: {
        message: (reply2?.result?.message ?? ''),
        status: (reply2?.result?.status ?? ''),
      },
    },
    {
      ticket: {
        status: (request?.ticket?.status ?? ''),
        ticket_id: (request?.ticket_id ?? ''),
      },
    },
  );
}

/**
 * Resumes a ticket.
 */
async function resumeTicketWorkflow(request) {
  validateContext(request.context);
  if (request.ticket) {
    await store.save(store.toTicketRecord(request.context.tenant_id, request.ticket));
  }
  await store.record(request.context.tenant_id, 'ResumeTicketWorkflow', request);
  logger.info('resume_ticket_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    ticket_transfer: { audit: { updated_by: (request?.ticket?.audit?.updated_by ?? '') } },
  };
  const reply1 = await withRetry(() => call((done) => ticketingTicketTransferValidationClient.validateTicketTransfer(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    sponsor: {
      contact: { name: request?.details?.notify?.name },
      primary_address: { postal_code: (request?.ticket?.billing_address?.postal_code ?? '') },
      relationship_manager: { user_id: (reply1?.ticket_transfer?.assignee?.user_id ?? '') },
    },
  };
  const task2 = deferred(() => call((done) => sponsorshipsSponsorApprovalClient.commentSponsorApproval(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      ticket: {
        audit: { created_by: (reply1?.ticket_transfer?.audit?.created_by ?? '') },
      },
    },
    {
      ticket: { lines: [{ attributes: (reply2?.sponsor?.attributes ?? {}) }] },
    },
    {
      ticket: {
        status: (request?.ticket?.status ?? ''),
        ticket_id: (request?.ticket_id ?? ''),
      },
    },
  );
}

/**
 * Retrys a ticket.
 */
async function retryTicketWorkflow(request) {
  validateContext(request.context);
  if (request.ticket) {
    await store.save(store.toTicketRecord(request.context.tenant_id, request.ticket));
  }
  await store.record(request.context.tenant_id, 'RetryTicketWorkflow', request);
  logger.info('retry_ticket_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    ticket_transfer: {
      assignee: {
        display_name: (request?.ticket?.counterparty?.display_name ?? ''),
        email: (request?.details?.notify?.email ?? ''),
      },
      audit: { created_at: request?.ticket?.audit?.created_at },
      requester: { name: request?.details?.notify?.name },
    },
  };
  const reply1 = await bus.request('acme.ticketing.v1.TicketWorkflowService/RetryTicketWorkflow:0', request1);
  const request2 = {
    box_office_sale: {
      audit: { updated_at: request?.ticket?.audit?.updated_at },
      billing_address: {
        lines: [...(request?.ticket?.billing_address?.lines ?? [])],
        locality: (request?.ticket?.billing_address?.locality ?? ''),
      },
      card: {
        last4: ((request?.ticket?.settlement === 'card' ? request?.ticket.card : undefined)?.last4 ?? ''),
      },
    },
    context: request?.context,
  };
  const reply2 = await runInContext(request.context, () => call((done) => boxofficeBoxOfficeSaleApprovalClient.recallBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    { result: { status: (reply2?.outcome?.status ?? '') } },
    {
      ticket: {
        metadata: (request?.ticket?.metadata ?? {}),
        ticket_id: (request?.ticket_id ?? ''),
      },
    },
  );
}

/**
 * Signals a ticket.
 */
async function signalTicketWorkflow(request) {
  validateContext(request.context);
  if (request.ticket) {
    await store.save(store.toTicketRecord(request.context.tenant_id, request.ticket));
  }
  await store.record(request.context.tenant_id, 'SignalTicketWorkflow', request);
  logger.info('signal_ticket_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    ticket: {
      bank: {
        bic: ((request?.ticket?.settlement === 'bank' ? request?.ticket.bank : undefined)?.bic ?? ''),
      },
    },
  };
  const reply1 = await call((done) => clients.ticketingTicketClient.updateTicket(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      ticket: {
        audit: { updated_at: reply1?.ticket?.audit?.updated_at },
        counterparty: { display_name: (reply1?.ticket?.counterparty?.display_name ?? '') },
      },
    },
    {
      ticket: {
        ticket_id: (request?.ticket_id ?? ''),
        total: request?.ticket?.total,
      },
    },
  );
}

/**
 * Starts a ticket.
 */
async function startTicketWorkflow(request) {
  validateContext(request.context);
  if (request.ticket) {
    await store.save(store.toTicketRecord(request.context.tenant_id, request.ticket));
  }
  await store.record(request.context.tenant_id, 'StartTicketWorkflow', request);
  logger.info('start_ticket_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    subscriber: { email: (request?.details?.notify?.email ?? '') },
  };
  const messages1 = await collect(concessionsMenuItemReportingClient.streamMenuItemMetrics(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.ticket?.audit?.created_at,
      labels: { ...(request?.details?.parameters ?? {}) },
      owner_id: (request?.ticket?.ticket_id ?? ''),
      region: (request?.ticket?.billing_address?.region ?? ''),
    },
  };
  const reply2 = await new Promise((resolve, reject) => {
    ticketingTicketTierQueryClient.browseTicketTiers(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request3 = {
    change: {
      discount: { amount: request?.ticket?.total },
      parameters: { ...(request?.details?.parameters ?? {}) },
      scopes: [...(request?.context?.actor?.roles ?? [])],
    },
    context: request?.context,
  };
  const messages3 = await exchange(venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request3.context), { deadline: deadline() }), [request3]);
  const reply3 = messages3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    {
      ticket: {
        notes: (request?.ticket?.notes ?? ''),
        ticket_id: (request?.ticket_id ?? ''),
      },
    },
  );
}

module.exports = { advanceTicketWorkflow, cancelTicketWorkflow, inspectTicketWorkflow, listTicketWorkflows, pauseTicketWorkflow, resumeTicketWorkflow, retryTicketWorkflow, signalTicketWorkflow, startTicketWorkflow };
