'use strict';

/**
 * Moves a concession order between the states of its lifecycle. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  auditAuditEventQueryClient,
  boxofficeBoxOfficeSaleCalculationClient,
  broadcastsBroadcastApprovalClient,
  concessionsConcessionOrderCalculationClient,
  concessionsConcessionOrderQueryClient,
  concessionsConcessionOrderReportingClient,
  concessionsMenuItemCalculationClient,
  concessionsMenuItemReportingClient,
  featureflagsFeatureFlagQueryClient,
  ledgerLedgerAccountQueryClient,
  metricsMetricSeriesValidationClient,
  sponsorshipsSponsorshipDealWorkflowClient,
  venuesVenueSectionNotificationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Activates a concession order.
 */
async function activateConcessionOrder(request) {
  validateContext(request.context);
  if (!request.concession_order_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
  }
  await store.record(request.context.tenant_id, 'ActivateConcessionOrder', request);
  logger.info('activate_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    menu_item: {
      owner: {
        display_name: (request?.details?.new_owner?.display_name ?? ''),
        email: (request?.details?.new_owner?.email ?? ''),
      },
    },
  };
  const client1 = clients.registry.get('acme.concessions.v1.MenuItemCalculationService');
  const reply1 = await call((done) => client1.compareMenuItemOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    box_office_sale: {
      billing_address: {
        lines: [...((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.lines ?? [])],
      },
    },
    context: request?.context,
  };
  const send2 = boxofficeBoxOfficeSaleCalculationClient.calculateBoxOfficeSale.bind(boxofficeBoxOfficeSaleCalculationClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      concession_order: { counterparty: { code: (reply1?.menu_item?.code ?? '') } },
      result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
    },
    {
      concession_order: {
        billing_address: { locality: (reply2?.box_office_sale?.billing_address?.locality ?? '') },
      },
      result: { details: (reply2?.result?.details ?? {}) },
    },
    {
      concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Expires a concession order.
 */
async function expireConcessionOrder(request) {
  validateContext(request.context);
  if (!request.concession_order_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
  }
  await store.record(request.context.tenant_id, 'ExpireConcessionOrder', request);
  logger.info('expire_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => venuesVenueSectionNotificationClient.listVenueSectionSubscriptions(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    context: input2?.context,
    details: {
      notify: {
        email: (input2?.details?.notify_contact?.email ?? ''),
        name: input2?.details?.notify_contact?.name,
        preferred_channel: (input2?.details?.notify_contact?.preferred_channel ?? ''),
      },
    },
    sponsorship_deal: { counterparty: { email: (input2?.details?.new_owner?.email ?? '') } },
  };
  const reply2 = await call((done) => sponsorshipsSponsorshipDealWorkflowClient.retrySponsorshipDealWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    {
      concession_order: {
        billing_address: {
          country_code: (reply2?.sponsorship_deal?.billing_address?.country_code ?? ''),
        },
      },
    },
    {
      concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Returns a concession order.
 */
async function getConcessionOrderLifecycle(request) {
  validateContext(request.context);
  if (!request.concession_order_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
  }
  await store.record(request.context.tenant_id, 'GetConcessionOrderLifecycle', request);
  logger.info('get_concession_order_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      owner_id: (request?.concession_order_id ?? ''),
    },
  };
  const messages1 = await collect(featureflagsFeatureFlagQueryClient.exportFeatureFlags(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    filter: {
      labels: { ...(request?.context?.baggage ?? {}) },
      owner_id: (request?.concession_order_id ?? ''),
    },
  };
  const messages2 = await collect(ledgerLedgerAccountQueryClient.exportLedgerAccounts(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  return {
    concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
  };
}

/**
 * Reinstates a concession order.
 */
async function reinstateConcessionOrder(request) {
  validateContext(request.context);
  if (!request.concession_order_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
  }
  await store.record(request.context.tenant_id, 'ReinstateConcessionOrder', request);
  logger.info('reinstate_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      owner_id: (request?.concession_order_id ?? ''),
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => concessionsConcessionOrderQueryClient.findConcessionOrders(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      labels: { ...(reply1?.result?.details ?? {}) },
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => concessionsMenuItemReportingClient.listMenuItemReports(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: { owner_id: (reply2?.result?.correlation_id ?? '') },
  };
  const merged3 = Object.assign({}, request3, { context: request3.context });
  const reply3 = await call((done) => auditAuditEventQueryClient.browseAuditEvents(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: {
        message: (reply1?.result?.message ?? ''),
        status: (reply1?.result?.status ?? ''),
      },
    },
    {
      result: {
        correlation_id: (reply2?.result?.correlation_id ?? ''),
        reference_ids: (reply2?.result?.reference_ids ?? []),
      },
    },
    { result: { details: (reply3?.result?.details ?? {}) } },
    {
      concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Renews a concession order.
 */
async function renewConcessionOrder(request) {
  validateContext(request.context);
  if (!request.concession_order_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
  }
  await store.record(request.context.tenant_id, 'RenewConcessionOrder', request);
  logger.info('renew_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { contact: { email: (request?.details?.notify_contact?.email ?? '') } },
    metric_series: {
      owner: {
        email: (request?.details?.new_owner?.email ?? ''),
        user_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    },
  };
  const reply1 = await withRetry(() => call((done) => metricsMetricSeriesValidationClient.checkMetricSeriesEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    concession_order: {
      audit: { updated_by: (reply1?.metric_series?.audit?.updated_by ?? '') },
    },
    context: request?.context,
  };
  const task2 = deferred(() => call((done) => concessionsConcessionOrderCalculationClient.estimateConcessionOrder(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  const request3 = {
    context: request?.context,
    details: {
      destination: { locality: (reply2?.concession_order?.billing_address?.locality ?? '') },
    },
    menu_item: {
      audit: { updated_at: reply1?.metric_series?.audit?.updated_at },
      components: { primary: {
        description: (reply1?.metric_series?.components?.primary?.description ?? ''),
      } },
      dimensions: { length: (reply1?.metric_series?.dimensions?.length ?? 0) },
    },
  };
  const reply3 = await bus.request('acme.concessions.v1.ConcessionOrderLifecycleService/RenewConcessionOrder:2', request3);
  const request4 = {
    broadcast: {
      organizer: { display_name: (reply3?.menu_item?.owner?.display_name ?? '') },
      participants: [{
        contact: {
          email: (request?.details?.new_owner?.email ?? ''),
          name: request?.details?.notify_contact?.name,
        },
      }],
    },
    context: request?.context,
    details: { approver: { user_id: (reply3?.menu_item?.owner?.user_id ?? '') } },
  };
  const reply4 = await runInContext(request.context, () => call((done) => broadcastsBroadcastApprovalClient.rejectBroadcast(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      concession_order: {
        billing_address: {
          locality: ((reply1?.metric_series?.locator === 'address' ? reply1?.metric_series.address : undefined)?.locality ?? ''),
        },
      },
    },
    {
      concession_order: { notes: (reply2?.concession_order?.notes ?? '') },
      result: { status: (reply2?.result?.status ?? '') },
    },
    {
      concession_order: {
        audit: { updated_by: (reply3?.menu_item?.audit?.updated_by ?? '') },
        counterparty: { menu_item_id: (reply3?.menu_item?.menu_item_id ?? '') },
      },
    },
    {
      concession_order: {
        audit: {
          created_by: (reply4?.broadcast?.audit?.created_by ?? ''),
          etag: (reply4?.broadcast?.audit?.etag ?? ''),
        },
      },
    },
    {
      concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Schedules a concession order.
 */
async function scheduleConcessionOrderExpiry(request) {
  validateContext(request.context);
  if (request.concession_order) {
    await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
  }
  await store.record(request.context.tenant_id, 'ScheduleConcessionOrderExpiry', request);
  logger.info('schedule_concession_order_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { contact: { email: (request?.details?.notify?.email ?? '') } },
    menu_item: {
      address: {
        recipient: (request?.concession_order?.billing_address?.recipient ?? ''),
      },
      audit: { created_at: request?.concession_order?.audit?.created_at },
    },
  };
  const reply1 = await call((done) => clients.concessionsMenuItemValidationClient.previewMenuItem(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: reply1?.menu_item?.audit?.created_at,
      region: (request?.concession_order?.billing_address?.region ?? ''),
    },
  };
  const reply2 = await new Promise((resolve, reject) => {
    ledgerLedgerAccountQueryClient.countLedgerAccounts(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    { outcome: { status: (reply2?.result?.status ?? '') } },
    {
      concession_order: {
        concession_order_id: (request?.concession_order_id ?? ''),
        status: (request?.concession_order?.status ?? ''),
      },
    },
  );
}

/**
 * Suspends a concession order.
 */
async function suspendConcessionOrder(request) {
  validateContext(request.context);
  if (!request.concession_order_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
  }
  await store.record(request.context.tenant_id, 'SuspendConcessionOrder', request);
  logger.info('suspend_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const resolveAuditEvents1 = promisify(auditAuditEventQueryClient.resolveAuditEvents.bind(auditAuditEventQueryClient));
  const reply1 = await resolveAuditEvents1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    annotations: { ...(request?.annotations ?? {}) },
    concession_order: {
      concession_order_id: (request?.concession_order_id ?? ''),
      counterparty: { display_name: (request?.details?.new_owner?.display_name ?? '') },
    },
    context: request?.context,
  };
  const reply2 = await call((done) => concessionsConcessionOrderCalculationClient.estimateConcessionOrder(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        status: (reply1?.result?.status ?? ''),
      },
    },
    {
      concession_order: {
        audit: { updated_by: (reply2?.concession_order?.audit?.updated_by ?? '') },
      },
      result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
    },
    {
      concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Terminates a concession order.
 */
async function terminateConcessionOrder(request) {
  validateContext(request.context);
  if (!request.concession_order_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
  }
  await store.record(request.context.tenant_id, 'TerminateConcessionOrder', request);
  logger.info('terminate_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
    },
  };
  const method1 = 'browseLedgerAccounts';
  const reply1 = await call((done) => ledgerLedgerAccountQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    annotations: { ...(request?.annotations ?? {}) },
    concession_order_id: (request?.concession_order_id ?? ''),
    context: request?.context,
    reason: (request?.reason ?? ''),
  };
  const client2 = clients.registry.get('acme.concessions.v1.ConcessionOrderReportingService');
  const reply2 = await call((done) => client2.generateConcessionOrderReport(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.notify_contact?.preferred_channel ?? '') });
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        status: (reply1?.result?.status ?? ''),
      },
    },
    {
      result: {
        correlation_id: (reply2?.result?.correlation_id ?? ''),
        reference_ids: (reply2?.result?.reference_ids ?? []),
      },
    },
    {
      concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

module.exports = { activateConcessionOrder, expireConcessionOrder, getConcessionOrderLifecycle, reinstateConcessionOrder, renewConcessionOrder, scheduleConcessionOrderExpiry, suspendConcessionOrder, terminateConcessionOrder };
