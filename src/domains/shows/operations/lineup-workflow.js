'use strict';

/**
 * Runs the long-lived workflow that takes a lineup from start to finish. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsWristbandApprovalClient,
  auditAuditEventValidationClient,
  boxofficeBoxOfficeSaleWorkflowClient,
  featureflagsFeatureFlagQueryClient,
  notificationsNotificationQueryClient,
  showsLineupApprovalClient,
  showsLineupCalculationClient,
  showsLineupValidationClient,
  showsPerformanceCalculationClient,
  showsShowApprovalClient,
  showsShowClient,
  showsShowWorkflowClient,
  treasuryCashPositionClient,
  venuesVenuePolicyNotificationClient,
  venuesVenueSectionNotificationClient,
  venuesVenueSectionSyncClient,
  webhooksWebhookEndpointQueryClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect, exchange } = require('../../../platform/streams');
const store = require('../store');

/**
 * Advances a lineup.
 */
async function advanceLineupWorkflow(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'AdvanceLineupWorkflow', request);
  logger.info('advance_lineup_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    box_office_sale: {
      audit: {
        updated_at: request?.lineup?.audit?.updated_at,
        updated_by: (request?.lineup?.audit?.updated_by ?? ''),
      },
    },
    context: request?.context,
    details: {
      notify: {
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => boxofficeBoxOfficeSaleWorkflowClient.cancelBoxOfficeSaleWorkflow(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    cash_position: { audit: { etag: (input2?.lineup?.audit?.etag ?? '') } },
    context: input2?.context,
  };
  const reply2 = await call((done) => treasuryCashPositionClient.createCashPosition(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      lineup: {
        billing_address: {
          country_code: (reply1?.box_office_sale?.billing_address?.country_code ?? ''),
        },
      },
    },
    { result: { message: (reply2?.result?.message ?? '') } },
    {
      lineup: {
        lineup_id: (request?.lineup_id ?? ''),
        number: (request?.lineup?.number ?? ''),
      },
    },
  );
}

/**
 * Cancels a lineup.
 */
async function cancelLineupWorkflow(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'CancelLineupWorkflow', request);
  logger.info('cancel_lineup_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    lineup: {
      audit: { created_by: (request?.lineup?.audit?.created_by ?? '') },
      counterparty: { code: (request?.lineup?.counterparty?.code ?? '') },
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => showsLineupCalculationClient.explainLineupCalculation(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: reply1?.lineup?.due_at,
      region: (request?.lineup?.billing_address?.region ?? ''),
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => featureflagsFeatureFlagQueryClient.suggestFeatureFlags(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    show: {
      audit: {
        created_at: reply1?.lineup?.audit?.created_at,
        updated_at: reply1?.lineup?.audit?.updated_at,
      },
      participants: [{ contact: { phone: (request?.details?.notify?.phone ?? '') } }],
    },
  };
  const merged3 = Object.assign({}, request3, { context: request3.context });
  const reply3 = await call((done) => showsShowWorkflowClient.advanceShowWorkflow(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    show: {
      audit: { created_at: reply3?.show?.audit?.created_at },
      organizer: { display_name: (reply1?.lineup?.counterparty?.display_name ?? '') },
      participants: [{ contact: { email: (request?.details?.notify?.email ?? '') } }],
    },
  };
  const reply4 = await withRetry(() => call((done) => showsShowClient.cloneShow(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    {
      outcome: {
        message: (reply2?.result?.message ?? ''),
        status: (reply2?.result?.status ?? ''),
      },
    },
    {
      lineup: {
        billing_address: { locality: (reply3?.show?.location?.locality ?? '') },
      },
      result: { message: (reply3?.result?.message ?? '') },
    },
    {
      lineup: {
        billing_address: { recipient: (reply4?.show?.location?.recipient ?? '') },
        metadata: (reply4?.show?.metadata ?? {}),
      },
    },
    {
      lineup: {
        lineup_id: (request?.lineup_id ?? ''),
        notes: (request?.lineup?.notes ?? ''),
      },
    },
  );
}

/**
 * Returns a lineup.
 */
async function inspectLineupWorkflow(request) {
  validateContext(request.context);
  if (!request.lineup_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'lineup_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectLineupWorkflow', request);
  logger.info('inspect_lineup_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      labels: { ...(request?.context?.baggage ?? {}) },
      owner_id: (request?.lineup_id ?? ''),
    },
  };
  const messages1 = await collect(notificationsNotificationQueryClient.exportNotifications(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    performance: { organizer: { email: (request?.context?.actor?.email ?? '') } },
  };
  const task2 = deferred(() => call((done) => showsPerformanceCalculationClient.recalculatePerformances(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  return assemble(
    {
      lineup: {
        billing_address: {
          postal_code: (reply2?.performance?.location?.postal_code ?? ''),
          region: (reply2?.performance?.location?.region ?? ''),
        },
      },
    },
    { lineup: { lineup_id: (request?.lineup_id ?? '') } },
  );
}

/**
 * Lists lineups a page at a time.
 */
async function listLineupWorkflows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListLineupWorkflows', request);
  logger.info('list_lineup_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    wristband: {
      address: { region: (request?.filter?.region ?? '') },
      audit: { updated_at: request?.filter?.created_after },
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const reply1 = await bus.request('acme.shows.v1.LineupWorkflowService/ListLineupWorkflows:0', request1);
  const request2 = {
    context: request?.context,
    filter: {
      labels: { ...(request?.filter?.labels ?? {}) },
      region: (request?.filter?.region ?? ''),
    },
  };
  const messages2 = await collect(webhooksWebhookEndpointQueryClient.exportWebhookEndpoints(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  const request3 = {
    change: {
      children: [{
        children: [{ parent_id: (request?.context?.actor?.user_id ?? '') }],
        parent_id: (request?.context?.actor?.user_id ?? ''),
      }],
      parent_id: (request?.context?.actor?.user_id ?? ''),
    },
    context: request?.context,
  };
  const messages3 = await exchange(venuesVenueSectionSyncClient.streamVenueSectionChanges(metadataFor(request3.context), { deadline: deadline() }), [request3]);
  const reply3 = messages3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    lineups: [{
      billing_address: {
        region: ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.region ?? ''),
      },
      counterparty: { display_name: (reply1?.wristband?.owner?.display_name ?? '') },
    }],
  };
}

/**
 * Pauses a lineup.
 */
async function pauseLineupWorkflow(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'PauseLineupWorkflow', request);
  logger.info('pause_lineup_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    audit_event: {
      subject: {
        display_name: (request?.lineup?.counterparty?.display_name ?? ''),
        number: (request?.lineup?.number ?? ''),
      },
    },
    context: request?.context,
    details: {
      destination: { recipient: (request?.lineup?.billing_address?.recipient ?? '') },
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => auditAuditEventValidationClient.scoreAuditEvent(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    details: {
      approver: {
        display_name: (request?.lineup?.counterparty?.display_name ?? ''),
        email: (request?.details?.notify?.email ?? ''),
      },
    },
    lineup: {
      audit: { etag: (request?.lineup?.audit?.etag ?? '') },
      bank: {
        bic: ((request?.lineup?.settlement === 'bank' ? request?.lineup.bank : undefined)?.bic ?? ''),
      },
    },
  };
  const reply2 = await call((done) => clients.showsLineupApprovalClient.escalateLineup(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: reply2?.lineup?.audit?.updated_at,
      owner_id: (reply2?.lineup?.counterparty?.venue_id ?? ''),
      region: (reply2?.lineup?.billing_address?.region ?? ''),
    },
  };
  const reply3 = await new Promise((resolve, reject) => {
    showsLineupApprovalClient.listPendingLineups(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request4 = {
    context: request?.context,
    details: {
      recipient: {
        name: request?.details?.notify?.name,
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
  };
  const sendVenuePolicyTestNotice4 = promisify(venuesVenuePolicyNotificationClient.sendVenuePolicyTestNotice.bind(venuesVenuePolicyNotificationClient));
  const reply4 = await sendVenuePolicyTestNotice4(request4, metadataFor(request4.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    {
      lineup: {
        audit: { created_by: (reply2?.lineup?.audit?.created_by ?? '') },
        billing_address: { locality: (reply2?.lineup?.billing_address?.locality ?? '') },
      },
    },
    { outcome: { message: (reply3?.result?.message ?? '') } },
    {
      lineup: {
        lines: [{
          tax: {
            amount: (reply4?.venue_policy?.action === 'discount' ? reply4?.venue_policy.discount : undefined)?.amount,
          },
        }],
      },
      outcome: { reference_id: (reply4?.outcome?.reference_id ?? '') },
    },
    {
      lineup: {
        lineup_id: (request?.lineup_id ?? ''),
        total: request?.lineup?.total,
      },
    },
  );
}

/**
 * Resumes a lineup.
 */
async function resumeLineupWorkflow(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'ResumeLineupWorkflow', request);
  logger.info('resume_lineup_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    lineup: {
      audit: { updated_by: (request?.lineup?.audit?.updated_by ?? '') },
      bank: {
        bic: ((request?.lineup?.settlement === 'bank' ? request?.lineup.bank : undefined)?.bic ?? ''),
      },
    },
  };
  const reply1 = await call((done) => showsLineupApprovalClient.rejectLineup(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  let selected2_1;
  switch (request?.lineup?.settlement) {
    case 'bank':
      selected2_1 = ((request?.lineup?.settlement === 'bank' ? request?.lineup.bank : undefined)?.bic ?? '');
      break;
    default:
      selected2_1 = '';
  }
  let selected2_4;
  switch (request?.lineup?.settlement) {
    case 'bank':
      selected2_4 = ((request?.lineup?.settlement === 'bank' ? request?.lineup.bank : undefined)?.iban ?? '');
      break;
    default:
      selected2_4 = '';
  }
  const request2 = {
    context: request?.context,
    details: {
      destination: { recipient: (reply1?.lineup?.billing_address?.recipient ?? '') },
    },
    lineup: {
      audit: { updated_at: reply1?.lineup?.audit?.updated_at },
      bank: {
        bic: selected2_1,
        iban: selected2_4,
      },
    },
  };
  const reply2 = await call((done) => showsLineupValidationClient.scoreLineup(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { lineup: { notes: (reply1?.lineup?.notes ?? '') } },
    {
      lineup: {
        billing_address: { country_code: (reply2?.lineup?.billing_address?.country_code ?? '') },
      },
      result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
    },
    {
      lineup: {
        lineup_id: (request?.lineup_id ?? ''),
        metadata: (request?.lineup?.metadata ?? {}),
      },
    },
  );
}

/**
 * Retrys a lineup.
 */
async function retryLineupWorkflow(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'RetryLineupWorkflow', request);
  logger.info('retry_lineup_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
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
  const client1 = clients.registry.get('acme.venues.v1.VenueSectionNotificationService');
  const reply1 = await call((done) => client1.unsubscribeVenueSectionUpdates(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { next_steps: (reply1?.outcome?.next_steps ?? []) } },
    {
      lineup: {
        due_at: request?.lineup?.due_at,
        lineup_id: (request?.lineup_id ?? ''),
      },
    },
  );
}

/**
 * Signals a lineup.
 */
async function signalLineupWorkflow(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'SignalLineupWorkflow', request);
  logger.info('signal_lineup_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { recipient: { phone: (request?.details?.notify?.phone ?? '') } },
  };
  const send1 = venuesVenuePolicyNotificationClient.sendVenuePolicyTestNotice.bind(venuesVenuePolicyNotificationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    show: {
      participants: [{ contact: { phone: (request?.details?.notify?.phone ?? '') } }],
    },
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => showsShowApprovalClient.approveShow(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  const { context: context3, ...body3 } = request;
  const input3 = { ...body3, context: context3 };
  const request3 = {
    context: input3?.context,
    lineup: {
      billing_address: { country_code: (input3?.lineup?.billing_address?.country_code ?? '') },
      card: {
        last4: ((input3?.lineup?.settlement === 'card' ? input3?.lineup.card : undefined)?.last4 ?? ''),
      },
      lines: [{
        tax: {
          amount: (reply1?.venue_policy?.action === 'discount' ? reply1?.venue_policy.discount : undefined)?.amount,
        },
      }],
    },
  };
  const reply3 = await call((done) => showsLineupApprovalClient.approveLineup(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      lineup: { audit: { etag: (reply1?.venue_policy?.audit?.etag ?? '') } },
      result: { status: (reply1?.outcome?.status ?? '') },
    },
    {
      lineup: {
        billing_address: { country_code: (reply2?.show?.location?.country_code ?? '') },
      },
      outcome: { message: (reply2?.result?.message ?? '') },
    },
    {
      lineup: { audit: { created_by: (reply3?.lineup?.audit?.created_by ?? '') } },
    },
    {
      lineup: {
        lineup_id: (request?.lineup_id ?? ''),
        number: (request?.lineup?.number ?? ''),
      },
    },
  );
}

/**
 * Starts a lineup.
 */
async function startLineupWorkflow(request) {
  validateContext(request.context);
  if (request.lineup) {
    await store.save(store.toLineupRecord(request.context.tenant_id, request.lineup));
  }
  await store.record(request.context.tenant_id, 'StartLineupWorkflow', request);
  logger.info('start_lineup_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
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
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => venuesVenueSectionNotificationClient.unsubscribeVenueSectionUpdates(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    show: {
      location: { country_code: (request?.lineup?.billing_address?.country_code ?? '') },
      participants: [{
        contact: {
          email: (reply1?.venue_section?.manager?.email ?? ''),
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      }],
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => showsShowApprovalClient.approveShow(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      lineup: { audit: { updated_at: reply1?.venue_section?.audit?.updated_at } },
    },
    {
      lineup: {
        billing_address: {
          lines: (reply2?.show?.location?.lines ?? []),
          region: (reply2?.show?.location?.region ?? ''),
        },
      },
    },
    {
      lineup: {
        lineup_id: (request?.lineup_id ?? ''),
        notes: (request?.lineup?.notes ?? ''),
      },
    },
  );
}

module.exports = { advanceLineupWorkflow, cancelLineupWorkflow, inspectLineupWorkflow, listLineupWorkflows, pauseLineupWorkflow, resumeLineupWorkflow, retryLineupWorkflow, signalLineupWorkflow, startLineupWorkflow };
