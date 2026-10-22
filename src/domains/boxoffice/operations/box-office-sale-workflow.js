'use strict';

/**
 * Runs the long-lived workflow that takes a box office sale from start to finish. Each operation validates, records,
 * and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  billingBillingAccountQueryClient,
  boxofficeBoxOfficeSaleApprovalClient,
  boxofficeBoxOfficeSaleClient,
  boxofficeCashDrawerCalculationClient,
  boxofficeCashDrawerLifecycleClient,
  boxofficeCashDrawerValidationClient,
  boxofficeGuestListCalculationClient,
  boxofficeGuestListLifecycleClient,
  boxofficeGuestListReportingClient,
  broadcastsBroadcastWorkflowClient,
  concessionsConcessionStandApprovalClient,
  sponsorshipsSponsorApprovalClient,
  venuesVenueSectionNotificationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Advances a box office sale.
 */
async function advanceBoxOfficeSaleWorkflow(request) {
  validateContext(request.context);
  if (request.box_office_sale) {
    await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
  }
  await store.record(request.context.tenant_id, 'AdvanceBoxOfficeSaleWorkflow', request);
  logger.info('advance_box_office_sale_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: { audit: { created_at: request?.box_office_sale?.audit?.created_at } },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => sponsorshipsSponsorApprovalClient.rejectSponsor(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    cash_drawer_id: (request?.box_office_sale?.counterparty?.cash_drawer_id ?? ''),
    context: request?.context,
    details: {
      new_owner: { user_id: (reply1?.sponsor?.relationship_manager?.user_id ?? '') },
      notify_contact: {
        name: reply1?.sponsor?.contact?.name,
        phone: (reply1?.sponsor?.contact?.phone ?? ''),
      },
    },
  };
  const reply2 = await withRetry(() => call((done) => boxofficeCashDrawerLifecycleClient.renewCashDrawer(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const request3 = {
    context: request?.context,
    details: { notify: { name: request?.details?.notify?.name } },
    guest_list: { audit: { etag: (reply2?.cash_drawer?.audit?.etag ?? '') } },
  };
  const task3 = deferred(() => call((done) => boxofficeGuestListLifecycleClient.scheduleGuestListExpiry(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  const reply3 = await task3();
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      box_office_sale: {
        billing_address: { postal_code: (reply1?.sponsor?.primary_address?.postal_code ?? '') },
        counterparty: {
          display_name: (reply1?.sponsor?.relationship_manager?.display_name ?? ''),
        },
      },
    },
    {
      box_office_sale: { number: (reply2?.cash_drawer?.holder?.number ?? '') },
      outcome: { status: (reply2?.outcome?.status ?? '') },
    },
    { box_office_sale: { due_at: reply3?.guest_list?.due_at } },
    {
      box_office_sale: {
        box_office_sale_id: (request?.box_office_sale_id ?? ''),
        notes: (request?.box_office_sale?.notes ?? ''),
      },
    },
  );
}

/**
 * Cancels a box office sale.
 */
async function cancelBoxOfficeSaleWorkflow(request) {
  validateContext(request.context);
  if (request.box_office_sale) {
    await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
  }
  await store.record(request.context.tenant_id, 'CancelBoxOfficeSaleWorkflow', request);
  logger.info('cancel_box_office_sale_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.details?.notify?.email ?? ''),
        name: request?.details?.notify?.name,
        phone: (request?.details?.notify?.phone ?? ''),
      },
    },
  };
  const reply1 = await bus.request('acme.boxoffice.v1.BoxOfficeSaleWorkflowService/CancelBoxOfficeSaleWorkflow:0', request1);
  const request2 = {
    context: request?.context,
    details: {
      approver: {
        email: (request?.details?.notify?.email ?? ''),
        user_id: (reply1?.venue_section?.manager?.user_id ?? ''),
      },
    },
  };
  const reply2 = await runInContext(request.context, () => call((done) => concessionsConcessionStandApprovalClient.approveConcessionStand(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      box_office_sale: { audit: { etag: (reply1?.venue_section?.audit?.etag ?? '') } },
      result: { status: (reply1?.outcome?.status ?? '') },
    },
    {
      box_office_sale: { audit: { etag: (reply2?.concession_stand?.audit?.etag ?? '') } },
    },
    {
      box_office_sale: {
        box_office_sale_id: (request?.box_office_sale_id ?? ''),
        number: (request?.box_office_sale?.number ?? ''),
      },
    },
  );
}

/**
 * Returns a box office sale.
 */
async function inspectBoxOfficeSaleWorkflow(request) {
  validateContext(request.context);
  if (!request.box_office_sale_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'box_office_sale_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectBoxOfficeSaleWorkflow', request);
  logger.info('inspect_box_office_sale_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { filters: { ...(request?.context?.baggage ?? {}) } },
  };
  const reply1 = await call((done) => clients.authorizationAccessPolicyQueryClient.summarizeAccessPolicies(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        message: (reply1?.result?.message ?? ''),
      },
    },
    {
      box_office_sale: { box_office_sale_id: (request?.box_office_sale_id ?? '') },
    },
  );
}

/**
 * Lists box office sales a page at a time.
 */
async function listBoxOfficeSaleWorkflows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListBoxOfficeSaleWorkflows', request);
  logger.info('list_box_office_sale_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const reply1 = await new Promise((resolve, reject) => {
    billingBillingAccountQueryClient.resolveBillingAccounts(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    box_office_sale: { billing_address: { region: (request?.filter?.region ?? '') } },
    context: request?.context,
    details: { approver: { user_id: (request?.context?.actor?.user_id ?? '') } },
  };
  const escalateBoxOfficeSale2 = promisify(boxofficeBoxOfficeSaleApprovalClient.escalateBoxOfficeSale.bind(boxofficeBoxOfficeSaleApprovalClient));
  const reply2 = await escalateBoxOfficeSale2(request2, metadataFor(request2.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    {
      box_office_sales: [{
        counterparty: {
          display_name: (reply2?.box_office_sale?.counterparty?.display_name ?? ''),
        },
      }],
      result: { status: (reply2?.result?.status ?? '') },
    },
  );
}

/**
 * Pauses a box office sale.
 */
async function pauseBoxOfficeSaleWorkflow(request) {
  validateContext(request.context);
  if (request.box_office_sale) {
    await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
  }
  await store.record(request.context.tenant_id, 'PauseBoxOfficeSaleWorkflow', request);
  logger.info('pause_box_office_sale_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    reason: (request?.reason ?? ''),
  };
  const reply1 = await call((done) => boxofficeGuestListReportingClient.generateGuestListReport(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.box_office_sale?.audit?.created_at,
      labels: { ...(request?.box_office_sale?.metadata ?? {}) },
      owner_id: (request?.box_office_sale?.counterparty?.cash_drawer_id ?? ''),
      region: (request?.box_office_sale?.billing_address?.region ?? ''),
    },
  };
  const method2 = 'listVenueSectionSubscriptions';
  const reply2 = await call((done) => venuesVenueSectionNotificationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
  return assemble(
    { outcome: { message: (reply1?.result?.message ?? '') } },
    {
      outcome: { status: (reply2?.result?.status ?? '') },
      result: { status: (reply2?.result?.status ?? '') },
    },
    {
      box_office_sale: {
        box_office_sale_id: (request?.box_office_sale_id ?? ''),
        counterparty: request?.box_office_sale?.counterparty,
      },
    },
  );
}

/**
 * Resumes a box office sale.
 */
async function resumeBoxOfficeSaleWorkflow(request) {
  validateContext(request.context);
  if (request.box_office_sale) {
    await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
  }
  await store.record(request.context.tenant_id, 'ResumeBoxOfficeSaleWorkflow', request);
  logger.info('resume_box_office_sale_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    cash_drawer: {
      audit: {
        created_at: request?.box_office_sale?.audit?.created_at,
        updated_at: request?.box_office_sale?.audit?.updated_at,
      },
      contact: {
        name: request?.details?.notify?.name,
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
    context: request?.context,
  };
  const client1 = clients.registry.get('acme.boxoffice.v1.CashDrawerValidationService');
  const reply1 = await call((done) => client1.checkCashDrawerEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: { region: (request?.box_office_sale?.billing_address?.region ?? '') },
  };
  const send2 = boxofficeBoxOfficeSaleClient.searchBoxOfficeSales.bind(boxofficeBoxOfficeSaleClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    broadcast: {
      organizer: { display_name: (reply1?.cash_drawer?.holder?.display_name ?? '') },
      participants: [{
        contact: {
          email: (request?.details?.notify?.email ?? ''),
          name: request?.details?.notify?.name,
          phone: (request?.details?.notify?.phone ?? ''),
        },
      }],
    },
    context: request?.context,
  };
  const spread3 = {
    ...request3,
    context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
  };
  const reply3 = await call((done) => broadcastsBroadcastWorkflowClient.pauseBroadcastWorkflow(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      box_office_sale: {
        audit: { created_by: (reply1?.cash_drawer?.audit?.created_by ?? '') },
        counterparty: { currency_code: (reply1?.cash_drawer?.currency_code ?? '') },
      },
    },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    { box_office_sale: { metadata: (reply3?.broadcast?.metadata ?? {}) } },
    {
      box_office_sale: {
        box_office_sale_id: (request?.box_office_sale_id ?? ''),
        status: (request?.box_office_sale?.status ?? ''),
      },
    },
  );
}

/**
 * Retrys a box office sale.
 */
async function retryBoxOfficeSaleWorkflow(request) {
  validateContext(request.context);
  if (request.box_office_sale) {
    await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
  }
  await store.record(request.context.tenant_id, 'RetryBoxOfficeSaleWorkflow', request);
  logger.info('retry_box_office_sale_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    cash_drawer_id: (input1?.box_office_sale?.counterparty?.cash_drawer_id ?? ''),
    context: input1?.context,
  };
  const reply1 = await call((done) => boxofficeCashDrawerCalculationClient.getCashDrawerRates(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    cash_drawer: {
      bank: {
        account_holder: ((reply1?.cash_drawer?.funding === 'bank' ? reply1?.cash_drawer.bank : undefined)?.account_holder ?? ''),
      },
      contact: {
        email: (request?.details?.notify?.email ?? ''),
        phone: (request?.details?.notify?.phone ?? ''),
      },
    },
    context: request?.context,
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => boxofficeCashDrawerCalculationClient.recalculateCashDrawers(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      box_office_sale: { audit: { updated_by: (reply1?.cash_drawer?.audit?.updated_by ?? '') } },
    },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    {
      box_office_sale: {
        box_office_sale_id: (request?.box_office_sale_id ?? ''),
        total: request?.box_office_sale?.total,
      },
    },
  );
}

/**
 * Signals a box office sale.
 */
async function signalBoxOfficeSaleWorkflow(request) {
  validateContext(request.context);
  if (request.box_office_sale) {
    await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
  }
  await store.record(request.context.tenant_id, 'SignalBoxOfficeSaleWorkflow', request);
  logger.info('signal_box_office_sale_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    cash_drawer: {
      bank: {
        account_holder: ((request?.box_office_sale?.settlement === 'bank' ? request?.box_office_sale.bank : undefined)?.account_holder ?? ''),
        iban: ((request?.box_office_sale?.settlement === 'bank' ? request?.box_office_sale.bank : undefined)?.iban ?? ''),
      },
      contact: {
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
    context: request?.context,
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => boxofficeCashDrawerValidationClient.validateCashDrawer(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    guest_list: {
      audit: { created_at: request?.box_office_sale?.audit?.created_at },
      billing_address: {
        postal_code: (request?.box_office_sale?.billing_address?.postal_code ?? ''),
      },
    },
  };
  const merged2 = Object.assign({}, request2, { context: request2.context });
  const reply2 = await call((done) => boxofficeGuestListCalculationClient.compareGuestListOptions(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      box_office_sale: {
        audit: {
          etag: (reply1?.cash_drawer?.audit?.etag ?? ''),
          updated_at: reply1?.cash_drawer?.audit?.updated_at,
        },
      },
    },
    {
      box_office_sale: {
        audit: { etag: (reply2?.guest_list?.audit?.etag ?? '') },
        counterparty: {
          cash_drawer_id: (reply2?.guest_list?.counterparty?.cash_drawer_id ?? ''),
        },
      },
    },
    {
      box_office_sale: {
        box_office_sale_id: (request?.box_office_sale_id ?? ''),
        notes: (request?.box_office_sale?.notes ?? ''),
      },
    },
  );
}

/**
 * Starts a box office sale.
 */
async function startBoxOfficeSaleWorkflow(request) {
  validateContext(request.context);
  if (request.box_office_sale) {
    await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
  }
  await store.record(request.context.tenant_id, 'StartBoxOfficeSaleWorkflow', request);
  logger.info('start_box_office_sale_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = [];
  for (const item of (request?.box_office_sale?.lines ?? [])) {
    const request1 = {
      context: request?.context,
      guest_list: { lines: [{ tax: { exemptions: [...(item?.tax?.exemptions ?? [])] } }] },
    };
    const reply1 = await call((done) => boxofficeGuestListCalculationClient.recalculateGuestLists(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    results1.push(reply1);
  }
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    box_office_sale: {
      card: {
        network: ((request?.box_office_sale?.settlement === 'card' ? request?.box_office_sale.card : undefined)?.network ?? ''),
      },
      counterparty: {
        display_name: (request?.box_office_sale?.counterparty?.display_name ?? ''),
      },
    },
    context: request?.context,
  };
  const reply2 = await withRetry(() => call((done) => boxofficeBoxOfficeSaleApprovalClient.escalateBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      box_office_sale: {
        counterparty: {
          currency_code: (reply2?.box_office_sale?.counterparty?.currency_code ?? ''),
        },
      },
    },
    {
      box_office_sale: {
        billing_address: request?.box_office_sale?.billing_address,
        box_office_sale_id: (request?.box_office_sale_id ?? ''),
      },
    },
  );
}

module.exports = { advanceBoxOfficeSaleWorkflow, cancelBoxOfficeSaleWorkflow, inspectBoxOfficeSaleWorkflow, listBoxOfficeSaleWorkflows, pauseBoxOfficeSaleWorkflow, resumeBoxOfficeSaleWorkflow, retryBoxOfficeSaleWorkflow, signalBoxOfficeSaleWorkflow, startBoxOfficeSaleWorkflow };
