'use strict';

/**
 * Runs the long-lived workflow that takes a guest list from start to finish. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const clients = require('../../../platform/clients');
const {
  boxofficeBoxOfficeSaleApprovalClient,
  boxofficeBoxOfficeSaleClient,
  boxofficeBoxOfficeSaleValidationClient,
  boxofficeCashDrawerApprovalClient,
  boxofficeCashDrawerWorkflowClient,
  concessionsMenuItemReportingClient,
  invoicingInvoiceBatchCalculationClient,
  metricsMetricSeriesClient,
  searchSearchIndexValidationClient,
  venuesVenuePolicyValidationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Advances a guest list.
 */
async function advanceGuestListWorkflow(request) {
  validateContext(request.context);
  if (request.guest_list) {
    await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
  }
  await store.record(request.context.tenant_id, 'AdvanceGuestListWorkflow', request);
  logger.info('advance_guest_list_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { contact: { phone: (request?.details?.notify?.phone ?? '') } },
    search_index: {
      address: {
        postal_code: (request?.guest_list?.billing_address?.postal_code ?? ''),
      },
      audit: { created_at: request?.guest_list?.audit?.created_at },
    },
  };
  const reply1 = await withRetry(() => call((done) => searchSearchIndexValidationClient.lintSearchIndexes(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    details: {
      recipient: {
        name: request?.details?.notify?.name,
        phone: (request?.details?.notify?.phone ?? ''),
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
  };
  const task2 = deferred(() => call((done) => concessionsMenuItemReportingClient.shareMenuItemReport(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  let selected3_2;
  switch (request?.guest_list?.settlement) {
    case 'card':
      selected3_2 = ((request?.guest_list?.settlement === 'card' ? request?.guest_list.card : undefined)?.network ?? '');
      break;
    default:
      selected3_2 = '';
  }
  let selected3_3;
  switch (reply2?.menu_item?.locator) {
    case 'address':
      selected3_3 = ((reply2?.menu_item?.locator === 'address' ? reply2?.menu_item.address : undefined)?.region ?? '');
      break;
    default:
      selected3_3 = '';
  }
  const request3 = {
    box_office_sale: {
      audit: { etag: (request?.guest_list?.audit?.etag ?? '') },
      billing_address: { region: selected3_3 },
      card: { network: selected3_2 },
    },
    context: request?.context,
  };
  const reply3 = await call((done) => boxofficeBoxOfficeSaleValidationClient.lintBoxOfficeSales(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      guest_list: {
        billing_address: {
          locality: ((reply1?.search_index?.locator === 'address' ? reply1?.search_index.address : undefined)?.locality ?? ''),
        },
      },
    },
    {
      guest_list: {
        billing_address: {
          region: ((reply2?.menu_item?.locator === 'address' ? reply2?.menu_item.address : undefined)?.region ?? ''),
        },
      },
    },
    { result: { status: (reply3?.result?.status ?? '') } },
    {
      guest_list: {
        guest_list_id: (request?.guest_list_id ?? ''),
        issued_at: request?.guest_list?.issued_at,
      },
    },
  );
}

/**
 * Cancels a guest list.
 */
async function cancelGuestListWorkflow(request) {
  validateContext(request.context);
  if (request.guest_list) {
    await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
  }
  await store.record(request.context.tenant_id, 'CancelGuestListWorkflow', request);
  logger.info('cancel_guest_list_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_policy: {
      audit: {
        created_at: request?.guest_list?.audit?.created_at,
        etag: (request?.guest_list?.audit?.etag ?? ''),
      },
      owner: { display_name: (request?.guest_list?.counterparty?.display_name ?? '') },
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => venuesVenuePolicyValidationClient.normalizeVenuePolicy(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const settled2 = await Promise.allSettled((request?.guest_list?.lines ?? []).map(async (item) => {
    const request2 = {
      context: request?.context,
      invoice_batch: {
        lines: [{
          attributes: { ...(item?.attributes ?? {}) },
          quantity: (item?.quantity ?? '0'),
          tax: {
            jurisdiction: (item?.tax?.jurisdiction ?? ''),
            rate: (item?.tax?.rate ?? 0),
          },
        }],
      },
    };
    const reply2 = await call((done) => invoicingInvoiceBatchCalculationClient.estimateInvoiceBatch(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return reply2;
  }));
  const results2 = settled2.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply2 = results2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      guest_list: { lines: [{ description: (reply1?.venue_policy?.description ?? '') }] },
      outcome: { next_steps: (reply1?.outcome?.next_steps ?? []) },
    },
    {
      guest_list: {
        billing_address: request?.guest_list?.billing_address,
        guest_list_id: (request?.guest_list_id ?? ''),
      },
    },
  );
}

/**
 * Returns a guest list.
 */
async function inspectGuestListWorkflow(request) {
  validateContext(request.context);
  if (!request.guest_list_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectGuestListWorkflow', request);
  logger.info('inspect_guest_list_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    invoice_batch: { counterparty: { email: (request?.context?.actor?.email ?? '') } },
  };
  const reply1 = await call((done) => clients.invoicingInvoiceBatchCalculationClient.simulateInvoiceBatch(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  return assemble(
    {
      guest_list: { audit: { etag: (reply1?.invoice_batch?.audit?.etag ?? '') } },
    },
    { guest_list: { guest_list_id: (request?.guest_list_id ?? '') } },
  );
}

/**
 * Pauses a guest list.
 */
async function pauseGuestListWorkflow(request) {
  validateContext(request.context);
  if (request.guest_list) {
    await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
  }
  await store.record(request.context.tenant_id, 'PauseGuestListWorkflow', request);
  logger.info('pause_guest_list_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  let selected1_2;
  switch (request?.guest_list?.settlement) {
    case 'card':
      selected1_2 = ((request?.guest_list?.settlement === 'card' ? request?.guest_list.card : undefined)?.last4 ?? '');
      break;
    default:
      selected1_2 = '';
  }
  let selected1_3;
  switch (request?.guest_list?.settlement) {
    case 'card':
      selected1_3 = ((request?.guest_list?.settlement === 'card' ? request?.guest_list.card : undefined)?.token ?? '');
      break;
    default:
      selected1_3 = '';
  }
  let selected1_4;
  switch (request?.guest_list?.settlement) {
    case 'card':
      selected1_4 = ((request?.guest_list?.settlement === 'card' ? request?.guest_list.card : undefined)?.holder_name ?? '');
      break;
    default:
      selected1_4 = '';
  }
  const request1 = {
    cash_drawer: {
      card: {
        holder_name: selected1_4,
        last4: selected1_2,
        token: selected1_3,
      },
    },
    context: request?.context,
    details: { approver: { email: (request?.details?.notify?.email ?? '') } },
  };
  const reply1 = await call((done) => boxofficeCashDrawerApprovalClient.commentCashDrawerApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    {
      guest_list: {
        guest_list_id: (request?.guest_list_id ?? ''),
        total: request?.guest_list?.total,
      },
    },
  );
}

/**
 * Retrys a guest list.
 */
async function retryGuestListWorkflow(request) {
  validateContext(request.context);
  if (request.guest_list) {
    await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
  }
  await store.record(request.context.tenant_id, 'RetryGuestListWorkflow', request);
  logger.info('retry_guest_list_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { region: (request?.guest_list?.billing_address?.region ?? '') },
  };
  const listPendingBoxOfficeSales1 = promisify(boxofficeBoxOfficeSaleApprovalClient.listPendingBoxOfficeSales.bind(boxofficeBoxOfficeSaleApprovalClient));
  const reply1 = await listPendingBoxOfficeSales1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    box_office_sale: {
      card: {
        last4: ((request?.guest_list?.settlement === 'card' ? request?.guest_list.card : undefined)?.last4 ?? ''),
      },
    },
    context: request?.context,
  };
  const reply2 = await call((done) => boxofficeBoxOfficeSaleClient.cloneBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { status: (reply1?.result?.status ?? '') },
      result: { status: (reply1?.result?.status ?? '') },
    },
    {
      outcome: { status: (reply2?.result?.status ?? '') },
      result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
    },
    {
      guest_list: {
        guest_list_id: (request?.guest_list_id ?? ''),
        issued_at: request?.guest_list?.issued_at,
      },
    },
  );
}

/**
 * Starts a guest list.
 */
async function startGuestListWorkflow(request) {
  validateContext(request.context);
  if (request.guest_list) {
    await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
  }
  await store.record(request.context.tenant_id, 'StartGuestListWorkflow', request);
  logger.info('start_guest_list_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.guest_list?.audit?.updated_at,
      owner_id: (request?.guest_list?.counterparty?.cash_drawer_id ?? ''),
      region: (request?.guest_list?.billing_address?.region ?? ''),
    },
  };
  const method1 = 'listCashDrawerWorkflows';
  const reply1 = await call((done) => boxofficeCashDrawerWorkflowClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    metric_series: {
      address: { locality: (request?.guest_list?.billing_address?.locality ?? '') },
    },
  };
  const client2 = clients.registry.get('acme.metrics.v1.MetricSeriesService');
  const reply2 = await call((done) => client2.updateMetricSeries(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    cash_drawer: { audit: { created_at: reply2?.metric_series?.audit?.created_at } },
    context: request?.context,
  };
  const send3 = boxofficeCashDrawerWorkflowClient.signalCashDrawerWorkflow.bind(boxofficeCashDrawerWorkflowClient);
  const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const results4 = [];
  for (const item of (request?.guest_list?.attachments ?? [])) {
    const request4 = {
      box_office_sale: {
        attachments: [{
          content_type: (item?.content_type ?? ''),
          file_name: (item?.file_name ?? ''),
          size_bytes: (item?.size_bytes ?? '0'),
        }],
      },
      context: request?.context,
    };
    const reply4 = await call((done) => boxofficeBoxOfficeSaleApprovalClient.commentBoxOfficeSaleApproval(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    results4.push(reply4);
  }
  const reply4 = results4.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    {
      guest_list: { audit: { created_at: reply2?.metric_series?.audit?.created_at } },
    },
    { outcome: { effective_at: reply3?.outcome?.effective_at } },
    {
      guest_list: {
        guest_list_id: (request?.guest_list_id ?? ''),
        notes: (request?.guest_list?.notes ?? ''),
      },
    },
  );
}

module.exports = { advanceGuestListWorkflow, cancelGuestListWorkflow, inspectGuestListWorkflow, pauseGuestListWorkflow, retryGuestListWorkflow, startGuestListWorkflow };
