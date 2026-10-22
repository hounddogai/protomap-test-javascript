'use strict';

/**
 * Runs the long-lived workflow that takes a cash drawer from start to finish. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsGateScanApprovalClient,
  auditAuditEventValidationClient,
  boxofficeBoxOfficeSaleApprovalClient,
  boxofficeBoxOfficeSaleCalculationClient,
  boxofficeBoxOfficeSaleClient,
  boxofficeBoxOfficeSaleQueryClient,
  boxofficeBoxOfficeSaleWorkflowClient,
  boxofficeCashDrawerLifecycleClient,
  boxofficeGuestListApprovalClient,
  venuesVenueNotificationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Advances a cash drawer.
 */
async function advanceCashDrawerWorkflow(request) {
  validateContext(request.context);
  if (request.cash_drawer) {
    await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
  }
  await store.record(request.context.tenant_id, 'AdvanceCashDrawerWorkflow', request);
  logger.info('advance_cash_drawer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const settled1 = await Promise.allSettled((request?.cash_drawer?.limits ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      guest_list: {
        card: {
          network: ((request?.cash_drawer?.funding === 'card' ? request?.cash_drawer.card : undefined)?.network ?? ''),
        },
        lines: [{ tax: { amount: item?.amount } }],
      },
    };
    const reply1 = await call((done) => boxofficeGuestListApprovalClient.submitGuestList(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return reply1;
  }));
  const results1 = settled1.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply1 = results1.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    cash_drawer: {
      available: request?.cash_drawer?.available,
      cash_drawer_id: (request?.cash_drawer_id ?? ''),
    },
  };
}

/**
 * Cancels a cash drawer.
 */
async function cancelCashDrawerWorkflow(request) {
  validateContext(request.context);
  if (request.cash_drawer) {
    await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
  }
  await store.record(request.context.tenant_id, 'CancelCashDrawerWorkflow', request);
  logger.info('cancel_cash_drawer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    gate_scan: {
      subject: { display_name: (request?.cash_drawer?.holder?.display_name ?? '') },
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => admissionsGateScanApprovalClient.commentGateScanApproval(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  let selected2_1;
  switch (request?.cash_drawer?.funding) {
    case 'card':
      selected2_1 = ((request?.cash_drawer?.funding === 'card' ? request?.cash_drawer.card : undefined)?.token ?? '');
      break;
    default:
      selected2_1 = '';
  }
  const request2 = {
    box_office_sale: {
      audit: { created_by: (request?.cash_drawer?.audit?.created_by ?? '') },
      card: { token: selected2_1 },
    },
    context: request?.context,
  };
  const reply2 = await call((done) => boxofficeBoxOfficeSaleApprovalClient.escalateBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { correlation_id: (reply1?.gate_scan?.correlation_id ?? '') } },
    {
      cash_drawer: {
        holder: {
          display_name: (reply2?.box_office_sale?.counterparty?.display_name ?? ''),
        },
      },
    },
    {
      cash_drawer: {
        cash_drawer_id: (request?.cash_drawer_id ?? ''),
        labels: (request?.cash_drawer?.labels ?? {}),
      },
    },
  );
}

/**
 * Returns a cash drawer.
 */
async function inspectCashDrawerWorkflow(request) {
  validateContext(request.context);
  if (!request.cash_drawer_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectCashDrawerWorkflow', request);
  logger.info('inspect_cash_drawer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.context?.baggage ?? {}) },
    context: request?.context,
    details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => venuesVenueNotificationClient.subscribeVenueUpdates(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    { cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') } },
  );
}

/**
 * Lists cash drawers a page at a time.
 */
async function listCashDrawerWorkflows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListCashDrawerWorkflows', request);
  logger.info('list_cash_drawer_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Pauses a cash drawer.
 */
async function pauseCashDrawerWorkflow(request) {
  validateContext(request.context);
  if (request.cash_drawer) {
    await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
  }
  await store.record(request.context.tenant_id, 'PauseCashDrawerWorkflow', request);
  logger.info('pause_cash_drawer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    box_office_sale: {
      audit: { updated_at: request?.cash_drawer?.audit?.updated_at },
      card: {
        network: ((request?.cash_drawer?.funding === 'card' ? request?.cash_drawer.card : undefined)?.network ?? ''),
      },
    },
    context: request?.context,
  };
  const reply1 = await withRetry(() => call((done) => boxofficeBoxOfficeSaleApprovalClient.rejectBoxOfficeSale(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  let selected2_1;
  switch (request?.cash_drawer?.funding) {
    case 'bank':
      selected2_1 = ((request?.cash_drawer?.funding === 'bank' ? request?.cash_drawer.bank : undefined)?.bic ?? '');
      break;
    default:
      selected2_1 = '';
  }
  const request2 = {
    box_office_sale: {
      audit: { created_by: (reply1?.box_office_sale?.audit?.created_by ?? '') },
      bank: { bic: selected2_1 },
      counterparty: {
        currency_code: (reply1?.box_office_sale?.counterparty?.currency_code ?? ''),
      },
    },
    context: request?.context,
  };
  const reply2 = await call((done) => boxofficeBoxOfficeSaleApprovalClient.escalateBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    box_office_sale_id: (request?.cash_drawer?.holder?.box_office_sale_id ?? ''),
    context: request?.context,
  };
  const reply3 = await bus.request('acme.boxoffice.v1.CashDrawerWorkflowService/PauseCashDrawerWorkflow:2', request3);
  const request4 = {
    box_office_sale_id: (request?.cash_drawer?.holder?.box_office_sale_id ?? ''),
    context: request?.context,
    etag: (reply2?.box_office_sale?.audit?.etag ?? ''),
    reason: (request?.reason ?? ''),
  };
  const reply4 = await runInContext(request.context, () => call((done) => boxofficeBoxOfficeSaleClient.deleteBoxOfficeSale(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
  return assemble(
    {
      cash_drawer: { audit: { updated_at: reply1?.box_office_sale?.audit?.updated_at } },
    },
    {
      cash_drawer: {
        cash_drawer_id: (reply2?.box_office_sale?.counterparty?.cash_drawer_id ?? ''),
      },
      result: { message: (reply2?.outcome?.message ?? '') },
    },
    { result: { correlation_id: (reply3?.result?.correlation_id ?? '') } },
    { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
    {
      cash_drawer: {
        available: request?.cash_drawer?.available,
        currency_code: (request?.cash_drawer?.currency_code ?? ''),
      },
    },
  );
}

/**
 * Resumes a cash drawer.
 */
async function resumeCashDrawerWorkflow(request) {
  validateContext(request.context);
  if (request.cash_drawer) {
    await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
  }
  await store.record(request.context.tenant_id, 'ResumeCashDrawerWorkflow', request);
  logger.info('resume_cash_drawer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: {
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
    feature_flag: {
      audit: { updated_by: (request?.cash_drawer?.audit?.updated_by ?? '') },
      owner: { email: (request?.cash_drawer?.contact?.email ?? '') },
    },
  };
  const reply1 = await call((done) => clients.featureflagsFeatureFlagValidationClient.normalizeFeatureFlag(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    box_office_sale: {
      audit: { etag: (request?.cash_drawer?.audit?.etag ?? '') },
      card: {
        network: ((request?.cash_drawer?.funding === 'card' ? request?.cash_drawer.card : undefined)?.network ?? ''),
      },
    },
    context: request?.context,
  };
  const reply2 = await new Promise((resolve, reject) => {
    boxofficeBoxOfficeSaleCalculationClient.recalculateBoxOfficeSales(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.outcome?.message ?? '') } },
    {
      cash_drawer: { currency_code: (reply2?.breakdown?.currency_code ?? '') },
    },
    {
      cash_drawer: {
        cash_drawer_id: (request?.cash_drawer_id ?? ''),
        opened_at: request?.cash_drawer?.opened_at,
      },
    },
  );
}

/**
 * Retrys a cash drawer.
 */
async function retryCashDrawerWorkflow(request) {
  validateContext(request.context);
  if (request.cash_drawer) {
    await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
  }
  await store.record(request.context.tenant_id, 'RetryCashDrawerWorkflow', request);
  logger.info('retry_cash_drawer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    guest_list: {
      audit: {
        created_at: request?.cash_drawer?.audit?.created_at,
        etag: (request?.cash_drawer?.audit?.etag ?? ''),
      },
      card: {
        last4: ((request?.cash_drawer?.funding === 'card' ? request?.cash_drawer.card : undefined)?.last4 ?? ''),
      },
    },
  };
  const approveGuestList1 = promisify(boxofficeGuestListApprovalClient.approveGuestList.bind(boxofficeGuestListApprovalClient));
  const reply1 = await approveGuestList1(request1, metadataFor(request1.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: {
        reference_id: (reply1?.outcome?.reference_id ?? ''),
        status: (reply1?.outcome?.status ?? ''),
      },
    },
    {
      cash_drawer: {
        cash_drawer_id: (request?.cash_drawer_id ?? ''),
        opened_at: request?.cash_drawer?.opened_at,
      },
    },
  );
}

/**
 * Signals a cash drawer.
 */
async function signalCashDrawerWorkflow(request) {
  validateContext(request.context);
  if (request.cash_drawer) {
    await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
  }
  await store.record(request.context.tenant_id, 'SignalCashDrawerWorkflow', request);
  logger.info('signal_cash_drawer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    cash_drawer_id: (request?.cash_drawer_id ?? ''),
    context: request?.context,
    details: { new_owner: { email: (request?.cash_drawer?.contact?.email ?? '') } },
  };
  const reply1 = await call((done) => boxofficeCashDrawerLifecycleClient.activateCashDrawer(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { approver: { email: (request?.cash_drawer?.contact?.email ?? '') } },
    guest_list: {
      audit: { created_by: (reply1?.cash_drawer?.audit?.created_by ?? '') },
      bank: {
        account_holder: ((reply1?.cash_drawer?.funding === 'bank' ? reply1?.cash_drawer.bank : undefined)?.account_holder ?? ''),
      },
    },
  };
  const method2 = 'escalateGuestList';
  const reply2 = await call((done) => boxofficeGuestListApprovalClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.result?.status ?? '') } },
    { result: { status: (reply2?.result?.status ?? '') } },
    {
      cash_drawer: {
        cash_drawer_id: (request?.cash_drawer_id ?? ''),
        currency_code: (request?.cash_drawer?.currency_code ?? ''),
      },
    },
  );
}

/**
 * Starts a cash drawer.
 */
async function startCashDrawerWorkflow(request) {
  validateContext(request.context);
  if (request.cash_drawer) {
    await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
  }
  await store.record(request.context.tenant_id, 'StartCashDrawerWorkflow', request);
  logger.info('start_cash_drawer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    audit_event: {
      subject: {
        display_name: (request?.cash_drawer?.holder?.display_name ?? ''),
        number: (request?.cash_drawer?.holder?.number ?? ''),
      },
    },
    context: request?.context,
    details: { currency_code: (request?.cash_drawer?.currency_code ?? '') },
    reason: (request?.reason ?? ''),
  };
  const client1 = clients.registry.get('acme.audit.v1.AuditEventValidationService');
  const reply1 = await call((done) => client1.scoreAuditEvent(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    box_office_sale_id: (request?.cash_drawer?.holder?.box_office_sale_id ?? ''),
    context: request?.context,
  };
  const send2 = boxofficeBoxOfficeSaleQueryClient.lookupBoxOfficeSale.bind(boxofficeBoxOfficeSaleQueryClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    { outcome: { message: (reply2?.result?.message ?? '') } },
    {
      cash_drawer: {
        cash_drawer_id: (request?.cash_drawer_id ?? ''),
        labels: (request?.cash_drawer?.labels ?? {}),
      },
    },
  );
}

module.exports = { advanceCashDrawerWorkflow, cancelCashDrawerWorkflow, inspectCashDrawerWorkflow, listCashDrawerWorkflows, pauseCashDrawerWorkflow, resumeCashDrawerWorkflow, retryCashDrawerWorkflow, signalCashDrawerWorkflow, startCashDrawerWorkflow };
