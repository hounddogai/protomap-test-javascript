'use strict';

/**
 * Moves a cash drawer between the states of its lifecycle. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  boxofficeBoxOfficeSaleQueryClient,
  boxofficeBoxOfficeSaleValidationClient,
  boxofficeCashDrawerCalculationClient,
  boxofficeGuestListCalculationClient,
  boxofficeGuestListValidationClient,
  broadcastsReplayClipApprovalClient,
  broadcastsStreamKeyApprovalClient,
  chatChatRoomClient,
  concessionsConcessionStandClient,
  contentArticleClient,
  documentsDocumentQueryClient,
  performersRiderApprovalClient,
  shippingShipmentQueryClient,
  sponsorshipsSponsorApprovalClient,
  venuesVenueHistoryClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect, upload } = require('../../../platform/streams');
const store = require('../store');

/**
 * Activates a cash drawer.
 */
async function activateCashDrawer(request) {
  validateContext(request.context);
  if (!request.cash_drawer_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
  }
  await store.record(request.context.tenant_id, 'ActivateCashDrawer', request);
  logger.info('activate_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { created_after: request?.details?.effective_at },
  };
  const method1 = 'countBoxOfficeSales';
  const reply1 = await call((done) => boxofficeBoxOfficeSaleQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
    },
  };
  const client2 = clients.registry.get('acme.chat.v1.ChatRoomService');
  const reply2 = await call((done) => client2.searchChatRooms(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    details: {
      approver: {
        display_name: (request?.details?.new_owner?.display_name ?? ''),
        email: (request?.details?.notify_contact?.email ?? ''),
        user_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    },
    rider: { owner: { email: (request?.details?.new_owner?.email ?? '') } },
  };
  const send3 = performersRiderApprovalClient.delegateRiderApproval.bind(performersRiderApprovalClient);
  const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    { outcome: { message: (reply2?.result?.message ?? '') } },
    {
      cash_drawer: { contact: { email: (reply3?.rider?.owner?.email ?? '') } },
    },
    {
      cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Expires a cash drawer.
 */
async function expireCashDrawer(request) {
  validateContext(request.context);
  if (!request.cash_drawer_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
  }
  await store.record(request.context.tenant_id, 'ExpireCashDrawer', request);
  logger.info('expire_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: {
        email: (request?.details?.notify_contact?.email ?? ''),
        name: request?.details?.notify_contact?.name,
        phone: (request?.details?.notify_contact?.phone ?? ''),
        preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
      },
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => boxofficeBoxOfficeSaleValidationClient.lintBoxOfficeSales(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    context: input2?.context,
    sponsor: {
      audit: { updated_at: reply1?.box_office_sale?.audit?.updated_at },
      relationship_manager: {
        display_name: (reply1?.box_office_sale?.counterparty?.display_name ?? ''),
        user_id: (input2?.details?.new_owner?.user_id ?? ''),
      },
    },
  };
  const reply2 = await call((done) => sponsorshipsSponsorApprovalClient.rejectSponsor(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') },
      result: { message: (reply1?.outcome?.message ?? '') },
    },
    {
      outcome: { message: (reply2?.result?.message ?? '') },
      result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
    },
    {
      cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Returns a cash drawer.
 */
async function getCashDrawerLifecycle(request) {
  validateContext(request.context);
  if (!request.cash_drawer_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
  }
  await store.record(request.context.tenant_id, 'GetCashDrawerLifecycle', request);
  logger.info('get_cash_drawer_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') } };
}

/**
 * Reinstates a cash drawer.
 */
async function reinstateCashDrawer(request) {
  validateContext(request.context);
  if (!request.cash_drawer_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
  }
  await store.record(request.context.tenant_id, 'ReinstateCashDrawer', request);
  logger.info('reinstate_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { approver: { user_id: (request?.details?.new_owner?.user_id ?? '') } },
    replay_clip: { owner: { email: (request?.details?.notify_contact?.email ?? '') } },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => broadcastsReplayClipApprovalClient.delegateReplayClipApproval(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { approver: { email: (reply1?.replay_clip?.owner?.email ?? '') } },
    stream_key: { contact: { email: (request?.details?.new_owner?.email ?? '') } },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => broadcastsStreamKeyApprovalClient.submitStreamKey(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      cash_drawer: {
        audit: {
          etag: (reply1?.replay_clip?.audit?.etag ?? ''),
          updated_at: reply1?.replay_clip?.audit?.updated_at,
        },
      },
    },
    { cash_drawer: { available: reply2?.stream_key?.available } },
    {
      cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Renews a cash drawer.
 */
async function renewCashDrawer(request) {
  validateContext(request.context);
  if (!request.cash_drawer_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
  }
  await store.record(request.context.tenant_id, 'RenewCashDrawer', request);
  logger.info('renew_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    header: {
      submitted_by: {
        email: (request?.details?.notify_contact?.email ?? ''),
        name: request?.details?.notify_contact?.name,
        phone: (request?.details?.notify_contact?.phone ?? ''),
        preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
      },
    },
  };
  const reply1 = await upload(
    (done) => chatChatRoomClient.batchCreateChatRooms(metadataFor(request1.context), { deadline: deadline() }, done),
    [request1],
  );
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') },
    outcome: { effective_at: request?.details?.effective_at },
  };
}

/**
 * Schedules a cash drawer.
 */
async function scheduleCashDrawerExpiry(request) {
  validateContext(request.context);
  if (request.cash_drawer) {
    await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
  }
  await store.record(request.context.tenant_id, 'ScheduleCashDrawerExpiry', request);
  logger.info('schedule_cash_drawer_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { labels: { ...(request?.cash_drawer?.labels ?? {}) } },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => contentArticleClient.searchArticles(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const results2 = [];
  for (const item of (request?.cash_drawer?.limits ?? [])) {
    const request2 = {
      context: request?.context,
      guest_list: {
        audit: { updated_at: request?.cash_drawer?.audit?.updated_at },
        card: {
          holder_name: ((request?.cash_drawer?.funding === 'card' ? request?.cash_drawer.card : undefined)?.holder_name ?? ''),
        },
        counterparty: { currency_code: (request?.cash_drawer?.currency_code ?? '') },
        lines: [{ tax: { amount: item?.amount } }],
      },
    };
    const reply2 = await call((done) => boxofficeGuestListCalculationClient.compareGuestListOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    results2.push(reply2);
  }
  const reply2 = results2.at(-1) ?? {};
  const request3 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: { filters: { ...(reply2?.result?.details ?? {}) } },
    reason: (request?.reason ?? ''),
  };
  const reply3 = await withRetry(() => call((done) => documentsDocumentQueryClient.summarizeDocuments(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    {
      result: {
        message: (reply3?.result?.message ?? ''),
        status: (reply3?.result?.status ?? ''),
      },
    },
    {
      cash_drawer: {
        available: request?.cash_drawer?.available,
        cash_drawer_id: (request?.cash_drawer_id ?? ''),
      },
    },
  );
}

/**
 * Suspends a cash drawer.
 */
async function suspendCashDrawer(request) {
  validateContext(request.context);
  if (!request.cash_drawer_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
  }
  await store.record(request.context.tenant_id, 'SuspendCashDrawer', request);
  logger.info('suspend_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
    },
  };
  const messages1 = await collect(shippingShipmentQueryClient.exportShipments(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
    },
  };
  const task2 = deferred(() => call((done) => boxofficeBoxOfficeSaleQueryClient.findBoxOfficeSales(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  const request3 = {
    context: request?.context,
    venue: {
      owner: {
        display_name: (request?.details?.new_owner?.display_name ?? ''),
        email: (request?.details?.new_owner?.email ?? ''),
        user_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    },
  };
  const reply3 = await bus.request('acme.boxoffice.v1.CashDrawerLifecycleService/SuspendCashDrawer:2', request3);
  const request4 = {
    cash_drawer: { audit: { updated_by: (reply3?.venue?.audit?.updated_by ?? '') } },
    context: request?.context,
  };
  const reply4 = await runInContext(request.context, () => call((done) => boxofficeCashDrawerCalculationClient.recalculateCashDrawers(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  const request5 = {
    context: request?.context,
    header: {
      source: (request?.cash_drawer_id ?? ''),
      submitted_by: request?.details?.notify_contact,
    },
  };
  const reply5 = await upload(
    (done) => concessionsConcessionStandClient.batchCreateConcessionStands(metadataFor(request5.context), { deadline: deadline() }, done),
    [request5],
  );
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply2?.result?.status ?? '') } },
    { outcome: { message: (reply3?.outcome?.message ?? '') } },
    { cash_drawer: { opened_at: reply4?.cash_drawer?.opened_at } },
    {
      cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Terminates a cash drawer.
 */
async function terminateCashDrawer(request) {
  validateContext(request.context);
  if (!request.cash_drawer_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
  }
  await store.record(request.context.tenant_id, 'TerminateCashDrawer', request);
  logger.info('terminate_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      notify: {
        email: (request?.details?.notify_contact?.email ?? ''),
        phone: (request?.details?.notify_contact?.phone ?? ''),
      },
    },
  };
  const reply1 = await call((done) => clients.concessionsConcessionOrderWorkflowClient.cancelConcessionOrderWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    guest_list: {
      audit: {
        created_at: reply1?.concession_order?.audit?.created_at,
        updated_at: reply1?.concession_order?.audit?.updated_at,
      },
      card: {
        network: ((reply1?.concession_order?.settlement === 'card' ? reply1?.concession_order.card : undefined)?.network ?? ''),
      },
      counterparty: { display_name: (request?.details?.new_owner?.display_name ?? '') },
    },
  };
  const reply2 = await new Promise((resolve, reject) => {
    boxofficeGuestListValidationClient.scoreGuestList(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.outcome?.status ?? '') } },
    {
      cash_drawer: { holder: { number: (reply2?.guest_list?.number ?? '') } },
    },
    {
      cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

module.exports = { activateCashDrawer, expireCashDrawer, getCashDrawerLifecycle, reinstateCashDrawer, renewCashDrawer, scheduleCashDrawerExpiry, suspendCashDrawer, terminateCashDrawer };
