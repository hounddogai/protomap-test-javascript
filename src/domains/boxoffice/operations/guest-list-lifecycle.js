'use strict';

/**
 * Moves a guest list between the states of its lifecycle. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionApprovalClient,
  admissionsWristbandReportingClient,
  admissionsWristbandWorkflowClient,
  boxofficeBoxOfficeSaleCalculationClient,
  boxofficeBoxOfficeSaleQueryClient,
  boxofficeCashDrawerValidationClient,
  boxofficeGuestListReportingClient,
  broadcastsBroadcastClient,
  concessionsConcessionStandApprovalClient,
  concessionsConcessionStandWorkflowClient,
  concessionsMenuItemWorkflowClient,
  notificationsNotificationValidationClient,
  venuesVenuePolicyNotificationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { upload } = require('../../../platform/streams');
const store = require('../store');

/**
 * Activates a guest list.
 */
async function activateGuestList(request) {
  validateContext(request.context);
  if (!request.guest_list_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
  }
  await store.record(request.context.tenant_id, 'ActivateGuestList', request);
  logger.info('activate_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      notify: {
        phone: (request?.details?.notify_contact?.phone ?? ''),
        preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
      },
    },
  };
  const reply1 = await call((done) => concessionsMenuItemWorkflowClient.pauseMenuItemWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    cash_drawer: { audit: { created_at: reply1?.menu_item?.audit?.created_at } },
    context: request?.context,
    details: { contact: { email: (request?.details?.new_owner?.email ?? '') } },
  };
  const method2 = 'previewCashDrawer';
  const reply2 = await call((done) => boxofficeCashDrawerValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
  };
  const client3 = clients.registry.get('acme.boxoffice.v1.GuestListReportingService');
  const reply3 = await call((done) => client3.scheduleGuestListReport(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      guest_list: { audit: { etag: (reply1?.menu_item?.audit?.etag ?? '') } },
      outcome: { status: (reply1?.result?.status ?? '') },
    },
    {
      guest_list: { audit: { updated_at: reply2?.cash_drawer?.audit?.updated_at } },
    },
    { result: { reference_ids: (reply3?.result?.reference_ids ?? []) } },
    {
      guest_list: { guest_list_id: (request?.guest_list_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Expires a guest list.
 */
async function expireGuestList(request) {
  validateContext(request.context);
  if (!request.guest_list_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
  }
  await store.record(request.context.tenant_id, 'ExpireGuestList', request);
  logger.info('expire_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    guest_list_id: (request?.guest_list_id ?? ''),
    reason: (request?.reason ?? ''),
  };
  const send1 = boxofficeGuestListReportingClient.generateGuestListReport.bind(boxofficeGuestListReportingClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    admission: { correlation_id: (reply1?.result?.correlation_id ?? '') },
    context: request?.context,
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => admissionsAdmissionApprovalClient.recallAdmission(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  const { context: context3, ...body3 } = request;
  const input3 = { ...body3, context: context3 };
  const request3 = {
    cash_drawer: {
      contact: {
        email: (input3?.details?.notify_contact?.email ?? ''),
        phone: (input3?.details?.notify_contact?.phone ?? ''),
        preferred_channel: (input3?.details?.notify_contact?.preferred_channel ?? ''),
      },
      holder: { display_name: (reply2?.admission?.subject?.display_name ?? '') },
    },
    context: input3?.context,
  };
  const reply3 = await call((done) => boxofficeCashDrawerValidationClient.scoreCashDrawer(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    guest_list_id: (request?.guest_list_id ?? ''),
  };
  const cloned4 = JSON.parse(JSON.stringify(request4));
  const reply4 = await call((done) => boxofficeGuestListReportingClient.fetchGuestListReport(cloned4, metadataFor(cloned4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.new_owner?.email ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      result: { details: (reply1?.result?.details ?? {}) },
    },
    { result: { correlation_id: (reply2?.admission?.correlation_id ?? '') } },
    {
      guest_list: { audit: { created_at: reply3?.cash_drawer?.audit?.created_at } },
    },
    { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
    {
      guest_list: { guest_list_id: (request?.guest_list_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Returns a guest list.
 */
async function getGuestListLifecycle(request) {
  validateContext(request.context);
  if (!request.guest_list_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
  }
  await store.record(request.context.tenant_id, 'GetGuestListLifecycle', request);
  logger.info('get_guest_list_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { guest_list: { guest_list_id: (request?.guest_list_id ?? '') } };
}

/**
 * Reinstates a guest list.
 */
async function reinstateGuestList(request) {
  validateContext(request.context);
  if (!request.guest_list_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
  }
  await store.record(request.context.tenant_id, 'ReinstateGuestList', request);
  logger.info('reinstate_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    box_office_sale: {
      counterparty: { display_name: (request?.details?.new_owner?.display_name ?? '') },
    },
    context: request?.context,
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => boxofficeBoxOfficeSaleCalculationClient.recalculateBoxOfficeSales(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: reply1?.box_office_sale?.audit?.created_at,
      region: (reply1?.box_office_sale?.billing_address?.region ?? ''),
    },
  };
  const merged2 = Object.assign({}, request2, { context: request2.context });
  const reply2 = await call((done) => admissionsWristbandReportingClient.countWristbandTotals(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: reply1?.box_office_sale?.audit?.updated_at,
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
      region: (reply1?.box_office_sale?.billing_address?.region ?? ''),
    },
  };
  const reply3 = await withRetry(() => call((done) => admissionsWristbandWorkflowClient.listWristbandWorkflows(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  const request4 = {
    concession_stand: {
      address: {
        postal_code: (reply1?.box_office_sale?.billing_address?.postal_code ?? ''),
        recipient: (reply1?.box_office_sale?.billing_address?.recipient ?? ''),
      },
      audit: { updated_at: reply1?.box_office_sale?.audit?.updated_at },
    },
    context: request?.context,
    details: { notify: { email: (request?.details?.notify_contact?.email ?? '') } },
  };
  const task4 = deferred(() => call((done) => concessionsConcessionStandWorkflowClient.resumeConcessionStandWorkflow(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  const reply4 = await task4();
  const request5 = {
    context: request?.context,
    header: {
      source: (request?.context?.actor?.user_id ?? ''),
      submitted_by: request?.details?.notify_contact,
    },
  };
  const reply5 = await upload(
    (done) => broadcastsBroadcastClient.batchCreateBroadcasts(metadataFor(request5.context), { deadline: deadline() }, done),
    [request5],
  );
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      guest_list: {
        billing_address: { region: (reply1?.box_office_sale?.billing_address?.region ?? '') },
      },
    },
    { outcome: { status: (reply2?.result?.status ?? '') } },
    { result: { status: (reply3?.result?.status ?? '') } },
    { outcome: { message: (reply4?.outcome?.message ?? '') } },
    {
      guest_list: { guest_list_id: (request?.guest_list_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Renews a guest list.
 */
async function renewGuestList(request) {
  validateContext(request.context);
  if (!request.guest_list_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
  }
  await store.record(request.context.tenant_id, 'RenewGuestList', request);
  logger.info('renew_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: {
        email: (request?.details?.notify_contact?.email ?? ''),
        name: request?.details?.notify_contact?.name,
      },
    },
  };
  const reply1 = await bus.request('acme.boxoffice.v1.GuestListLifecycleService/RenewGuestList:0', request1);
  const request2 = {
    context: request?.context,
    filter: {
      created_after: reply1?.box_office_sale?.audit?.created_at,
      labels: { ...(reply1?.result?.details ?? {}) },
      owner_id: (reply1?.outcome?.reference_id ?? ''),
      region: (reply1?.box_office_sale?.billing_address?.region ?? ''),
    },
  };
  const reply2 = await runInContext(request.context, () => call((done) => boxofficeGuestListReportingClient.countGuestListTotals(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const request3 = {
    context: request?.context,
    venue: {
      address: { locality: (reply1?.box_office_sale?.billing_address?.locality ?? '') },
      audit: { etag: (reply1?.box_office_sale?.audit?.etag ?? '') },
      owner: { email: (request?.details?.new_owner?.email ?? '') },
    },
  };
  const reply3 = await call((done) => clients.venuesVenueClient.cloneVenue(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    details: { contact: { email: (reply3?.venue?.owner?.email ?? '') } },
    notification: {
      assignee: {
        display_name: (reply3?.venue?.owner?.display_name ?? ''),
        email: (request?.details?.notify_contact?.email ?? ''),
      },
    },
  };
  const reply4 = await new Promise((resolve, reject) => {
    notificationsNotificationValidationClient.previewNotification(request4, metadataFor(request4.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      guest_list: {
        billing_address: { lines: (reply1?.box_office_sale?.billing_address?.lines ?? []) },
      },
    },
    {
      result: {
        details: (reply2?.result?.details ?? {}),
        reference_ids: (reply2?.result?.reference_ids ?? []),
      },
    },
    {
      guest_list: {
        counterparty: { display_name: (reply3?.venue?.owner?.display_name ?? '') },
      },
    },
    {
      guest_list: { audit: { updated_at: reply4?.notification?.audit?.updated_at } },
      result: { status: (reply4?.outcome?.status ?? '') },
    },
    {
      guest_list: { guest_list_id: (request?.guest_list_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Schedules a guest list.
 */
async function scheduleGuestListExpiry(request) {
  validateContext(request.context);
  if (request.guest_list) {
    await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
  }
  await store.record(request.context.tenant_id, 'ScheduleGuestListExpiry', request);
  logger.info('schedule_guest_list_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      recipient: {
        name: request?.details?.notify?.name,
        phone: (request?.details?.notify?.phone ?? ''),
      },
    },
  };
  const sendVenuePolicyTestNotice1 = promisify(venuesVenuePolicyNotificationClient.sendVenuePolicyTestNotice.bind(venuesVenuePolicyNotificationClient));
  const reply1 = await sendVenuePolicyTestNotice1(request1, metadataFor(request1.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { effective_at: reply1?.outcome?.effective_at } },
    {
      guest_list: {
        guest_list_id: (request?.guest_list_id ?? ''),
        notes: (request?.guest_list?.notes ?? ''),
      },
    },
  );
}

/**
 * Suspends a guest list.
 */
async function suspendGuestList(request) {
  validateContext(request.context);
  if (!request.guest_list_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
  }
  await store.record(request.context.tenant_id, 'SuspendGuestList', request);
  logger.info('suspend_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    concession_stand: { owner: { user_id: (request?.details?.new_owner?.user_id ?? '') } },
    context: request?.context,
  };
  const reply1 = await call((done) => concessionsConcessionStandApprovalClient.rejectConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      guest_list: {
        billing_address: {
          postal_code: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.postal_code ?? ''),
        },
      },
      result: { status: (reply1?.result?.status ?? '') },
    },
    {
      guest_list: { guest_list_id: (request?.guest_list_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Terminates a guest list.
 */
async function terminateGuestList(request) {
  validateContext(request.context);
  if (!request.guest_list_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
  }
  await store.record(request.context.tenant_id, 'TerminateGuestList', request);
  logger.info('terminate_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
    },
  };
  const method1 = 'findBoxOfficeSales';
  const reply1 = await call((done) => boxofficeBoxOfficeSaleQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { status: (reply1?.result?.status ?? '') },
      result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
    },
    {
      guest_list: { guest_list_id: (request?.guest_list_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

module.exports = { activateGuestList, expireGuestList, getGuestListLifecycle, reinstateGuestList, renewGuestList, scheduleGuestListExpiry, suspendGuestList, terminateGuestList };
