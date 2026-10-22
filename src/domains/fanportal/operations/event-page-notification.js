'use strict';

/**
 * Tells the people who follow an event page what changed. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const clients = require('../../../platform/clients');
const {
  concessionsConcessionStandWorkflowClient,
  concessionsMenuItemWorkflowClient,
  devicesManagedDeviceCalculationClient,
  identityUserValidationClient,
  maintenanceWorkOrderQueryClient,
  notificationsNotificationQueryClient,
  seatingSeatAssignmentReportingClient,
  sponsorshipsActivationApprovalClient,
  venuesVenuePolicySyncClient,
  venuesVenueQueryClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Returns an event page.
 */
async function getEventPagePreferences(request) {
  validateContext(request.context);
  if (!request.event_page_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'event_page_id is required');
  }
  await store.record(request.context.tenant_id, 'GetEventPagePreferences', request);
  logger.info('get_event_page_preferences', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { event_page: { event_page_id: (request?.event_page_id ?? '') } };
}

/**
 * Lists event pages a page at a time.
 */
async function listEventPageSubscriptions(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListEventPageSubscriptions', request);
  logger.info('list_event_page_subscriptions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: { email: (request?.context?.actor?.email ?? '') },
      locale: (request?.context?.locale ?? ''),
    },
    managed_device: {
      address: { region: (request?.filter?.region ?? '') },
      owner: { user_id: (request?.context?.actor?.user_id ?? '') },
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    devicesManagedDeviceCalculationClient.explainManagedDeviceCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    event_pages: [{
      lines: [{
        description: (reply1?.managed_device?.components?.primary?.description ?? ''),
      }],
    }],
  };
}

/**
 * Notifys an event page.
 */
async function notifyEventPageWatchers(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'NotifyEventPageWatchers', request);
  logger.info('notify_event_page_watchers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      notify: {
        email: (request?.details?.recipient?.email ?? ''),
        name: request?.details?.recipient?.name,
        phone: (request?.details?.recipient?.phone ?? ''),
      },
    },
  };
  const advanceMenuItemWorkflow1 = promisify(concessionsMenuItemWorkflowClient.advanceMenuItemWorkflow.bind(concessionsMenuItemWorkflowClient));
  const reply1 = await advanceMenuItemWorkflow1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    context: request?.context,
    details: {
      contact: {
        email: (reply1?.menu_item?.owner?.email ?? ''),
        name: request?.details?.recipient?.name,
      },
    },
    user: {
      primary_address: {
        recipient: ((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.recipient ?? ''),
      },
    },
  };
  const reply2 = await call((done) => identityUserValidationClient.explainUserDecision(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: {
        next_steps: (reply1?.outcome?.next_steps ?? []),
        reference_id: (reply1?.outcome?.reference_id ?? ''),
      },
    },
    {
      event_page: {
        number: ((reply2?.user?.identity_document === 'national_id' ? reply2?.user.national_id : undefined)?.number ?? ''),
      },
      outcome: { reference_id: (reply2?.outcome?.reference_id ?? '') },
    },
    { event_page: { event_page_id: (request?.event_page_id ?? '') } },
  );
}

/**
 * Sends an event page.
 */
async function sendEventPageTestNotice(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'SendEventPageTestNotice', request);
  logger.info('send_event_page_test_notice', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: { filters: { ...(request?.details?.variables ?? {}) } },
    reason: (request?.reason ?? ''),
  };
  const method1 = 'summarizeNotifications';
  const reply1 = await call((done) => notificationsNotificationQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      recipient: {
        name: request?.details?.recipient?.name,
        phone: (request?.details?.recipient?.phone ?? ''),
        preferred_channel: (request?.details?.recipient?.preferred_channel ?? ''),
      },
      variables: { ...(request?.details?.variables ?? {}) },
    },
  };
  const client2 = clients.registry.get('acme.seating.v1.SeatAssignmentReportingService');
  const reply2 = await call((done) => client2.shareSeatAssignmentReport(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    user_id: (request?.context?.actor?.user_id ?? ''),
  };
  const send3 = identityUserValidationClient.getUserRules.bind(identityUserValidationClient);
  const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.channel ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      result: { message: (reply1?.result?.message ?? '') },
    },
    {
      event_page: {
        audit: { created_by: (reply2?.seat_assignment?.audit?.created_by ?? '') },
      },
      result: { details: (reply2?.result?.details ?? {}) },
    },
    {
      event_page: { audit: { created_at: reply3?.user?.audit?.created_at } },
      result: { details: (reply3?.result?.details ?? {}) },
    },
    { event_page: { event_page_id: (request?.event_page_id ?? '') } },
  );
}

/**
 * Streams the changes of event pages as they happen.
 */
async function streamEventPageNotices(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'StreamEventPageNotices', request);
  logger.info('stream_event_page_notices', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    user: {
      contact: { preferred_channel: (request?.subscriber?.preferred_channel ?? '') },
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => identityUserValidationClient.previewUser(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    concession_stand: {
      address: { locality: (reply1?.user?.primary_address?.locality ?? '') },
      audit: { created_by: (reply1?.user?.audit?.created_by ?? '') },
    },
    context: input2?.context,
    details: { notify: { phone: (reply1?.user?.contact?.phone ?? '') } },
  };
  const reply2 = await call((done) => concessionsConcessionStandWorkflowClient.cancelConcessionStandWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.subscriber?.phone ?? '') });
  return assemble(
    {
      event_page: {
        billing_address: { postal_code: (reply1?.user?.primary_address?.postal_code ?? '') },
      },
    },
    {
      event_page: {
        billing_address: {
          lines: ((reply2?.concession_stand?.locator === 'address' ? reply2?.concession_stand.address : undefined)?.lines ?? []),
        },
      },
      result: { details: (reply2?.result?.details ?? {}) },
    },
    {
      event_page: { billing_address: { region: (request?.filter?.region ?? '') } },
    },
  );
}

/**
 * Subscribes an event page.
 */
async function subscribeEventPageUpdates(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'SubscribeEventPageUpdates', request);
  logger.info('subscribe_event_page_updates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { labels: { ...(request?.details?.variables ?? {}) } },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => maintenanceWorkOrderQueryClient.findWorkOrders(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    subscriber: {
      email: (request?.details?.recipient?.email ?? ''),
      preferred_channel: (request?.details?.recipient?.preferred_channel ?? ''),
    },
  };
  const messages2 = await collect(seatingSeatAssignmentReportingClient.streamSeatAssignmentMetrics(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  const request3 = {
    context: request?.context,
    venue_policy: { owner: { email: (request?.details?.recipient?.email ?? '') } },
  };
  const copied3 = structuredClone(request3);
  const reply3 = await call((done) => venuesVenuePolicySyncClient.reconcileVenuePolicies(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    },
    { outcome: { message: (reply3?.outcome?.message ?? '') } },
    { event_page: { event_page_id: (request?.event_page_id ?? '') } },
  );
}

/**
 * Unsubscribes an event page.
 */
async function unsubscribeEventPageUpdates(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'UnsubscribeEventPageUpdates', request);
  logger.info('unsubscribe_event_page_updates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.event_page_id ?? ''),
    },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => venuesVenueQueryClient.findVenues(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    activation: {
      requester: {
        email: (request?.details?.recipient?.email ?? ''),
        name: request?.details?.recipient?.name,
        phone: (request?.details?.recipient?.phone ?? ''),
        preferred_channel: (request?.details?.recipient?.preferred_channel ?? ''),
      },
    },
    context: request?.context,
  };
  const reply2 = await withRetry(() => call((done) => sponsorshipsActivationApprovalClient.delegateActivationApproval(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const request3 = {
    context: request?.context,
    subscriber: {
      email: (request?.details?.recipient?.email ?? ''),
      phone: (request?.details?.recipient?.phone ?? ''),
      preferred_channel: (reply2?.activation?.requester?.preferred_channel ?? ''),
    },
  };
  const messages3 = await collect(seatingSeatAssignmentReportingClient.streamSeatAssignmentMetrics(request3, metadataFor(request3.context), { deadline: deadline() }));
  const reply3 = messages3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      result: { details: (reply1?.result?.details ?? {}) },
    },
    {
      event_page: { audit: { created_at: reply2?.activation?.audit?.created_at } },
      outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) },
    },
    { event_page: { event_page_id: (request?.event_page_id ?? '') } },
  );
}

module.exports = { getEventPagePreferences, listEventPageSubscriptions, notifyEventPageWatchers, sendEventPageTestNotice, streamEventPageNotices, subscribeEventPageUpdates, unsubscribeEventPageUpdates };
