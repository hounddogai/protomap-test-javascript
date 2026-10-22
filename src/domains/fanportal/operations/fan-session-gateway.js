'use strict';

/**
 * The edge API that apps call for fan sessions. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionApprovalClient,
  admissionsAdmissionLifecycleClient,
  documentsDocumentQueryClient,
  documentsDocumentValidationClient,
  fanportalEventPageClient,
  fanportalEventPageQueryClient,
  fanportalFanSessionClient,
  fanportalFanSessionQueryClient,
  fanportalWishlistNotificationClient,
  fanportalWishlistQueryClient,
  metricsMetricSeriesQueryClient,
  metricsMetricSeriesValidationClient,
  performersPerformerContractWorkflowClient,
  ticketingTicketTransferApprovalClient,
  venuesVenueClient,
  venuesVenuePolicySyncClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Composes a fan session.
 */
async function composeFanSessionPage(request) {
  validateContext(request.context);
  if (request.fan_session) {
    await store.save(store.toFanSessionRecord(request.context.tenant_id, request.fan_session));
  }
  await store.record(request.context.tenant_id, 'ComposeFanSessionPage', request);
  logger.info('compose_fan_session_page', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.fan_session?.assignee?.email ?? ''),
        name: request?.fan_session?.requester?.name,
        phone: (request?.fan_session?.requester?.phone ?? ''),
        preferred_channel: (request?.fan_session?.requester?.preferred_channel ?? ''),
      },
    },
  };
  const reply1 = await withRetry(() => call((done) => fanportalWishlistNotificationClient.subscribeWishlistUpdates(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    venue_policy: {
      audit: {
        created_by: (reply1?.wishlist?.audit?.created_by ?? ''),
        etag: (reply1?.wishlist?.audit?.etag ?? ''),
        updated_at: reply1?.wishlist?.audit?.updated_at,
      },
    },
  };
  const task2 = deferred(() => call((done) => venuesVenuePolicySyncClient.resolveVenuePolicyConflict(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  const request3 = {
    context: request?.context,
    metric_series: {
      address: { recipient: (reply1?.wishlist?.billing_address?.recipient ?? '') },
      audit: {
        updated_at: request?.fan_session?.audit?.updated_at,
        updated_by: (reply1?.wishlist?.audit?.updated_by ?? ''),
      },
      owner: { display_name: (reply2?.venue_policy?.owner?.display_name ?? '') },
    },
  };
  const reply3 = await bus.request('acme.fanportal.v1.FanSessionGatewayService/ComposeFanSessionPage:2', request3);
  const request4 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.fan_session?.assignee?.email ?? ''),
        name: request?.fan_session?.requester?.name,
        preferred_channel: (request?.fan_session?.requester?.preferred_channel ?? ''),
      },
    },
  };
  const reply4 = await runInContext(request.context, () => call((done) => fanportalWishlistNotificationClient.notifyWishlistWatchers(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      fan_session: { audit: { etag: (reply1?.wishlist?.audit?.etag ?? '') } },
    },
    { result: { status: (reply2?.result?.status ?? '') } },
    {
      fan_session: {
        assignee: { display_name: (reply3?.metric_series?.owner?.display_name ?? '') },
      },
      result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
    },
    {
      fan_session: {
        audit: { etag: (reply4?.wishlist?.audit?.etag ?? '') },
        sla: { due_at: reply4?.wishlist?.due_at },
      },
    },
    {
      fan_session: {
        fan_session_id: (request?.fan_session_id ?? ''),
        status: (request?.fan_session?.status ?? ''),
      },
    },
  );
}

/**
 * Fetchs a fan session.
 */
async function fetchFanSessionView(request) {
  validateContext(request.context);
  if (request.fan_session) {
    await store.save(store.toFanSessionRecord(request.context.tenant_id, request.fan_session));
  }
  await store.record(request.context.tenant_id, 'FetchFanSessionView', request);
  logger.info('fetch_fan_session_view', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    subscriber: {
      email: (request?.fan_session?.assignee?.email ?? ''),
      preferred_channel: (request?.fan_session?.requester?.preferred_channel ?? ''),
    },
  };
  const messages1 = await collect(fanportalFanSessionClient.watchFanSessions(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    seat_map: {
      children: [{
        children: [{
          audit: { etag: (request?.fan_session?.audit?.etag ?? '') },
          manager: { display_name: (request?.fan_session?.assignee?.display_name ?? '') },
        }],
        manager: { email: (request?.fan_session?.requester?.email ?? '') },
      }],
    },
  };
  const reply2 = await call((done) => clients.seatingSeatMapApprovalClient.approveSeatMap(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.fan_session?.assignee?.email ?? ''),
        name: request?.fan_session?.requester?.name,
      },
    },
  };
  const reply3 = await new Promise((resolve, reject) => {
    fanportalWishlistNotificationClient.sendWishlistTestNotice(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      fan_session: { assignee: { email: (reply2?.seat_map?.manager?.email ?? '') } },
    },
    { result: { message: (reply3?.result?.message ?? '') } },
    {
      fan_session: {
        fan_session_id: (request?.fan_session_id ?? ''),
        status: (request?.fan_session?.status ?? ''),
      },
    },
  );
}

/**
 * Returns a fan session.
 */
async function getFanSessionSummary(request) {
  validateContext(request.context);
  if (!request.fan_session_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'fan_session_id is required');
  }
  await store.record(request.context.tenant_id, 'GetFanSessionSummary', request);
  logger.info('get_fan_session_summary', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { fan_session: { fan_session_id: (request?.fan_session_id ?? '') } };
}

/**
 * Refreshs a fan session.
 */
async function refreshFanSession(request) {
  validateContext(request.context);
  if (request.fan_session) {
    await store.save(store.toFanSessionRecord(request.context.tenant_id, request.fan_session));
  }
  await store.record(request.context.tenant_id, 'RefreshFanSession', request);
  logger.info('refresh_fan_session', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { created_after: request?.fan_session?.sla?.due_at },
  };
  const findDocuments1 = promisify(documentsDocumentQueryClient.findDocuments.bind(documentsDocumentQueryClient));
  const reply1 = await findDocuments1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    context: request?.context,
    details: {
      approver: { user_id: (request?.fan_session?.assignee?.user_id ?? '') },
    },
    ticket_transfer: { requester: { email: (request?.fan_session?.assignee?.email ?? '') } },
  };
  const reply2 = await call((done) => ticketingTicketTransferApprovalClient.delegateTicketTransferApproval(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    details: {
      notify: {
        email: (reply2?.ticket_transfer?.assignee?.email ?? ''),
        preferred_channel: (request?.fan_session?.requester?.preferred_channel ?? ''),
      },
    },
  };
  const method3 = 'scheduleAdmissionExpiry';
  const reply3 = await call((done) => admissionsAdmissionLifecycleClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    filter: {
      created_after: reply2?.ticket_transfer?.sla?.due_at,
      labels: { ...(reply1?.result?.details ?? {}) },
      owner_id: (request?.fan_session?.assignee?.user_id ?? ''),
    },
  };
  const client4 = clients.registry.get('acme.fanportal.v1.WishlistNotificationService');
  const reply4 = await call((done) => client4.listWishlistSubscriptions(request4, metadataFor(request4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    {
      fan_session: {
        sla: {
          paused_for: reply2?.ticket_transfer?.sla?.paused_for,
          policy_id: (reply2?.ticket_transfer?.sla?.policy_id ?? ''),
        },
      },
    },
    {
      fan_session: {
        assignee: { display_name: (reply3?.admission?.subject?.display_name ?? '') },
      },
      result: { details: (reply3?.result?.details ?? {}) },
    },
    { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
    {
      fan_session: {
        fan_session_id: (request?.fan_session_id ?? ''),
        priority: (request?.fan_session?.priority ?? 0),
      },
    },
  );
}

/**
 * Lists fan sessions a page at a time.
 */
async function searchFanSessionCatalog(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'SearchFanSessionCatalog', request);
  logger.info('search_fan_session_catalog', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { locale: (request?.context?.locale ?? '') },
    document: { billing_address: { region: (request?.filter?.region ?? '') } },
  };
  const send1 = documentsDocumentValidationClient.validateDocument.bind(documentsDocumentValidationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      owner_id: (request?.filter?.owner_id ?? ''),
      region: (request?.filter?.region ?? ''),
      statuses: [...(request?.filter?.statuses ?? [])],
    },
    page: { page_token: (request?.page?.page_token ?? '') },
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => fanportalFanSessionClient.searchFanSessions(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        status: (reply1?.outcome?.status ?? ''),
      },
    },
    {
      result: {
        reference_ids: (reply2?.result?.reference_ids ?? []),
        status: (reply2?.result?.status ?? ''),
      },
    },
  );
}

/**
 * Starts a fan session.
 */
async function startFanSessionCheckout(request) {
  validateContext(request.context);
  if (request.fan_session) {
    await store.save(store.toFanSessionRecord(request.context.tenant_id, request.fan_session));
  }
  await store.record(request.context.tenant_id, 'StartFanSessionCheckout', request);
  logger.info('start_fan_session_checkout', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = await Promise.all((request?.fan_session?.history ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      filter: {
        created_after: item?.changed_at,
        labels: { ...(request?.annotations ?? {}) },
        owner_id: ((request?.fan_session?.origin === 'customer' ? request?.fan_session.customer : undefined)?.wishlist_id ?? ''),
      },
    };
    const reply1 = await call((done) => metricsMetricSeriesQueryClient.countMetricSerieses(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return reply1;
  }));
  const reply1 = results1.at(-1) ?? {};
  const settled2 = await Promise.allSettled((request?.fan_session?.comments ?? []).map(async (item) => {
    const request2 = {
      context: request?.context,
      filter: {
        created_after: item?.created_at,
        labels: { ...(request?.fan_session?.custom_fields ?? {}) },
        owner_id: (item?.comment_id ?? ''),
      },
    };
    const reply2 = await call((done) => fanportalFanSessionClient.listFanSessions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return reply2;
  }));
  const results2 = settled2.map((outcome) => {
    if (outcome.status === 'rejected') throw outcome.reason;
    return outcome.value;
  });
  const reply2 = results2.at(-1) ?? {};
  const { context: context3, ...body3 } = request;
  const input3 = { ...body3, context: context3 };
  const request3 = {
    context: input3?.context,
    details: {
      recipient: {
        email: (input3?.details?.notify?.email ?? ''),
        name: input3?.fan_session?.requester?.name,
        phone: (input3?.details?.notify?.phone ?? ''),
      },
    },
  };
  const reply3 = await call((done) => fanportalWishlistNotificationClient.notifyWishlistWatchers(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      fan_session: { sla: { due_at: reply3?.wishlist?.due_at } },
      result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
    },
    {
      fan_session: {
        fan_session_id: (request?.fan_session_id ?? ''),
        subject: (request?.fan_session?.subject ?? ''),
      },
    },
  );
}

/**
 * Streams the changes of fan sessions as they happen.
 */
async function streamFanSessionUpdates(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'StreamFanSessionUpdates', request);
  logger.info('stream_fan_session_updates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue: {
      address: { region: (request?.filter?.region ?? '') },
      audit: { created_at: request?.filter?.created_after },
      owner: {
        email: (request?.subscriber?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => venuesVenueClient.updateVenue(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  let selected2_1;
  switch (reply1?.venue?.locator) {
    case 'address':
      selected2_1 = ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.region ?? '');
      break;
    default:
      selected2_1 = '';
  }
  const request2 = {
    context: request?.context,
    filter: { region: selected2_1 },
  };
  const reply2 = await call((done) => fanportalEventPageClient.searchEventPages(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.filter?.created_after,
      labels: { ...(request?.filter?.labels ?? {}) },
      query: (request?.filter?.query ?? ''),
      region: ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.region ?? ''),
    },
  };
  const merged3 = Object.assign({}, request3, { context: request3.context });
  const reply3 = await call((done) => fanportalEventPageQueryClient.suggestEventPages(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    performer_contract: {
      audit: {
        created_at: reply1?.venue?.audit?.created_at,
        created_by: (reply1?.venue?.audit?.created_by ?? ''),
      },
      counterparty: { venue_id: (reply1?.venue?.venue_id ?? '') },
    },
  };
  const reply4 = await withRetry(() => call((done) => performersPerformerContractWorkflowClient.resumePerformerContractWorkflow(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
  return assemble(
    { fan_session: { audit: { etag: (reply1?.venue?.audit?.etag ?? '') } } },
    { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
    { result: { message: (reply3?.result?.message ?? '') } },
    {
      fan_session: { audit: { created_at: reply4?.performer_contract?.audit?.created_at } },
    },
    {
      fan_session: {
        requester: {
          name: request?.subscriber?.name,
          phone: (request?.subscriber?.phone ?? ''),
        },
      },
    },
  );
}

/**
 * Submits a fan session.
 */
async function submitFanSessionForm(request) {
  validateContext(request.context);
  if (request.fan_session) {
    await store.save(store.toFanSessionRecord(request.context.tenant_id, request.fan_session));
  }
  await store.record(request.context.tenant_id, 'SubmitFanSessionForm', request);
  logger.info('submit_fan_session_form', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  let selected1_1;
  switch (request?.fan_session?.origin) {
    case 'customer':
      selected1_1 = ((request?.fan_session?.origin === 'customer' ? request?.fan_session.customer : undefined)?.display_name ?? '');
      break;
    default:
      selected1_1 = '';
  }
  const request1 = {
    admission: { subject: { display_name: selected1_1 } },
    context: request?.context,
    details: { approver: { email: (request?.fan_session?.requester?.email ?? '') } },
  };
  const reply1 = await call((done) => admissionsAdmissionApprovalClient.submitAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.fan_session?.assignee?.email ?? ''),
        name: request?.fan_session?.requester?.name,
        preferred_channel: (request?.fan_session?.requester?.preferred_channel ?? ''),
      },
    },
    wishlist_id: ((request?.fan_session?.origin === 'customer' ? request?.fan_session.customer : undefined)?.wishlist_id ?? ''),
  };
  const reply2 = await bus.request('acme.fanportal.v1.FanSessionGatewayService/SubmitFanSessionForm:1', request2);
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.outcome?.message ?? '') } },
    {
      fan_session: { audit: { etag: (reply2?.wishlist?.audit?.etag ?? '') } },
    },
    {
      fan_session: {
        fan_session_id: (request?.fan_session_id ?? ''),
        status: (request?.fan_session?.status ?? ''),
      },
    },
  );
}

/**
 * Tracks a fan session.
 */
async function trackFanSession(request) {
  validateContext(request.context);
  if (request.fan_session) {
    await store.save(store.toFanSessionRecord(request.context.tenant_id, request.fan_session));
  }
  await store.record(request.context.tenant_id, 'TrackFanSession', request);
  logger.info('track_fan_session', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = [];
  for (const item of (request?.fan_session?.comments ?? [])) {
    const request1 = {
      context: request?.context,
      details: {
        recipient: {
          email: (item?.author_email ?? ''),
          name: request?.fan_session?.requester?.name,
          phone: (request?.fan_session?.requester?.phone ?? ''),
          preferred_channel: (request?.fan_session?.requester?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await call((done) => fanportalWishlistNotificationClient.unsubscribeWishlistUpdates(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    results1.push(reply1);
  }
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    fan_session: {
      customer: {
        wishlist_id: ((request?.fan_session?.origin === 'customer' ? request?.fan_session.customer : undefined)?.wishlist_id ?? ''),
      },
      requester: { email: (request?.fan_session?.assignee?.email ?? '') },
      sla: { paused_for: request?.fan_session?.sla?.paused_for },
    },
  };
  const reply2 = await runInContext(request.context, () => call((done) => fanportalFanSessionClient.updateFanSession(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  let selected3_1;
  switch (reply2?.fan_session?.origin) {
    case 'customer':
      selected3_1 = ((reply2?.fan_session?.origin === 'customer' ? reply2?.fan_session.customer : undefined)?.wishlist_id ?? '');
      break;
    default:
      selected3_1 = '';
  }
  const request3 = {
    context: request?.context,
    reason: (request?.reason ?? ''),
    wishlist_id: selected3_1,
  };
  const reply3 = await call((done) => fanportalWishlistQueryClient.summarizeWishlists(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.session_id ?? '') });
  return assemble(
    {
      fan_session: {
        assignee: {
          display_name: ((reply2?.fan_session?.origin === 'customer' ? reply2?.fan_session.customer : undefined)?.display_name ?? ''),
        },
      },
    },
    { result: { message: (reply3?.result?.message ?? '') } },
    {
      fan_session: {
        fan_session_id: (request?.fan_session_id ?? ''),
        subject: (request?.fan_session?.subject ?? ''),
      },
    },
  );
}

/**
 * Takes a stream of fan sessions and answers once at the end.
 */
async function uploadFanSessionAttachments(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'UploadFanSessionAttachments', request);
  logger.info('upload_fan_session_attachments', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: {
        email: ((request?.chunk === 'record' ? request.record : undefined)?.assignee?.email ?? ''),
      },
    },
    document: {
      audit: {
        created_at: (request?.chunk === 'record' ? request.record : undefined)?.audit?.created_at,
        created_by: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.created_by ?? ''),
      },
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    documentsDocumentValidationClient.checkDocumentEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    admission: {
      source: ((request?.chunk === 'header' ? request.header : undefined)?.source ?? ''),
      subject: {
        display_name: ((request?.chunk === 'record' ? request.record : undefined)?.assignee?.display_name ?? ''),
      },
    },
    context: request?.context,
    details: {
      approver: {
        email: ((request?.chunk === 'record' ? request.record : undefined)?.requester?.email ?? ''),
      },
    },
  };
  const commentAdmissionApproval2 = promisify(admissionsAdmissionApprovalClient.commentAdmissionApproval.bind(admissionsAdmissionApprovalClient));
  const reply2 = await commentAdmissionApproval2(request2, metadataFor(request2.context), { deadline: deadline() });
  const request3 = {
    context: request?.context,
    filter: {
      created_after: reply2?.outcome?.effective_at,
      region: (reply1?.document?.billing_address?.region ?? ''),
    },
  };
  const reply3 = await call((done) => fanportalFanSessionQueryClient.browseFanSessions(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? '') });
  return assemble(
    { result: { status: (reply1?.outcome?.status ?? '') } },
    { result: { correlation_id: (reply2?.admission?.correlation_id ?? '') } },
    { result: { message: (reply3?.result?.message ?? '') } },
  );
}

module.exports = { composeFanSessionPage, fetchFanSessionView, getFanSessionSummary, refreshFanSession, searchFanSessionCatalog, startFanSessionCheckout, streamFanSessionUpdates, submitFanSessionForm, trackFanSession, uploadFanSessionAttachments };
