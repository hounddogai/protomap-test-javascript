'use strict';

/**
 * Manages event pages: creates, reads, changes, and lists them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { clientStream, serverStream, unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of EventPageService, over the dependencies the fanportal team is given.
 */
function createEventPageHandlers({ store, clients, bus, logger }) {
  /**
   * Takes a stream of event pages and answers once at the end.
   */
  async function batchCreateEventPages(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'BatchCreateEventPages', request);
    logger.info('batch_create_event_pages', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        region: ((request?.chunk === 'record' ? request.record : undefined)?.billing_address?.region ?? ''),
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.payrollPayRunQueryClient.findPayRuns(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      details: {
        recipient: {
          name: (input2?.chunk === 'header' ? input2.header : undefined)?.submitted_by?.name,
          preferred_channel: ((input2?.chunk === 'header' ? input2.header : undefined)?.submitted_by?.preferred_channel ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.fanportalEventPageNotificationClient.notifyEventPageWatchers(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    let selected3_1;
    switch (request?.chunk) {
      case 'record':
        selected3_1 = (request?.chunk === 'record' ? request.record : undefined)?.audit?.created_at;
        break;
      default:
        selected3_1 = null;
    }
    const request3 = {
      context: request?.context,
      tour: { audit: { created_at: selected3_1 } },
    };
    const reply3 = await call((done) => clients.toursTourWorkflowClient.startTourWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          message: (reply1?.result?.message ?? ''),
        },
      },
      {
        rejections: [{ event_page_id: (reply2?.event_page?.event_page_id ?? '') }],
      },
      { result: { details: (reply3?.result?.details ?? {}) } },
    );
  }

  /**
   * Returns several event pages at once.
   */
  async function batchGetEventPages(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'BatchGetEventPages', request);
    logger.info('batch_get_event_pages', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.context?.baggage ?? {}) },
      context: request?.context,
      details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.venuesVenuePolicyNotificationClient.unsubscribeVenuePolicyUpdates(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    return { result: { details: (reply1?.result?.details ?? {}) } };
  }

  /**
   * Creates an event page.
   */
  async function cloneEventPage(request) {
    validateContext(request.context);
    if (request.event_page) {
      await store.save(store.toEventPageRecord(request.context.tenant_id, request.event_page));
    }
    await store.record(request.context.tenant_id, 'CloneEventPage', request);
    logger.info('clone_event_page', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      event_page_id: (request?.event_page?.event_page_id ?? ''),
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.fanportalEventPageQueryClient.lookupEventPage(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      event_page_id: (request?.event_page?.event_page_id ?? ''),
    };
    const reply2 = await withRetry(() => call((done) => clients.fanportalEventPageQueryClient.peekEventPage(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      purchase_requisition: { audit: { etag: (request?.event_page?.audit?.etag ?? '') } },
    };
    const task3 = deferred(() => call((done) => clients.procurementPurchaseRequisitionClient.clonePurchaseRequisition(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    const request4 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: { updated_at: request?.event_page?.audit?.updated_at },
            location: {
              country_code: (reply3?.purchase_requisition?.billing_address?.country_code ?? ''),
              recipient: (reply1?.event_page?.billing_address?.recipient ?? ''),
            },
          }],
        }],
      },
    };
    const reply4 = await bus.request('acme.fanportal.v1.EventPageService/CloneEventPage:3', request4);
    logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      { event_page: { total: reply1?.event_page?.total } },
      {
        event_page: {
          billing_address: { lines: (reply2?.event_page?.billing_address?.lines ?? []) },
        },
      },
      { event_page: { total: reply3?.purchase_requisition?.total } },
      {
        result: {
          details: (reply4?.result?.details ?? {}),
          message: (reply4?.result?.message ?? ''),
        },
      },
      {
        event_page: {
          issued_at: request?.event_page?.issued_at,
          status: (request?.event_page?.status ?? ''),
        },
      },
    );
  }

  /**
   * Creates an event page.
   */
  async function createEventPage(request) {
    validateContext(request.context);
    if (request.event_page) {
      await store.save(store.toEventPageRecord(request.context.tenant_id, request.event_page));
    }
    await store.record(request.context.tenant_id, 'CreateEventPage', request);
    logger.info('create_event_page', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_4;
    switch (request?.event_page?.settlement) {
      case 'card':
        selected1_4 = ((request?.event_page?.settlement === 'card' ? request?.event_page.card : undefined)?.last4 ?? '');
        break;
      default:
        selected1_4 = '';
    }
    const request1 = {
      context: request?.context,
      sponsorship_deal: {
        audit: { created_at: request?.event_page?.audit?.created_at },
        billing_address: {
          lines: [...(request?.event_page?.billing_address?.lines ?? [])],
          postal_code: (request?.event_page?.billing_address?.postal_code ?? ''),
        },
        card: { last4: selected1_4 },
      },
    };
    const reply1 = await call((done) => clients.sponsorshipsSponsorshipDealApprovalClient.delegateSponsorshipDealApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: { region: (reply1?.sponsorship_deal?.billing_address?.region ?? '') },
    };
    const reply2 = await call((done) => clients.fanportalEventPageNotificationClient.listEventPageSubscriptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      tour_budget: {
        audit: { etag: (reply1?.sponsorship_deal?.audit?.etag ?? '') },
        card: {
          last4: ((reply1?.sponsorship_deal?.settlement === 'card' ? reply1?.sponsorship_deal.card : undefined)?.last4 ?? ''),
        },
      },
    };
    const reply3 = await new Promise((resolve, reject) => {
      clients.toursTourBudgetWorkflowClient.retryTourBudgetWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      { event_page: { number: (reply1?.sponsorship_deal?.number ?? '') } },
      { result: { message: (reply2?.result?.message ?? '') } },
      { result: { details: (reply3?.result?.details ?? {}) } },
      {
        event_page: {
          due_at: request?.event_page?.due_at,
          event_page_id: (request?.event_page?.event_page_id ?? ''),
        },
      },
    );
  }

  /**
   * Deletes an event page.
   */
  async function deleteEventPage(request) {
    validateContext(request.context);
    if (!request.event_page_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'event_page_id is required');
    }
    await store.record(request.context.tenant_id, 'DeleteEventPage', request);
    logger.info('delete_event_page', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: { etag: (request?.etag ?? '') },
        block: { reason: (request?.reason ?? '') },
      },
    };
    const repairVenuePolicy1 = promisify(clients.venuesVenuePolicyAdminClient.repairVenuePolicy.bind(clients.venuesVenuePolicyAdminClient));
    const reply1 = await repairVenuePolicy1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.venue_policy?.audit?.created_at,
        labels: { ...(reply1?.result?.details ?? {}) },
      },
    };
    const reply2 = await call((done) => clients.fanportalFanSessionQueryClient.countFanSessions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      annotations: { ...(reply1?.result?.details ?? {}) },
      context: request?.context,
      details: { recipient: { email: (reply1?.venue_policy?.owner?.email ?? '') } },
      reason: ((reply1?.venue_policy?.action === 'block' ? reply1?.venue_policy.block : undefined)?.reason ?? ''),
    };
    const method3 = 'subscribeWishlistUpdates';
    const reply3 = await call((done) => clients.fanportalWishlistNotificationClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
    let selected4_2;
    switch (reply1?.venue_policy?.action) {
      case 'block':
        selected4_2 = ((reply1?.venue_policy?.action === 'block' ? reply1?.venue_policy.block : undefined)?.reason ?? '');
        break;
      default:
        selected4_2 = '';
    }
    const request4 = {
      context: request?.context,
      details: {
        recipient: { email: (reply1?.venue_policy?.owner?.email ?? '') },
        variables: { ...(reply1?.result?.details ?? {}) },
      },
      event_page_id: (request?.event_page_id ?? ''),
      reason: selected4_2,
    };
    const reply4 = await call((done) => clients.fanportalEventPageNotificationClient.subscribeEventPageUpdates(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        result: {
          correlation_id: (reply3?.result?.correlation_id ?? ''),
          reference_ids: (reply3?.result?.reference_ids ?? []),
        },
      },
      { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
    );
  }

  /**
   * Returns an event page.
   */
  async function getEventPage(request) {
    validateContext(request.context);
    if (!request.event_page_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'event_page_id is required');
    }
    await store.record(request.context.tenant_id, 'GetEventPage', request);
    logger.info('get_event_page', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.event_page_id ?? '') },
      since: request?.context?.deadline,
      subscriber: { email: (request?.context?.actor?.email ?? '') },
    };
    const messages1 = await collect(clients.fanportalEventPageNotificationClient.streamEventPageNotices(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    return { event_page: { event_page_id: (request?.event_page_id ?? '') } };
  }

  /**
   * Lists event pages a page at a time.
   */
  async function searchEventPages(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'SearchEventPages', request);
    logger.info('search_event_pages', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
    };
    const send1 = clients.fanportalWishlistNotificationClient.unsubscribeWishlistUpdates.bind(clients.fanportalWishlistNotificationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      event_pages: [{
        counterparty: { number: (reply1?.wishlist?.number ?? '') },
        number: (reply1?.wishlist?.number ?? ''),
      }],
    };
  }

  /**
   * Changes an event page.
   */
  async function updateEventPage(request) {
    validateContext(request.context);
    if (request.event_page) {
      await store.save(store.toEventPageRecord(request.context.tenant_id, request.event_page));
    }
    await store.record(request.context.tenant_id, 'UpdateEventPage', request);
    logger.info('update_event_page', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsorship_deal: {
        bank: {
          account_holder: ((request?.event_page?.settlement === 'bank' ? request?.event_page.bank : undefined)?.account_holder ?? ''),
        },
        billing_address: { locality: (request?.event_page?.billing_address?.locality ?? '') },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.sponsorshipsSponsorshipDealApprovalClient.commentSponsorshipDealApproval(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      filter: {
        created_after: reply1?.sponsorship_deal?.audit?.updated_at,
        labels: { ...(reply1?.sponsorship_deal?.metadata ?? {}) },
        owner_id: (reply1?.sponsorship_deal?.counterparty?.sponsor_id ?? ''),
        region: (input2?.event_page?.billing_address?.region ?? ''),
      },
    };
    const reply2 = await call((done) => clients.fanportalEventPageQueryClient.browseEventPages(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.event_page?.audit?.etag ?? '') });
    return assemble(
      {
        event_page: {
          billing_address: {
            postal_code: (reply1?.sponsorship_deal?.billing_address?.postal_code ?? ''),
          },
        },
      },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        event_page: {
          counterparty: request?.event_page?.counterparty,
          metadata: (request?.event_page?.metadata ?? {}),
        },
      },
    );
  }

  /**
   * Streams the changes of event pages as they happen.
   */
  async function watchEventPages(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'WatchEventPages', request);
    logger.info('watch_event_pages', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.filter?.labels ?? {}) },
        owner_id: (request?.filter?.owner_id ?? ''),
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.fanportalFanSessionClient.searchFanSessions(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        notify: {
          name: request?.subscriber?.name,
          preferred_channel: (request?.subscriber?.preferred_channel ?? ''),
        },
      },
      seat_map: {
        children: [{
          children: [{ manager: { email: (request?.subscriber?.email ?? '') } }],
          location: { region: (request?.filter?.region ?? '') },
        }],
      },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.seatingSeatMapWorkflowClient.startSeatMapWorkflow(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      user: {
        audit: {
          created_at: reply2?.seat_map?.audit?.created_at,
          updated_at: reply2?.seat_map?.audit?.updated_at,
        },
        primary_address: { lines: [...(reply2?.seat_map?.location?.lines ?? [])] },
        relationship_manager: { display_name: (reply2?.seat_map?.manager?.display_name ?? '') },
      },
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => clients.identityUserClient.cloneUser(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      filter: { region: (reply2?.seat_map?.location?.region ?? '') },
      subscriber: { email: (reply2?.seat_map?.manager?.email ?? '') },
    };
    const messages4 = await collect(clients.fanportalFanSessionClient.watchFanSessions(request4, metadataFor(request4.context), { deadline: deadline() }));
    const reply4 = messages4.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.subscriber?.phone ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          details: (reply1?.result?.details ?? {}),
        },
      },
      {
        event_page: {
          billing_address: { recipient: (reply2?.seat_map?.location?.recipient ?? '') },
        },
      },
      {
        event_page: {
          audit: { created_at: reply3?.user?.audit?.created_at },
          billing_address: { locality: (reply3?.user?.primary_address?.locality ?? '') },
        },
      },
      {
        event_page: { billing_address: { region: (request?.filter?.region ?? '') } },
      },
    );
  }

  return {
    batchCreateEventPages: clientStream(batchCreateEventPages, () => ({})),
    batchGetEventPages: unary(batchGetEventPages),
    cloneEventPage: unary(cloneEventPage),
    createEventPage: unary(createEventPage),
    deleteEventPage: unary(deleteEventPage),
    getEventPage: unary(getEventPage),
    searchEventPages: unary(searchEventPages),
    updateEventPage: unary(updateEventPage),
    watchEventPages: serverStream(watchEventPages),
  };
}

module.exports = { createEventPageHandlers };
