'use strict';

/**
 * Moves a menu item between the states of its lifecycle.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { upload } = require('../../../platform/streams');

/**
 * The handlers of MenuItemLifecycleService, over the dependencies the concessions team is given.
 */
function createMenuItemLifecycleHandlers({ store, clients, bus, logger }) {
  /**
   * Activates a menu item.
   */
  async function activateMenuItem(request) {
    validateContext(request.context);
    if (!request.menu_item_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
    }
    await store.record(request.context.tenant_id, 'ActivateMenuItem', request);
    logger.info('activate_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{ manager: { email: (request?.details?.notify_contact?.email ?? '') } }],
        }],
      },
    };
    const reply1 = await call((done) => clients.venuesVenueSectionValidationClient.lintVenueSections(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: {
          owner: { display_name: (reply1?.venue_section?.manager?.display_name ?? '') },
        },
      },
      {
        menu_item: { menu_item_id: (request?.menu_item_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Expires a menu item.
   */
  async function expireMenuItem(request) {
    validateContext(request.context);
    if (!request.menu_item_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
    }
    await store.record(request.context.tenant_id, 'ExpireMenuItem', request);
    logger.info('expire_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: { owner: { user_id: (request?.details?.new_owner?.user_id ?? '') } },
    };
    const method1 = 'simulateMenuItem';
    const reply1 = await call((done) => clients.concessionsMenuItemCalculationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: { dimensions: { weight: (reply1?.menu_item?.dimensions?.weight ?? 0) } },
      },
      {
        menu_item: { menu_item_id: (request?.menu_item_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Returns a menu item.
   */
  async function getMenuItemLifecycle(request) {
    validateContext(request.context);
    if (!request.menu_item_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
    }
    await store.record(request.context.tenant_id, 'GetMenuItemLifecycle', request);
    logger.info('get_menu_item_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { menu_item: { menu_item_id: (request?.menu_item_id ?? '') } };
  }

  /**
   * Reinstates a menu item.
   */
  async function reinstateMenuItem(request) {
    validateContext(request.context);
    if (!request.menu_item_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
    }
    await store.record(request.context.tenant_id, 'ReinstateMenuItem', request);
    logger.info('reinstate_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const client1 = clients.registry.get('acme.authorization.v1.AccessPolicyService');
    const reply1 = await call((done) => client1.batchGetAccessPolicies(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (request?.menu_item_id ?? ''),
      },
    };
    const send2 = clients.venuesVenueNotificationClient.listVenueSubscriptions.bind(clients.venuesVenueNotificationClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      { outcome: { status: (reply2?.result?.status ?? '') } },
      {
        menu_item: { menu_item_id: (request?.menu_item_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Renews a menu item.
   */
  async function renewMenuItem(request) {
    validateContext(request.context);
    if (!request.menu_item_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
    }
    await store.record(request.context.tenant_id, 'RenewMenuItem', request);
    logger.info('renew_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.details?.new_owner?.user_id ?? '') },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.authorizationAccessPolicyClient.listAccessPolicies(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      annotations: { ...(input2?.annotations ?? {}) },
      context: input2?.context,
      lineup: {
        counterparty: { display_name: (input2?.details?.new_owner?.display_name ?? '') },
      },
      reason: (input2?.reason ?? ''),
    };
    const reply2 = await call((done) => clients.showsLineupCalculationClient.recalculateLineups(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      broadcast: {
        participants: [{
          contact: {
            preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
          },
        }],
      },
      context: request?.context,
      details: {
        approver: { display_name: (request?.details?.new_owner?.display_name ?? '') },
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => clients.broadcastsBroadcastApprovalClient.approveBroadcast(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.notify_contact?.phone ?? '') });
    return assemble(
      { outcome: { status: (reply1?.result?.status ?? '') } },
      { result: { message: (reply2?.result?.message ?? '') } },
      { result: { correlation_id: (reply3?.result?.correlation_id ?? '') } },
      {
        menu_item: { menu_item_id: (request?.menu_item_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Schedules a menu item.
   */
  async function scheduleMenuItemExpiry(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await store.record(request.context.tenant_id, 'ScheduleMenuItemExpiry', request);
    logger.info('schedule_menu_item_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.menu_item?.locator) {
      case 'address':
        selected1_1 = ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.region ?? '');
        break;
      default:
        selected1_1 = '';
    }
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.menu_item?.audit?.updated_at,
        owner_id: (request?.menu_item?.menu_item_id ?? ''),
        region: selected1_1,
      },
    };
    const reply1 = await call((done) => clients.fuelFuelCardClient.listFuelCards(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      header: {
        source: (request?.context?.actor?.user_id ?? ''),
        submitted_by: request?.details?.notify,
      },
    };
    const reply2 = await upload(
      (done) => clients.venuesVenueClient.batchCreateVenues(metadataFor(request2.context), { deadline: deadline() }, done),
      [request2],
    );
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        menu_item: {
          menu_item_id: (request?.menu_item_id ?? ''),
          name: (request?.menu_item?.name ?? ''),
        },
      },
    );
  }

  /**
   * Suspends a menu item.
   */
  async function suspendMenuItem(request) {
    validateContext(request.context);
    if (!request.menu_item_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
    }
    await store.record(request.context.tenant_id, 'SuspendMenuItem', request);
    logger.info('suspend_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          email: (request?.details?.notify_contact?.email ?? ''),
          name: request?.details?.notify_contact?.name,
          phone: (request?.details?.notify_contact?.phone ?? ''),
        },
      },
      sponsorship_deal: { counterparty: { email: (request?.details?.new_owner?.email ?? '') } },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.sponsorshipsSponsorshipDealWorkflowClient.resumeSponsorshipDealWorkflow(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { email: (request?.details?.notify_contact?.email ?? '') } },
      menu_item: {
        owner: {
          email: (request?.details?.new_owner?.email ?? ''),
          user_id: (request?.details?.new_owner?.user_id ?? ''),
        },
      },
    };
    const reply2 = await withRetry(() => call((done) => clients.concessionsMenuItemValidationClient.previewMenuItem(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.outcome?.status ?? '') } },
      {
        menu_item: { category_path: (reply2?.menu_item?.category_path ?? []) },
      },
      {
        menu_item: { menu_item_id: (request?.menu_item_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Terminates a menu item.
   */
  async function terminateMenuItem(request) {
    validateContext(request.context);
    if (!request.menu_item_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
    }
    await store.record(request.context.tenant_id, 'TerminateMenuItem', request);
    logger.info('terminate_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          email: (request?.details?.notify_contact?.email ?? ''),
          phone: (request?.details?.notify_contact?.phone ?? ''),
        },
      },
      reason: (request?.reason ?? ''),
      stream_key: { contact: { email: (request?.details?.new_owner?.email ?? '') } },
    };
    const task1 = deferred(() => call((done) => clients.broadcastsStreamKeyWorkflowClient.advanceStreamKeyWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const reply2 = await bus.request('acme.concessions.v1.MenuItemLifecycleService/TerminateMenuItem:1', request2);
    logger.info('local_audit', { value: (request?.details?.notify_contact?.preferred_channel ?? '') });
    return assemble(
      { result: { status: (reply1?.outcome?.status ?? '') } },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      {
        menu_item: { menu_item_id: (request?.menu_item_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  return {
    activateMenuItem: unary(activateMenuItem),
    expireMenuItem: unary(expireMenuItem),
    getMenuItemLifecycle: unary(getMenuItemLifecycle),
    reinstateMenuItem: unary(reinstateMenuItem),
    renewMenuItem: unary(renewMenuItem),
    scheduleMenuItemExpiry: unary(scheduleMenuItemExpiry),
    suspendMenuItem: unary(suspendMenuItem),
    terminateMenuItem: unary(terminateMenuItem),
  };
}

module.exports = { createMenuItemLifecycleHandlers };
