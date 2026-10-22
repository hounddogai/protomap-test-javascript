'use strict';

/**
 * Routes menu items through approval, with escalation and delegation. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  broadcastsReplayClipApprovalClient,
  concessionsConcessionOrderApprovalClient,
  concessionsConcessionOrderCalculationClient,
  concessionsConcessionOrderReportingClient,
  concessionsMenuItemReportingClient,
  concessionsMenuItemValidationClient,
  configurationConfigSetQueryClient,
  featureflagsFeatureFlagClient,
  identityUserValidationClient,
  notificationsNotificationQueryClient,
  ordersOrderClient,
  promotionsPromotionClient,
  sponsorshipsSponsorApprovalClient,
  sponsorshipsSponsorshipDealApprovalClient,
  venuesVenuePolicySyncClient,
  venuesVenueSectionValidationClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Approves a menu item.
 */
async function approveMenuItem(request) {
  validateContext(request.context);
  if (request.menu_item) {
    await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
  }
  await store.record(request.context.tenant_id, 'ApproveMenuItem', request);
  logger.info('approve_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  let selected1_1;
  switch (request?.menu_item?.locator) {
    case 'address':
      selected1_1 = ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.locality ?? '');
      break;
    default:
      selected1_1 = '';
  }
  let selected1_2;
  switch (request?.menu_item?.locator) {
    case 'address':
      selected1_2 = ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.recipient ?? '');
      break;
    default:
      selected1_2 = '';
  }
  const request1 = {
    context: request?.context,
    user: {
      audit: { updated_at: request?.menu_item?.audit?.updated_at },
      primary_address: {
        locality: selected1_1,
        recipient: selected1_2,
      },
      relationship_manager: { email: (request?.details?.approver?.email ?? '') },
    },
  };
  const reply1 = await call((done) => identityUserValidationClient.validateUser(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      menu_item: { audit: { updated_by: (reply1?.user?.audit?.updated_by ?? '') } },
    },
    {
      menu_item: {
        menu_item_id: (request?.menu_item_id ?? ''),
        status: (request?.menu_item?.status ?? ''),
      },
    },
  );
}

/**
 * Comments a menu item.
 */
async function commentMenuItemApproval(request) {
  validateContext(request.context);
  if (request.menu_item) {
    await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
  }
  await store.record(request.context.tenant_id, 'CommentMenuItemApproval', request);
  logger.info('comment_menu_item_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    concession_order: {
      audit: { updated_by: (request?.menu_item?.audit?.updated_by ?? '') },
      billing_address: {
        locality: ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.locality ?? ''),
      },
      counterparty: { display_name: (request?.details?.approver?.display_name ?? '') },
      lines: [{
        description: (request?.menu_item?.components?.primary?.description ?? ''),
      }],
    },
    context: request?.context,
  };
  const method1 = 'recalculateConcessionOrders';
  const reply1 = await call((done) => concessionsConcessionOrderCalculationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    venue_policy: { owner: { email: (request?.details?.approver?.email ?? '') } },
  };
  const client2 = clients.registry.get('acme.venues.v1.VenuePolicySyncService');
  const reply2 = await call((done) => client2.syncVenuePolicies(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    venue_section: {
      children: [{
        children: [{
          audit: { updated_by: (request?.menu_item?.audit?.updated_by ?? '') },
          location: {
            locality: (reply1?.concession_order?.billing_address?.locality ?? ''),
            region: (reply1?.concession_order?.billing_address?.region ?? ''),
          },
          manager: { user_id: (request?.details?.approver?.user_id ?? '') },
        }],
      }],
    },
  };
  const send3 = venuesVenueSectionValidationClient.normalizeVenueSection.bind(venuesVenueSectionValidationClient);
  const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      menu_item: { audit: { updated_at: reply1?.concession_order?.audit?.updated_at } },
    },
    { outcome: { effective_at: reply2?.outcome?.effective_at } },
    {
      menu_item: { owner: { user_id: (reply3?.venue_section?.manager?.user_id ?? '') } },
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
 * Delegates a menu item.
 */
async function delegateMenuItemApproval(request) {
  validateContext(request.context);
  if (request.menu_item) {
    await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
  }
  await store.record(request.context.tenant_id, 'DelegateMenuItemApproval', request);
  logger.info('delegate_menu_item_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    menu_item: {
      address: {
        lines: [...((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.lines ?? [])],
      },
      owner: {
        email: (request?.details?.approver?.email ?? ''),
        user_id: (request?.menu_item?.owner?.user_id ?? ''),
      },
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => concessionsMenuItemValidationClient.validateMenuItem(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    context: input2?.context,
    filter: {
      region: ((input2?.menu_item?.locator === 'address' ? input2?.menu_item.address : undefined)?.region ?? ''),
    },
  };
  const reply2 = await call((done) => concessionsMenuItemReportingClient.countMenuItemTotals(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      menu_item: {
        components: { primary: {
          component_id: (reply1?.menu_item?.components?.primary?.component_id ?? ''),
        } },
      },
    },
    {
      outcome: { status: (reply2?.result?.status ?? '') },
      result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
    },
    {
      menu_item: {
        code: (request?.menu_item?.code ?? ''),
        menu_item_id: (request?.menu_item_id ?? ''),
      },
    },
  );
}

/**
 * Escalates a menu item.
 */
async function escalateMenuItem(request) {
  validateContext(request.context);
  if (request.menu_item) {
    await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
  }
  await store.record(request.context.tenant_id, 'EscalateMenuItem', request);
  logger.info('escalate_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
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
      created_after: request?.details?.effective_at,
      region: selected1_1,
    },
  };
  const reply1 = await call((done) => ordersOrderClient.listOrders(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    concession_order: {
      audit: { etag: (request?.menu_item?.audit?.etag ?? '') },
      billing_address: {
        lines: [...((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.lines ?? [])],
      },
    },
    context: request?.context,
    details: {
      approver: { display_name: (request?.details?.approver?.display_name ?? '') },
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => concessionsConcessionOrderApprovalClient.escalateConcessionOrder(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    feature_flag: {
      audit: {
        created_by: (request?.menu_item?.audit?.created_by ?? ''),
        updated_by: (request?.menu_item?.audit?.updated_by ?? ''),
      },
      owner: { user_id: (request?.menu_item?.owner?.user_id ?? '') },
    },
  };
  const merged3 = Object.assign({}, request3, { context: request3.context });
  const reply3 = await call((done) => featureflagsFeatureFlagClient.createFeatureFlag(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.result?.status ?? '') } },
    {
      menu_item: {
        audit: { updated_by: (reply2?.concession_order?.audit?.updated_by ?? '') },
      },
      result: { message: (reply2?.outcome?.message ?? '') },
    },
    {
      menu_item: { owner: { user_id: (reply3?.feature_flag?.owner?.user_id ?? '') } },
      result: { message: (reply3?.result?.message ?? '') },
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
 * Returns a menu item.
 */
async function getMenuItemApprovalStatus(request) {
  validateContext(request.context);
  if (!request.menu_item_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
  }
  await store.record(request.context.tenant_id, 'GetMenuItemApprovalStatus', request);
  logger.info('get_menu_item_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { menu_item: { menu_item_id: (request?.menu_item_id ?? '') } };
}

/**
 * Lists menu items a page at a time.
 */
async function listPendingMenuItems(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListPendingMenuItems', request);
  logger.info('list_pending_menu_items', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    promotion: {
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const reply1 = await withRetry(() => call((done) => promotionsPromotionClient.createPromotion(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    menu_items: [{
      audit: { created_at: reply1?.promotion?.audit?.created_at },
      owner: { display_name: (reply1?.promotion?.owner?.display_name ?? '') },
    }],
  };
}

/**
 * Recalls a menu item.
 */
async function recallMenuItem(request) {
  validateContext(request.context);
  if (request.menu_item) {
    await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
  }
  await store.record(request.context.tenant_id, 'RecallMenuItem', request);
  logger.info('recall_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      approver: { display_name: (request?.menu_item?.owner?.display_name ?? '') },
    },
    replay_clip: {
      components: { primary: {
        component_id: (request?.menu_item?.components?.primary?.component_id ?? ''),
      } },
      dimensions: { width: (request?.menu_item?.dimensions?.width ?? 0) },
      owner: { display_name: (request?.details?.approver?.display_name ?? '') },
    },
  };
  const task1 = deferred(() => call((done) => broadcastsReplayClipApprovalClient.rejectReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.menu_item?.audit?.updated_at,
      region: ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.region ?? ''),
    },
    subscriber: { email: (request?.menu_item?.owner?.email ?? '') },
  };
  const messages2 = await collect(concessionsConcessionOrderReportingClient.streamConcessionOrderMetrics(request2, metadataFor(request2.context), { deadline: deadline() }));
  const reply2 = messages2.at(-1) ?? {};
  const request3 = {
    context: request?.context,
    menu_item: {
      components: { primary: {
        component_id: (reply1?.replay_clip?.components?.primary?.component_id ?? ''),
        description: (request?.menu_item?.components?.primary?.description ?? ''),
      } },
      owner: {
        display_name: (reply1?.replay_clip?.owner?.display_name ?? ''),
        email: (reply1?.replay_clip?.owner?.email ?? ''),
      },
    },
  };
  const reply3 = await bus.request('acme.concessions.v1.MenuItemApprovalService/RecallMenuItem:2', request3);
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      menu_item: { owner: { user_id: (reply1?.replay_clip?.owner?.user_id ?? '') } },
      result: { message: (reply1?.result?.message ?? '') },
    },
    {
      menu_item: {
        components: { primary: { quantity: (reply3?.menu_item?.components?.primary?.quantity ?? '0') } },
      },
    },
    {
      menu_item: {
        menu_item_id: (request?.menu_item_id ?? ''),
        specs: (request?.menu_item?.specs ?? {}),
      },
    },
  );
}

/**
 * Rejects a menu item.
 */
async function rejectMenuItem(request) {
  validateContext(request.context);
  if (request.menu_item) {
    await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
  }
  await store.record(request.context.tenant_id, 'RejectMenuItem', request);
  logger.info('reject_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  let selected1_3;
  switch (request?.menu_item?.locator) {
    case 'address':
      selected1_3 = ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.region ?? '');
      break;
    default:
      selected1_3 = '';
  }
  const request1 = {
    context: request?.context,
    details: {
      approver: {
        email: (request?.menu_item?.owner?.email ?? ''),
        user_id: (request?.menu_item?.owner?.user_id ?? ''),
      },
    },
    sponsorship_deal: {
      billing_address: { region: selected1_3 },
      lines: [{
        description: (request?.menu_item?.components?.primary?.description ?? ''),
      }],
    },
  };
  const reply1 = await call((done) => sponsorshipsSponsorshipDealApprovalClient.delegateSponsorshipDealApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    concession_stand: {
      address: { lines: [...(reply1?.sponsorship_deal?.billing_address?.lines ?? [])] },
      audit: {
        created_by: (reply1?.sponsorship_deal?.audit?.created_by ?? ''),
        etag: (reply1?.sponsorship_deal?.audit?.etag ?? ''),
      },
      owner: { email: (request?.menu_item?.owner?.email ?? '') },
    },
    context: request?.context,
  };
  const reply2 = await call((done) => clients.concessionsConcessionStandClient.createConcessionStand(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      menu_item: {
        owner: {
          display_name: (reply1?.sponsorship_deal?.counterparty?.display_name ?? ''),
        },
      },
    },
    {
      menu_item: { audit: { updated_at: reply2?.concession_stand?.audit?.updated_at } },
      result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
    },
    {
      menu_item: { menu_item_id: (request?.menu_item_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Submits a menu item.
 */
async function submitMenuItem(request) {
  validateContext(request.context);
  if (request.menu_item) {
    await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
  }
  await store.record(request.context.tenant_id, 'SubmitMenuItem', request);
  logger.info('submit_menu_item', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      owner_id: (request?.details?.approver?.user_id ?? ''),
      region: ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.region ?? ''),
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    notificationsNotificationQueryClient.findNotifications(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  let selected2_1;
  switch (request?.menu_item?.locator) {
    case 'address':
      selected2_1 = ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.country_code ?? '');
      break;
    default:
      selected2_1 = '';
  }
  const request2 = {
    concession_order: { billing_address: { country_code: selected2_1 } },
    context: request?.context,
    details: {
      approver: { display_name: (request?.menu_item?.owner?.display_name ?? '') },
    },
  };
  const reply2 = await call((done) => concessionsConcessionOrderApprovalClient.recallConcessionOrder(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: reply2?.concession_order?.audit?.updated_at,
      region: (reply2?.concession_order?.billing_address?.region ?? ''),
    },
  };
  const reply3 = await call((done) => configurationConfigSetQueryClient.browseConfigSets(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    sponsor: {
      audit: { created_at: request?.menu_item?.audit?.created_at },
      relationship_manager: {
        display_name: (reply2?.concession_order?.counterparty?.display_name ?? ''),
        user_id: (request?.details?.approver?.user_id ?? ''),
      },
    },
  };
  const method4 = 'recallSponsor';
  const reply4 = await call((done) => sponsorshipsSponsorApprovalClient[method4](request4, metadataFor(request4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    { outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) } },
    { result: { correlation_id: (reply3?.result?.correlation_id ?? '') } },
    {
      menu_item: { audit: { updated_by: (reply4?.sponsor?.audit?.updated_by ?? '') } },
    },
    {
      menu_item: {
        code: (request?.menu_item?.code ?? ''),
        menu_item_id: (request?.menu_item_id ?? ''),
      },
    },
  );
}

module.exports = { approveMenuItem, commentMenuItemApproval, delegateMenuItemApproval, escalateMenuItem, getMenuItemApprovalStatus, listPendingMenuItems, recallMenuItem, rejectMenuItem, submitMenuItem };
