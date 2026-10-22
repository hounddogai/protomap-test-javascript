'use strict';

/**
 * Runs the long-lived workflow that takes a menu item from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect, exchange } = require('../../../platform/streams');

/**
 * The handlers of MenuItemWorkflowService, over the dependencies the concessions team is given.
 */
function createMenuItemWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a menu item.
   */
  async function advanceMenuItemWorkflow(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await store.record(request.context.tenant_id, 'AdvanceMenuItemWorkflow', request);
    logger.info('advance_menu_item_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { notify: { email: (request?.details?.notify?.email ?? '') } },
      stream_key: {
        audit: {
          etag: (request?.menu_item?.audit?.etag ?? ''),
          updated_by: (request?.menu_item?.audit?.updated_by ?? ''),
        },
        contact: { name: request?.details?.notify?.name },
      },
    };
    const startStreamKeyWorkflow1 = promisify(clients.broadcastsStreamKeyWorkflowClient.startStreamKeyWorkflow.bind(clients.broadcastsStreamKeyWorkflowClient));
    const reply1 = await startStreamKeyWorkflow1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      filter: { labels: { ...(reply1?.stream_key?.labels ?? {}) } },
    };
    const reply2 = await call((done) => clients.contractsContractQueryClient.suggestContracts(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: { audit: { created_by: (reply1?.stream_key?.audit?.created_by ?? '') } },
        outcome: { effective_at: reply1?.outcome?.effective_at },
      },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.result?.status ?? ''),
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
   * Returns a menu item.
   */
  async function inspectMenuItemWorkflow(request) {
    validateContext(request.context);
    if (!request.menu_item_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'menu_item_id is required');
    }
    await store.record(request.context.tenant_id, 'InspectMenuItemWorkflow', request);
    logger.info('inspect_menu_item_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: {
        audit: { updated_at: request?.context?.deadline },
        contact: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const method1 = 'advanceStreamKeyWorkflow';
    const reply1 = await call((done) => clients.broadcastsStreamKeyWorkflowClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      { result: { message: (reply1?.outcome?.message ?? '') } },
      { menu_item: { menu_item_id: (request?.menu_item_id ?? '') } },
    );
  }

  /**
   * Lists menu items a page at a time.
   */
  async function listMenuItemWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListMenuItemWorkflows', request);
    logger.info('list_menu_item_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: { owner: { email: (request?.context?.actor?.email ?? '') } },
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
    };
    const client1 = clients.registry.get('acme.concessions.v1.ConcessionStandValidationService');
    const reply1 = await call((done) => client1.scoreConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        labels: { ...(request?.filter?.labels ?? {}) },
        owner_id: (request?.filter?.owner_id ?? ''),
        region: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.region ?? ''),
      },
    };
    const messages2 = await collect(clients.concessionsConcessionOrderReportingClient.streamConcessionOrderMetrics(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    const request3 = {
      change: {
        block: { notify_email: (request?.context?.actor?.email ?? '') },
        parameters: { ...(request?.context?.baggage ?? {}) },
        scopes: [...(request?.context?.actor?.roles ?? [])],
      },
      context: request?.context,
    };
    const messages3 = await exchange(clients.venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request3.context), { deadline: deadline() }), [request3]);
    const reply3 = messages3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      menu_items: [{
        audit: { updated_by: (reply1?.concession_stand?.audit?.updated_by ?? '') },
        owner: { user_id: (reply1?.concession_stand?.owner?.user_id ?? '') },
      }],
    };
  }

  /**
   * Pauses a menu item.
   */
  async function pauseMenuItemWorkflow(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await store.record(request.context.tenant_id, 'PauseMenuItemWorkflow', request);
    logger.info('pause_menu_item_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: { updated_by: (request?.menu_item?.audit?.updated_by ?? '') },
        owner: { display_name: (request?.menu_item?.owner?.display_name ?? '') },
      },
    };
    const send1 = clients.venuesVenuePolicyHistoryClient.compareVenuePolicyVersions.bind(clients.venuesVenuePolicyHistoryClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { email: (request?.details?.notify?.email ?? '') } },
      menu_item: {
        address: {
          locality: ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.locality ?? ''),
        },
        components: { primary: {
          component_id: (request?.menu_item?.components?.primary?.component_id ?? ''),
        } },
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.concessionsMenuItemValidationClient.checkMenuItemEligibility(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = { context: input3?.context };
    const reply3 = await call((done) => clients.seatingSeatMapQueryClient.resolveSeatMaps(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      menu_item: { owner: { user_id: (reply1?.venue_policy?.owner?.user_id ?? '') } },
    };
    const cloned4 = JSON.parse(JSON.stringify(request4));
    const reply4 = await call((done) => clients.concessionsMenuItemCalculationClient.explainMenuItemCalculation(cloned4, metadataFor(cloned4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: { owner: { email: (reply1?.venue_policy?.owner?.email ?? '') } },
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      },
      {
        menu_item: { dimensions: { width: (reply2?.menu_item?.dimensions?.width ?? 0) } },
      },
      { result: { message: (reply3?.result?.message ?? '') } },
      {
        menu_item: { audit: { updated_at: reply4?.menu_item?.audit?.updated_at } },
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
   * Retrys a menu item.
   */
  async function retryMenuItemWorkflow(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await store.record(request.context.tenant_id, 'RetryMenuItemWorkflow', request);
    logger.info('retry_menu_item_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item_id: (request?.menu_item_id ?? ''),
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.concessionsMenuItemReportingClient.generateMenuItemReport(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { approver: { email: (request?.details?.notify?.email ?? '') } },
      sponsor: { audit: { updated_by: (request?.menu_item?.audit?.updated_by ?? '') } },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.sponsorshipsSponsorApprovalClient.submitSponsor(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        menu_item: {
          menu_item_id: (request?.menu_item_id ?? ''),
          specs: (request?.menu_item?.specs ?? {}),
        },
      },
    );
  }

  /**
   * Signals a menu item.
   */
  async function signalMenuItemWorkflow(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await store.record(request.context.tenant_id, 'SignalMenuItemWorkflow', request);
    logger.info('signal_menu_item_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      user_id: (request?.menu_item?.owner?.user_id ?? ''),
    };
    const reply1 = await withRetry(() => call((done) => clients.identityUserQueryClient.lookupUser(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: {
          audit: {
            created_at: reply1?.user?.audit?.created_at,
            etag: (reply1?.user?.audit?.etag ?? ''),
          },
        },
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
   * Starts a menu item.
   */
  async function startMenuItemWorkflow(request) {
    validateContext(request.context);
    if (request.menu_item) {
      await store.save(store.toMenuItemRecord(request.context.tenant_id, request.menu_item));
    }
    await store.record(request.context.tenant_id, 'StartMenuItemWorkflow', request);
    logger.info('start_menu_item_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        owner: { display_name: (request?.menu_item?.owner?.display_name ?? '') },
      },
    };
    const task1 = deferred(() => call((done) => clients.venuesVenuePolicySyncClient.resolveVenuePolicyConflict(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    let selected2_1;
    switch (request?.menu_item?.locator) {
      case 'address':
        selected2_1 = ((request?.menu_item?.locator === 'address' ? request?.menu_item.address : undefined)?.region ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.menu_item?.audit?.created_at,
        region: selected2_1,
      },
    };
    const reply2 = await call((done) => clients.concessionsConcessionOrderReportingClient.listConcessionOrderReports(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      venue_policy: {
        audit: { created_at: request?.menu_item?.audit?.created_at },
        discount: {
          percent: ((reply1?.venue_policy?.action === 'discount' ? reply1?.venue_policy.discount : undefined)?.percent ?? 0),
        },
      },
    };
    const reply3 = await runInContext(request.context, () => call((done) => clients.venuesVenuePolicyHistoryClient.compareVenuePolicyVersions(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const request4 = {
      context: request?.context,
      feature_flag: {
        owner: {
          display_name: (reply1?.venue_policy?.owner?.display_name ?? ''),
          user_id: (reply3?.venue_policy?.owner?.user_id ?? ''),
        },
        route: {
          target: ((reply3?.venue_policy?.action === 'route' ? reply3?.venue_policy.route : undefined)?.target ?? ''),
        },
      },
    };
    const reply4 = await call((done) => clients.featureflagsFeatureFlagClient.createFeatureFlag(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        menu_item: { audit: { etag: (reply1?.venue_policy?.audit?.etag ?? '') } },
        outcome: { status: (reply1?.result?.status ?? '') },
      },
      { outcome: { message: (reply2?.result?.message ?? '') } },
      {
        menu_item: { owner: { email: (reply3?.venue_policy?.owner?.email ?? '') } },
      },
      {
        menu_item: {
          audit: { updated_by: (reply4?.feature_flag?.audit?.updated_by ?? '') },
        },
      },
      {
        menu_item: {
          menu_item_id: (request?.menu_item_id ?? ''),
          status: (request?.menu_item?.status ?? ''),
        },
      },
    );
  }

  return {
    advanceMenuItemWorkflow: unary(advanceMenuItemWorkflow),
    inspectMenuItemWorkflow: unary(inspectMenuItemWorkflow),
    listMenuItemWorkflows: unary(listMenuItemWorkflows),
    pauseMenuItemWorkflow: unary(pauseMenuItemWorkflow),
    retryMenuItemWorkflow: unary(retryMenuItemWorkflow),
    signalMenuItemWorkflow: unary(signalMenuItemWorkflow),
    startMenuItemWorkflow: unary(startMenuItemWorkflow),
  };
}

module.exports = { createMenuItemWorkflowHandlers };
