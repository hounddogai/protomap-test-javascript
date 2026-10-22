'use strict';

/**
 * Answers read-only questions about wishlists, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve WishlistQueryService, recording every operation of the fanportal team.
 */
class WishlistQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'WishlistQueryService' });
  }

  /**
   * Lists wishlists a page at a time.
   */
  browseWishlists(call, callback) {
    this.executeBrowseWishlists(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseWishlists(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseWishlists', request);
    this.logger.info('browse_wishlists', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Counts wishlists by status.
   */
  countWishlists(call, callback) {
    this.executeCountWishlists(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountWishlists(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountWishlists', request);
    this.logger.info('count_wishlists', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      ledger_account: { labels: { ...(request?.filter?.labels ?? {}) } },
    };
    const reply1 = await call((done) => this.clients.ledgerLedgerAccountValidationClient.checkLedgerAccountEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_stand: {
        audit: {
          created_by: (reply1?.ledger_account?.audit?.created_by ?? ''),
          etag: (reply1?.ledger_account?.audit?.etag ?? ''),
          updated_by: (reply1?.ledger_account?.audit?.updated_by ?? ''),
        },
        owner: { display_name: (reply1?.ledger_account?.holder?.display_name ?? '') },
      },
      context: request?.context,
    };
    const method2 = 'advanceConcessionStandWorkflow';
    const reply2 = await call((done) => this.clients.concessionsConcessionStandWorkflowClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      { result: { details: (reply2?.result?.details ?? {}) } },
    );
  }

  /**
   * Streams every matching wishlist.
   */
  exportWishlists(call) {
    serverStream((request) => this.executeExportWishlists(request))(call);
  }

  async executeExportWishlists(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportWishlists', request);
    this.logger.info('export_wishlists', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.filter?.labels ?? {}) },
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const client1 = this.clients.registry.get('acme.fanportal.v1.FanSessionQueryService');
    const reply1 = await call((done) => client1.findFanSessions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.filter?.labels ?? {}) },
        query: (request?.filter?.query ?? ''),
      },
    };
    const messages2 = await collect(this.clients.fanportalEventPageNotificationClient.streamEventPageNotices(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    const request3 = {
      broadcast: {
        location: { region: (request?.filter?.region ?? '') },
        organizer: { user_id: (request?.context?.actor?.user_id ?? '') },
        participants: [{ contact: { email: (request?.context?.actor?.email ?? '') } }],
      },
      context: request?.context,
    };
    const send3 = this.clients.broadcastsBroadcastWorkflowClient.resumeBroadcastWorkflow.bind(this.clients.broadcastsBroadcastWorkflowClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        result: { details: (reply3?.result?.details ?? {}) },
        wishlist: {
          billing_address: { postal_code: (reply3?.broadcast?.location?.postal_code ?? '') },
        },
      },
      {
        wishlist: { billing_address: { region: (request?.filter?.region ?? '') } },
      },
    );
  }

  /**
   * Lists wishlists a page at a time.
   */
  findWishlists(call, callback) {
    this.executeFindWishlists(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindWishlists(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindWishlists', request);
    this.logger.info('find_wishlists', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.filter?.labels ?? {}) },
      context: request?.context,
      details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.fanportalEventPageNotificationClient.unsubscribeEventPageUpdates(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      wishlists: [{
        billing_address: { recipient: (reply1?.event_page?.billing_address?.recipient ?? '') },
        due_at: reply1?.event_page?.due_at,
      }],
    };
  }

  /**
   * Returns a wishlist.
   */
  lookupWishlist(call, callback) {
    this.executeLookupWishlist(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupWishlist(request) {
    validateContext(request.context);
    if (!request.wishlist_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wishlist_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupWishlist', request);
    this.logger.info('lookup_wishlist', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      filter: {
        created_after: input1?.context?.deadline,
        labels: { ...(input1?.context?.baggage ?? {}) },
        owner_id: (input1?.wishlist_id ?? ''),
      },
    };
    const reply1 = await call((done) => this.clients.fanportalFanSessionClient.listFanSessions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      { wishlist: { wishlist_id: (request?.wishlist_id ?? '') } },
    );
  }

  /**
   * Returns several wishlists at once.
   */
  resolveWishlists(call, callback) {
    this.executeResolveWishlists(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveWishlists(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveWishlists', request);
    this.logger.info('resolve_wishlists', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        owner_id: (request?.context?.tenant_id ?? ''),
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.fanportalEventPageQueryClient.findEventPages(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    };
  }

  /**
   * Summarizes a wishlist.
   */
  summarizeWishlists(call, callback) {
    this.executeSummarizeWishlists(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeWishlists(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeWishlists', request);
    this.logger.info('summarize_wishlists', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      onboarding_plan: { lines: [{ attributes: { ...(request?.details?.filters ?? {}) } }] },
      reason: (request?.reason ?? ''),
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.onboardingOnboardingPlanCalculationClient.calculateOnboardingPlan(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.onboarding_plan?.audit?.updated_at,
        labels: { ...(request?.details?.filters ?? {}) },
        owner_id: (reply1?.onboarding_plan?.counterparty?.equipment_id ?? ''),
        region: (reply1?.onboarding_plan?.billing_address?.region ?? ''),
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.loyaltyLoyaltyMemberQueryClient.browseLoyaltyMembers(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.onboarding_plan?.audit?.updated_at,
        region: (reply1?.onboarding_plan?.billing_address?.region ?? ''),
      },
    };
    const reply3 = await withRetry(() => call((done) => this.clients.fanportalFanSessionQueryClient.findFanSessions(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          details: (reply1?.result?.details ?? {}),
        },
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          status: (reply2?.result?.status ?? ''),
        },
      },
      { result: { reference_ids: (reply3?.result?.reference_ids ?? []) } },
      { period: request?.details?.period },
    );
  }
}

module.exports = { WishlistQueryService };
