'use strict';

/**
 * Answers read-only questions about concession orders, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve ConcessionOrderQueryService, recording every operation of the concessions team.
 */
class ConcessionOrderQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'ConcessionOrderQueryService' });
  }

  /**
   * Lists concession orders a page at a time.
   */
  browseConcessionOrders(call, callback) {
    this.executeBrowseConcessionOrders(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseConcessionOrders(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseConcessionOrders', request);
    this.logger.info('browse_concession_orders', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: { created_at: request?.filter?.created_after },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const send1 = this.clients.venuesVenuePolicySyncClient.pushVenuePolicy.bind(this.clients.venuesVenuePolicySyncClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      concession_orders: [{
        lines: [{
          tax: {
            amount: (reply1?.venue_policy?.action === 'discount' ? reply1?.venue_policy.discount : undefined)?.amount,
          },
        }],
      }],
    };
  }

  /**
   * Counts concession orders by status.
   */
  countConcessionOrders(call, callback) {
    this.executeCountConcessionOrders(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountConcessionOrders(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountConcessionOrders', request);
    this.logger.info('count_concession_orders', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
      ledger_account: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        labels: { ...(request?.filter?.labels ?? {}) },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.ledgerLedgerAccountValidationClient.scoreLedgerAccount(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      concession_order: {
        audit: {
          etag: (reply1?.ledger_account?.audit?.etag ?? ''),
          updated_by: (reply1?.ledger_account?.audit?.updated_by ?? ''),
        },
        card: {
          last4: ((reply1?.ledger_account?.funding === 'card' ? reply1?.ledger_account.card : undefined)?.last4 ?? ''),
          token: ((reply1?.ledger_account?.funding === 'card' ? reply1?.ledger_account.card : undefined)?.token ?? ''),
        },
      },
      context: input2?.context,
    };
    const reply2 = await call((done) => this.clients.concessionsConcessionOrderCalculationClient.compareConcessionOrderOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    );
  }

  /**
   * Streams every matching concession order.
   */
  exportConcessionOrders(call) {
    serverStream((request) => this.executeExportConcessionOrders(request))(call);
  }

  async executeExportConcessionOrders(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportConcessionOrders', request);
    this.logger.info('export_concession_orders', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: {
        address: { region: (request?.filter?.region ?? '') },
        audit: { updated_at: request?.filter?.created_after },
        owner: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.broadcastsReplayClipCalculationClient.simulateReplayClip(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    let selected2_2;
    switch (reply1?.replay_clip?.locator) {
      case 'address':
        selected2_2 = ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.locality ?? '');
        break;
      default:
        selected2_2 = '';
    }
    const request2 = {
      concession_stand: {
        owner: {
          display_name: (reply1?.replay_clip?.owner?.display_name ?? ''),
          email: (reply1?.replay_clip?.owner?.email ?? ''),
        },
      },
      context: request?.context,
      details: { destination: { locality: selected2_2 } },
    };
    const reply2 = await call((done) => this.clients.concessionsConcessionStandValidationClient.scoreConcessionStand(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      menu_item: {
        address: {
          country_code: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.country_code ?? ''),
          postal_code: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.postal_code ?? ''),
        },
      },
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => this.clients.concessionsMenuItemCalculationClient.explainMenuItemCalculation(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      {
        concession_order: { audit: { etag: (reply1?.replay_clip?.audit?.etag ?? '') } },
      },
      {
        concession_order: { audit: { created_at: reply2?.concession_stand?.audit?.created_at } },
      },
      {
        concession_order: {
          counterparty: { menu_item_id: (reply3?.menu_item?.menu_item_id ?? '') },
          lines: [{
            description: (reply3?.menu_item?.components?.primary?.description ?? ''),
          }],
        },
      },
      {
        concession_order: { billing_address: { region: (request?.filter?.region ?? '') } },
      },
    );
  }

  /**
   * Lists concession orders a page at a time.
   */
  findConcessionOrders(call, callback) {
    this.executeFindConcessionOrders(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindConcessionOrders(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindConcessionOrders', request);
    this.logger.info('find_concession_orders', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { locale: (request?.context?.locale ?? '') },
      user: {
        primary_address: { region: (request?.filter?.region ?? '') },
        relationship_manager: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.identityUserValidationClient.checkUserEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      concession_orders: [{
        billing_address: { country_code: (reply1?.user?.primary_address?.country_code ?? '') },
      }],
    };
  }

  /**
   * Returns a concession order.
   */
  lookupConcessionOrder(call, callback) {
    this.executeLookupConcessionOrder(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupConcessionOrder(request) {
    validateContext(request.context);
    if (!request.concession_order_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupConcessionOrder', request);
    this.logger.info('lookup_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { locale: (request?.context?.locale ?? '') },
      sponsor: {
        relationship_manager: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const task1 = deferred(() => call((done) => this.clients.sponsorshipsSponsorValidationClient.normalizeSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    return assemble(
      {
        concession_order: {
          audit: { created_by: (reply1?.sponsor?.audit?.created_by ?? '') },
          billing_address: { recipient: (reply1?.sponsor?.primary_address?.recipient ?? '') },
        },
      },
      {
        concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      },
    );
  }

  /**
   * Returns a concession order.
   */
  peekConcessionOrder(call, callback) {
    this.executePeekConcessionOrder(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekConcessionOrder(request) {
    validateContext(request.context);
    if (!request.concession_order_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekConcessionOrder', request);
    this.logger.info('peek_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return {
      concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
    };
  }

  /**
   * Returns several concession orders at once.
   */
  resolveConcessionOrders(call, callback) {
    this.executeResolveConcessionOrders(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveConcessionOrders(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveConcessionOrders', request);
    this.logger.info('resolve_concession_orders', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return {};
  }

  /**
   * Lists concession orders a page at a time.
   */
  suggestConcessionOrders(call, callback) {
    this.executeSuggestConcessionOrders(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuggestConcessionOrders(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SuggestConcessionOrders', request);
    this.logger.info('suggest_concession_orders', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Summarizes a concession order.
   */
  summarizeConcessionOrders(call, callback) {
    this.executeSummarizeConcessionOrders(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeConcessionOrders(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeConcessionOrders', request);
    this.logger.info('summarize_concession_orders', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { labels: { ...(request?.details?.filters ?? {}) } },
    };
    const reply1 = await this.bus.request('acme.concessions.v1.ConcessionOrderQueryService/SummarizeConcessionOrders:0', request1);
    const request2 = { context: request?.context };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.broadcastsBroadcastClient.batchGetBroadcasts(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      { period: request?.details?.period },
    );
  }
}

module.exports = { ConcessionOrderQueryService };
