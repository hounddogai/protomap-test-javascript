'use strict';

/**
 * Answers read-only questions about sponsorship deals, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve SponsorshipDealQueryService, recording every operation of the sponsorships team.
 */
class SponsorshipDealQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'SponsorshipDealQueryService' });
  }

  /**
   * Lists sponsorship deals a page at a time.
   */
  browseSponsorshipDeals(call, callback) {
    this.executeBrowseSponsorshipDeals(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseSponsorshipDeals(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseSponsorshipDeals', request);
    this.logger.info('browse_sponsorship_deals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      activation: { requester: { email: (request?.context?.actor?.email ?? '') } },
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.sponsorshipsActivationCalculationClient.recalculateActivations(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: { message: (reply1?.result?.message ?? '') },
      sponsorship_deals: [{ audit: { etag: (reply1?.activation?.audit?.etag ?? '') } }],
    };
  }

  /**
   * Counts sponsorship deals by status.
   */
  countSponsorshipDeals(call, callback) {
    this.executeCountSponsorshipDeals(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountSponsorshipDeals(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountSponsorshipDeals', request);
    this.logger.info('count_sponsorship_deals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Streams every matching sponsorship deal.
   */
  exportSponsorshipDeals(call) {
    serverStream((request) => this.executeExportSponsorshipDeals(request))(call);
  }

  async executeExportSponsorshipDeals(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportSponsorshipDeals', request);
    this.logger.info('export_sponsorship_deals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
      stream_key: { labels: { ...(request?.filter?.labels ?? {}) } },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.compareStreamKeyOptions(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const reply2 = await withRetry(() => call((done) => this.clients.broadcastsBroadcastQueryClient.resolveBroadcasts(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = { context: request?.context };
    const task3 = deferred(() => call((done) => this.clients.broadcastsStreamKeyQueryClient.resolveStreamKeys(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    const request4 = {
      context: request?.context,
      details: {
        recipient: {
          email: (reply1?.stream_key?.contact?.email ?? ''),
          name: reply1?.stream_key?.contact?.name,
          phone: (reply1?.stream_key?.contact?.phone ?? ''),
          preferred_channel: (reply1?.stream_key?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply4 = await this.bus.request('acme.sponsorships.v1.SponsorshipDealQueryService/ExportSponsorshipDeals:3', request4);
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        result: {
          correlation_id: (reply3?.result?.correlation_id ?? ''),
          reference_ids: (reply3?.result?.reference_ids ?? []),
        },
      },
      {
        result: {
          correlation_id: (reply4?.result?.correlation_id ?? ''),
          status: (reply4?.outcome?.status ?? ''),
        },
      },
      {
        sponsorship_deal: { billing_address: { region: (request?.filter?.region ?? '') } },
      },
    );
  }

  /**
   * Lists sponsorship deals a page at a time.
   */
  findSponsorshipDeals(call, callback) {
    this.executeFindSponsorshipDeals(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindSponsorshipDeals(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindSponsorshipDeals', request);
    this.logger.info('find_sponsorship_deals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        primary_address: { region: (request?.filter?.region ?? '') },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.sponsorshipsSponsorValidationClient.normalizeSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      filter: {
        owner_id: (request?.filter?.owner_id ?? ''),
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const messages2 = await collect(this.clients.sponsorshipsSponsorQueryClient.exportSponsors(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      sponsorship_deals: [{
        billing_address: { region: (reply1?.sponsor?.primary_address?.region ?? '') },
        number: ((reply1?.sponsor?.identity_document === 'national_id' ? reply1?.sponsor.national_id : undefined)?.number ?? ''),
      }],
    };
  }

  /**
   * Returns a sponsorship deal.
   */
  lookupSponsorshipDeal(call, callback) {
    this.executeLookupSponsorshipDeal(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupSponsorshipDeal(request) {
    validateContext(request.context);
    if (!request.sponsorship_deal_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsorship_deal_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupSponsorshipDeal', request);
    this.logger.info('lookup_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.context?.tenant_id ?? '') },
    };
    const reply1 = await call((done) => this.clients.sponsorshipsSponsorQueryClient.countSponsors(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      activation: {
        assignee: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
      context: request?.context,
    };
    const reply2 = await new Promise((resolve, reject) => {
      this.clients.sponsorshipsActivationValidationClient.normalizeActivation(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        sponsorship_deal: { counterparty: { email: (reply2?.activation?.assignee?.email ?? '') } },
      },
      {
        sponsorship_deal: { sponsorship_deal_id: (request?.sponsorship_deal_id ?? '') },
      },
    );
  }

  /**
   * Returns a sponsorship deal.
   */
  peekSponsorshipDeal(call, callback) {
    this.executePeekSponsorshipDeal(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekSponsorshipDeal(request) {
    validateContext(request.context);
    if (!request.sponsorship_deal_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsorship_deal_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekSponsorshipDeal', request);
    this.logger.info('peek_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      activation: {
        assignee: { user_id: (request?.context?.actor?.user_id ?? '') },
        external_reference: (request?.sponsorship_deal_id ?? ''),
      },
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
    };
    const validateActivation1 = promisify(this.clients.sponsorshipsActivationValidationClient.validateActivation.bind(this.clients.sponsorshipsActivationValidationClient));
    const reply1 = await validateActivation1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      filter: { created_after: reply1?.activation?.sla?.due_at },
    };
    const reply2 = await call((done) => this.clients.broadcastsBroadcastQueryClient.browseBroadcasts(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      {
        sponsorship_deal: {
          audit: { etag: (reply1?.activation?.audit?.etag ?? '') },
          counterparty: {
            display_name: ((reply1?.activation?.origin === 'customer' ? reply1?.activation.customer : undefined)?.display_name ?? ''),
          },
        },
      },
      { result: { details: (reply2?.result?.details ?? {}) } },
      {
        sponsorship_deal: { sponsorship_deal_id: (request?.sponsorship_deal_id ?? '') },
      },
    );
  }

  /**
   * Returns several sponsorship deals at once.
   */
  resolveSponsorshipDeals(call, callback) {
    this.executeResolveSponsorshipDeals(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveSponsorshipDeals(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveSponsorshipDeals', request);
    this.logger.info('resolve_sponsorship_deals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      activation: {
        assignee: { user_id: (request?.context?.actor?.user_id ?? '') },
        requester: { email: (request?.context?.actor?.email ?? '') },
        sla: { due_at: request?.context?.deadline },
      },
      context: request?.context,
    };
    const method1 = 'quoteActivation';
    const reply1 = await call((done) => this.clients.sponsorshipsActivationCalculationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return {
      result: { details: (reply1?.result?.details ?? {}) },
      sponsorship_deals: [{
        counterparty: { display_name: (reply1?.activation?.assignee?.display_name ?? '') },
      }],
    };
  }

  /**
   * Lists sponsorship deals a page at a time.
   */
  suggestSponsorshipDeals(call, callback) {
    this.executeSuggestSponsorshipDeals(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuggestSponsorshipDeals(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SuggestSponsorshipDeals', request);
    this.logger.info('suggest_sponsorship_deals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Summarizes a sponsorship deal.
   */
  summarizeSponsorshipDeals(call, callback) {
    this.executeSummarizeSponsorshipDeals(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeSponsorshipDeals(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeSponsorshipDeals', request);
    this.logger.info('summarize_sponsorship_deals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const client1 = this.clients.registry.get('acme.broadcasts.v1.BroadcastService');
    const reply1 = await call((done) => client1.batchGetBroadcasts(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
    };
    const send2 = this.clients.sponsorshipsSponsorshipDealValidationClient.checkSponsorshipDealEligibility.bind(this.clients.sponsorshipsSponsorshipDealValidationClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          message: (reply2?.result?.message ?? ''),
        },
      },
      { period: request?.details?.period },
    );
  }
}

module.exports = { SponsorshipDealQueryService };
