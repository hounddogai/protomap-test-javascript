'use strict';

/**
 * Answers read-only questions about sponsors, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve SponsorQueryService, recording every operation of the sponsorships team.
 */
class SponsorQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'SponsorQueryService' });
  }

  /**
   * Lists sponsors a page at a time.
   */
  browseSponsors(call, callback) {
    this.executeBrowseSponsors(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseSponsors(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseSponsors', request);
    this.logger.info('browse_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      activation: { requester: { email: (request?.context?.actor?.email ?? '') } },
      context: request?.context,
    };
    const send1 = this.clients.sponsorshipsActivationValidationClient.lintActivations.bind(this.clients.sponsorshipsActivationValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      sponsors: [{
        relationship_manager: { display_name: (reply1?.activation?.assignee?.display_name ?? '') },
      }],
    };
  }

  /**
   * Counts sponsors by status.
   */
  countSponsors(call, callback) {
    this.executeCountSponsors(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountSponsors(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountSponsors', request);
    this.logger.info('count_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.sponsorshipsActivationCalculationClient.compareActivationOptions(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        details: (reply1?.result?.details ?? {}),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    };
  }

  /**
   * Streams every matching sponsor.
   */
  exportSponsors(call) {
    serverStream((request) => this.executeExportSponsors(request))(call);
  }

  async executeExportSponsors(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportSponsors', request);
    this.logger.info('export_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      activation: {
        assignee: { user_id: (input1?.context?.actor?.user_id ?? '') },
        requester: { email: (input1?.context?.actor?.email ?? '') },
      },
      context: input1?.context,
    };
    const reply1 = await call((done) => this.clients.sponsorshipsActivationValidationClient.normalizeActivation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      activation: {
        audit: { updated_by: (reply1?.activation?.audit?.updated_by ?? '') },
        channel: {
          address: ((reply1?.activation?.origin === 'channel' ? reply1?.activation.channel : undefined)?.address ?? ''),
        },
        comments: [{ created_at: reply1?.activation?.audit?.created_at }],
        requester: { email: (reply1?.activation?.assignee?.email ?? '') },
      },
      context: request?.context,
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.sponsorshipsActivationCalculationClient.estimateActivation(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      sponsor: { contact: { email: (reply1?.activation?.assignee?.email ?? '') } },
    };
    const copied3 = structuredClone(request3);
    const reply3 = await call((done) => this.clients.sponsorshipsSponsorValidationClient.scoreSponsor(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      {
        result: { message: (reply1?.result?.message ?? '') },
        sponsor: {
          relationship_manager: {
            display_name: ((reply1?.activation?.origin === 'customer' ? reply1?.activation.customer : undefined)?.display_name ?? ''),
          },
        },
      },
      {
        sponsor: {
          contact: {
            email: (reply2?.activation?.assignee?.email ?? ''),
            name: reply2?.activation?.requester?.name,
          },
        },
      },
      {
        result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
        sponsor: { audit: { updated_at: reply3?.sponsor?.audit?.updated_at } },
      },
      {
        sponsor: { primary_address: { region: (request?.filter?.region ?? '') } },
      },
    );
  }

  /**
   * Lists sponsors a page at a time.
   */
  findSponsors(call, callback) {
    this.executeFindSponsors(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindSponsors(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindSponsors', request);
    this.logger.info('find_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
      replay_clip: {
        address: { region: (request?.filter?.region ?? '') },
        owner: { user_id: (request?.context?.actor?.user_id ?? '') },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.broadcastsReplayClipValidationClient.previewReplayClip(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      replay_clip: {
        address: {
          country_code: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.country_code ?? ''),
        },
      },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.broadcastsReplayClipCalculationClient.estimateReplayClip(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        sponsors: [{
          primary_address: {
            postal_code: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.postal_code ?? ''),
          },
        }],
      },
      {
        sponsors: [{
          primary_address: {
            locality: ((reply2?.replay_clip?.locator === 'address' ? reply2?.replay_clip.address : undefined)?.locality ?? ''),
          },
        }],
      },
    );
  }

  /**
   * Returns a sponsor.
   */
  lookupSponsor(call, callback) {
    this.executeLookupSponsor(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupSponsor(request) {
    validateContext(request.context);
    if (!request.sponsor_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsor_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupSponsor', request);
    this.logger.info('lookup_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        relationship_manager: { email: (request?.context?.actor?.email ?? '') },
        sponsor_id: (request?.sponsor_id ?? ''),
      },
    };
    const task1 = deferred(() => call((done) => this.clients.sponsorshipsSponsorCalculationClient.recalculateSponsors(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    return assemble(
      {
        sponsor: {
          relationship_manager: { email: (reply1?.sponsor?.contact?.email ?? '') },
        },
      },
      { sponsor: { sponsor_id: (request?.sponsor_id ?? '') } },
    );
  }

  /**
   * Returns several sponsors at once.
   */
  resolveSponsors(call, callback) {
    this.executeResolveSponsors(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveSponsors(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveSponsors', request);
    this.logger.info('resolve_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
    };
    const reply1 = await this.bus.request('acme.sponsorships.v1.SponsorQueryService/ResolveSponsors:0', request1);
    const request2 = {
      activation: {
        audit: { created_by: (reply1?.sponsorship_deal?.audit?.created_by ?? '') },
        customer: {
          display_name: (reply1?.sponsorship_deal?.counterparty?.display_name ?? ''),
        },
      },
      context: request?.context,
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.sponsorshipsActivationCalculationClient.simulateActivation(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    return assemble(
      {
        result: { status: (reply1?.outcome?.status ?? '') },
        sponsors: [{ audit: { created_at: reply1?.sponsorship_deal?.audit?.created_at } }],
      },
      {
        sponsors: [{
          audit: { created_at: reply2?.activation?.audit?.created_at },
          contact: { phone: (reply2?.activation?.requester?.phone ?? '') },
        }],
      },
    );
  }

  /**
   * Lists sponsors a page at a time.
   */
  suggestSponsors(call, callback) {
    this.executeSuggestSponsors(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuggestSponsors(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SuggestSponsors', request);
    this.logger.info('suggest_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: { primary_address: { region: (request?.filter?.region ?? '') } },
    };
    const reply1 = await call((done) => this.clients.sponsorshipsSponsorValidationClient.checkSponsorEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      sponsors: [{
        primary_address: { region: (reply1?.sponsor?.primary_address?.region ?? '') },
      }],
    };
  }

  /**
   * Summarizes a sponsor.
   */
  summarizeSponsors(call, callback) {
    this.executeSummarizeSponsors(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeSponsors(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeSponsors', request);
    this.logger.info('summarize_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      activation: { assignee: { email: (request?.context?.actor?.email ?? '') } },
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.sponsorshipsActivationCalculationClient.simulateActivation(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = { context: request?.context };
    const resolveVenueSections2 = promisify(this.clients.venuesVenueSectionQueryClient.resolveVenueSections.bind(this.clients.venuesVenueSectionQueryClient));
    const reply2 = await resolveVenueSections2(request2, metadataFor(request2.context), { deadline: deadline() });
    const request3 = {
      context: request?.context,
      sponsor_id: (request?.sponsor_id ?? ''),
    };
    const reply3 = await call((done) => this.clients.sponsorshipsSponsorCalculationClient.getSponsorRates(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        result: {
          reference_ids: (reply3?.result?.reference_ids ?? []),
          status: (reply3?.result?.status ?? ''),
        },
      },
      { period: request?.details?.period },
    );
  }
}

module.exports = { SponsorQueryService };
