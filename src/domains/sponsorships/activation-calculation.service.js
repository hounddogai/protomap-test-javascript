'use strict';

/**
 * Computes the figures of an activation, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve ActivationCalculationService, recording every operation of the sponsorships team.
 */
class ActivationCalculationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'ActivationCalculationService' });
  }

  /**
   * Calculates an activation.
   */
  calculateActivation(call, callback) {
    this.executeCalculateActivation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCalculateActivation(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'CalculateActivation', request);
    this.logger.info('calculate_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { name: request?.activation?.requester?.name } },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.broadcastsStreamKeyValidationClient.explainStreamKeyDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      filter: {
        labels: { ...(reply1?.stream_key?.labels ?? {}) },
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const task2 = deferred(() => call((done) => this.clients.broadcastsBroadcastQueryClient.browseBroadcasts(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    const request3 = {
      context: request?.context,
      replay_clip: {
        owner: {
          display_name: ((request?.activation?.origin === 'customer' ? request?.activation.customer : undefined)?.display_name ?? ''),
        },
      },
    };
    const reply3 = await this.bus.request('acme.sponsorships.v1.ActivationCalculationService/CalculateActivation:2', request3);
    const request4 = {
      context: request?.context,
      sponsor: {
        audit: { created_at: reply3?.replay_clip?.audit?.created_at },
        relationship_manager: { email: (reply3?.replay_clip?.owner?.email ?? '') },
      },
    };
    const reply4 = await runInContext(request.context, () => call((done) => this.clients.sponsorshipsSponsorCalculationClient.simulateSponsor(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        breakdown: { currency_code: (reply1?.stream_key?.currency_code ?? '') },
        result: { status: (reply1?.outcome?.status ?? '') },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        breakdown: { currency_code: (reply3?.breakdown?.currency_code ?? '') },
      },
      {
        activation: { requester: { email: (reply4?.sponsor?.contact?.email ?? '') } },
      },
      {
        activation: {
          activation_id: (request?.activation_id ?? ''),
          subject: (request?.activation?.subject ?? ''),
        },
      },
    );
  }

  /**
   * Compares an activation.
   */
  compareActivationOptions(call, callback) {
    this.executeCompareActivationOptions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCompareActivationOptions(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'CompareActivationOptions', request);
    this.logger.info('compare_activation_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: { contact: { email: (request?.activation?.requester?.email ?? '') } },
    };
    const reply1 = await call((done) => this.clients.broadcastsStreamKeyValidationClient.normalizeStreamKey(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        breakdown: { currency_code: (reply1?.stream_key?.currency_code ?? '') },
      },
      {
        activation: {
          activation_id: (request?.activation_id ?? ''),
          custom_fields: (request?.activation?.custom_fields ?? {}),
        },
      },
    );
  }

  /**
   * Estimates an activation.
   */
  estimateActivation(call, callback) {
    this.executeEstimateActivation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEstimateActivation(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'EstimateActivation', request);
    this.logger.info('estimate_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          locality: (request?.details?.destination?.locality ?? ''),
          recipient: (request?.details?.destination?.recipient ?? ''),
        },
      },
      sponsor: {
        contact: {
          preferred_channel: (request?.activation?.requester?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.sponsorshipsSponsorValidationClient.scoreSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      venue: {
        audit: { updated_by: (reply1?.sponsor?.audit?.updated_by ?? '') },
        owner: { user_id: (request?.activation?.assignee?.user_id ?? '') },
      },
    };
    const cloneVenue2 = promisify(this.clients.venuesVenueClient.cloneVenue.bind(this.clients.venuesVenueClient));
    const reply2 = await cloneVenue2(request2, metadataFor(request2.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        activation: {
          assignee: { email: (reply2?.venue?.owner?.email ?? '') },
          audit: { created_by: (reply2?.venue?.audit?.created_by ?? '') },
        },
      },
      {
        activation: {
          activation_id: (request?.activation_id ?? ''),
          priority: (request?.activation?.priority ?? 0),
        },
      },
    );
  }

  /**
   * Returns an activation.
   */
  getActivationRates(call, callback) {
    this.executeGetActivationRates(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetActivationRates(request) {
    validateContext(request.context);
    if (!request.activation_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'activation_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetActivationRates', request);
    this.logger.info('get_activation_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { locale: (request?.context?.locale ?? '') },
      metric_series: {
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const reply1 = await call((done) => this.clients.metricsMetricSeriesValidationClient.explainMetricSeriesDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      { activation: { activation_id: (request?.activation_id ?? '') } },
    );
  }

  /**
   * Quotes an activation.
   */
  quoteActivation(call, callback) {
    this.executeQuoteActivation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeQuoteActivation(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'QuoteActivation', request);
    this.logger.info('quote_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: {
        address: {
          country_code: (request?.details?.destination?.country_code ?? ''),
          locality: (request?.details?.destination?.locality ?? ''),
        },
        audit: { updated_by: (request?.activation?.audit?.updated_by ?? '') },
      },
    };
    const method1 = 'normalizeReplayClip';
    const reply1 = await call((done) => this.clients.broadcastsReplayClipValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const results2 = [];
    for (const item of (request?.activation?.comments ?? [])) {
      const request2 = {
        context: request?.context,
        filter: {
          created_after: item?.created_at,
          owner_id: (item?.comment_id ?? ''),
          region: (request?.details?.destination?.region ?? ''),
        },
      };
      const reply2 = await call((done) => this.clients.documentsDocumentQueryClient.suggestDocuments(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      results2.push(reply2);
    }
    const reply2 = results2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        activation: {
          activation_id: (request?.activation_id ?? ''),
          requester: request?.activation?.requester,
        },
      },
    );
  }

  /**
   * Recalculates an activation.
   */
  recalculateActivations(call, callback) {
    this.executeRecalculateActivations(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecalculateActivations(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'RecalculateActivations', request);
    this.logger.info('recalculate_activations', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: { contact: { name: request?.activation?.requester?.name } },
    };
    const client1 = this.clients.registry.get('acme.sponsorships.v1.SponsorValidationService');
    const reply1 = await call((done) => client1.explainSponsorDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    let selected2_3;
    switch (request?.activation?.origin) {
      case 'customer':
        selected2_3 = ((request?.activation?.origin === 'customer' ? request?.activation.customer : undefined)?.display_name ?? '');
        break;
      default:
        selected2_3 = '';
    }
    const request2 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            location: {
              lines: [...(reply1?.sponsor?.primary_address?.lines ?? [])],
              postal_code: (request?.details?.destination?.postal_code ?? ''),
            },
            manager: {
              display_name: selected2_3,
              email: (reply1?.sponsor?.relationship_manager?.email ?? ''),
            },
          }],
        }],
      },
    };
    const reply2 = await call((done) => this.clients.venuesVenueSectionValidationClient.normalizeVenueSection(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        activation: { audit: { etag: (reply1?.sponsor?.audit?.etag ?? '') } },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        activation: { audit: { updated_at: reply2?.venue_section?.audit?.updated_at } },
        result: { message: (reply2?.outcome?.message ?? '') },
      },
      {
        activation: {
          activation_id: (request?.activation_id ?? ''),
          custom_fields: (request?.activation?.custom_fields ?? {}),
        },
      },
    );
  }

  /**
   * Simulates an activation.
   */
  simulateActivation(call, callback) {
    this.executeSimulateActivation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSimulateActivation(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'SimulateActivation', request);
    this.logger.info('simulate_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsorship_deal: {
        audit: {
          created_at: request?.activation?.audit?.created_at,
          updated_by: (request?.activation?.audit?.updated_by ?? ''),
        },
        counterparty: { email: (request?.activation?.assignee?.email ?? '') },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.sponsorshipsSponsorshipDealCalculationClient.estimateSponsorshipDeal(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const results2 = await Promise.all((request?.activation?.comments ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        sponsor: { audit: { created_at: item?.created_at } },
      };
      const reply2 = await call((done) => this.clients.sponsorshipsSponsorValidationClient.normalizeSponsor(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      broadcast: {
        audit: { created_by: (reply1?.sponsorship_deal?.audit?.created_by ?? '') },
        location: { lines: [...(reply1?.sponsorship_deal?.billing_address?.lines ?? [])] },
        participants: [{
          contact: {
            preferred_channel: (input3?.activation?.requester?.preferred_channel ?? ''),
          },
        }],
      },
      context: input3?.context,
    };
    const reply3 = await call((done) => this.clients.broadcastsBroadcastValidationClient.checkBroadcastEligibility(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        activation: {
          audit: {
            created_at: reply1?.sponsorship_deal?.audit?.created_at,
            updated_by: (reply1?.sponsorship_deal?.audit?.updated_by ?? ''),
          },
        },
      },
      {
        activation: { audit: { etag: (reply3?.broadcast?.audit?.etag ?? '') } },
        result: { status: (reply3?.result?.status ?? '') },
      },
      {
        activation: { activation_id: (request?.activation_id ?? '') },
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
      },
    );
  }
}

module.exports = { ActivationCalculationService };
