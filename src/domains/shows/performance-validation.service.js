'use strict';

/**
 * Checks performances against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve PerformanceValidationService, recording every operation of the shows team.
 */
class PerformanceValidationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'PerformanceValidationService' });
  }

  /**
   * Checks a performance.
   */
  checkPerformanceEligibility(call, callback) {
    this.executeCheckPerformanceEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCheckPerformanceEligibility(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'CheckPerformanceEligibility', request);
    this.logger.info('check_performance_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_hold: {
        participants: [{
          contact: {
            email: (request?.performance?.organizer?.email ?? ''),
            preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.seatingSeatHoldValidationClient.explainSeatHoldDecision(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      sponsorship_deal: {
        billing_address: {
          postal_code: (reply1?.seat_hold?.location?.postal_code ?? ''),
          recipient: (reply1?.seat_hold?.location?.recipient ?? ''),
          region: (request?.performance?.location?.region ?? ''),
        },
      },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.sponsorshipsSponsorshipDealValidationClient.normalizeSponsorshipDeal(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        performance: {
          location: { country_code: (reply1?.seat_hold?.location?.country_code ?? '') },
          title: (reply1?.seat_hold?.title ?? ''),
        },
      },
      { outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) } },
      {
        performance: {
          performance_id: (request?.performance_id ?? ''),
          window: request?.performance?.window,
        },
      },
    );
  }

  /**
   * Explains a performance.
   */
  explainPerformanceDecision(call, callback) {
    this.executeExplainPerformanceDecision(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainPerformanceDecision(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'ExplainPerformanceDecision', request);
    this.logger.info('explain_performance_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      activation: { requester: { phone: (request?.details?.contact?.phone ?? '') } },
      context: request?.context,
    };
    const task1 = deferred(() => call((done) => this.clients.sponsorshipsActivationValidationClient.scoreActivation(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      show: {
        participants: [{ contact: { phone: (reply1?.activation?.requester?.phone ?? '') } }],
      },
    };
    const reply2 = await this.bus.request('acme.shows.v1.PerformanceValidationService/ExplainPerformanceDecision:1', request2);
    const request3 = {
      context: request?.context,
      details: {
        destination: {
          locality: (request?.performance?.location?.locality ?? ''),
          postal_code: (reply2?.show?.location?.postal_code ?? ''),
        },
      },
      replay_clip: {
        address: { recipient: (request?.performance?.location?.recipient ?? '') },
        owner: { email: (request?.details?.contact?.email ?? '') },
      },
    };
    const reply3 = await runInContext(request.context, () => call((done) => this.clients.broadcastsReplayClipCalculationClient.compareReplayClipOptions(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const request4 = {
      context: request?.context,
      sponsorship_deal: {
        audit: { updated_by: (reply3?.replay_clip?.audit?.updated_by ?? '') },
        billing_address: {
          country_code: ((reply3?.replay_clip?.locator === 'address' ? reply3?.replay_clip.address : undefined)?.country_code ?? ''),
          locality: ((reply3?.replay_clip?.locator === 'address' ? reply3?.replay_clip.address : undefined)?.locality ?? ''),
        },
      },
    };
    const reply4 = await call((done) => this.clients.sponsorshipsSponsorshipDealValidationClient.validateSponsorshipDeal(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        performance: {
          participants: [{ contact: { phone: (reply1?.activation?.requester?.phone ?? '') } }],
        },
      },
      {
        performance: {
          participants: [{ contact: { email: (reply2?.show?.organizer?.email ?? '') } }],
        },
      },
      {
        performance: {
          location: {
            locality: ((reply3?.replay_clip?.locator === 'address' ? reply3?.replay_clip.address : undefined)?.locality ?? ''),
          },
        },
      },
      {
        performance: {
          organizer: {
            display_name: (reply4?.sponsorship_deal?.counterparty?.display_name ?? ''),
          },
        },
        result: { status: (reply4?.outcome?.status ?? '') },
      },
      {
        performance: {
          performance_id: (request?.performance_id ?? ''),
          title: (request?.performance?.title ?? ''),
        },
      },
    );
  }

  /**
   * Returns a performance.
   */
  getPerformanceRules(call, callback) {
    this.executeGetPerformanceRules(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetPerformanceRules(request) {
    validateContext(request.context);
    if (!request.performance_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performance_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetPerformanceRules', request);
    this.logger.info('get_performance_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        organizer: { user_id: (request?.context?.actor?.user_id ?? '') },
        participants: [{
          contact: { email: (request?.context?.actor?.email ?? '') },
          participant_id: (request?.performance_id ?? ''),
        }],
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.showsShowCalculationClient.compareShowOptions(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    return assemble(
      {
        performance: {
          audit: { etag: (reply1?.show?.audit?.etag ?? '') },
          location: { region: (reply1?.show?.location?.region ?? '') },
        },
      },
      { performance: { performance_id: (request?.performance_id ?? '') } },
    );
  }

  /**
   * Lints a performance.
   */
  lintPerformances(call, callback) {
    this.executeLintPerformances(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLintPerformances(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'LintPerformances', request);
    this.logger.info('lint_performances', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.performance?.location?.region ?? '') },
    };
    const suggestLedgerAccounts1 = promisify(this.clients.ledgerLedgerAccountQueryClient.suggestLedgerAccounts.bind(this.clients.ledgerLedgerAccountQueryClient));
    const reply1 = await suggestLedgerAccounts1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      show: {
        participants: [{ contact: { phone: (request?.details?.contact?.phone ?? '') } }],
        weekly: {
          days: [...((request?.performance?.recurrence === 'weekly' ? request?.performance.weekly : undefined)?.days ?? [])],
        },
      },
    };
    const reply2 = await call((done) => this.clients.showsShowCalculationClient.calculateShow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const results3 = [];
    for (const item of (request?.performance?.participants ?? [])) {
      const request3 = {
        context: request?.context,
        show: {
          participants: [{
            participant_id: (item?.participant_id ?? ''),
            required: (item?.required ?? false),
          }],
        },
      };
      const reply3 = await call((done) => this.clients.showsShowCalculationClient.recalculateShows(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      results3.push(reply3);
    }
    const reply3 = results3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      {
        performance: {
          audit: {
            created_by: (reply2?.show?.audit?.created_by ?? ''),
            updated_at: reply2?.show?.audit?.updated_at,
          },
        },
      },
      {
        performance: {
          performance_id: (request?.performance_id ?? ''),
          window: request?.performance?.window,
        },
      },
    );
  }

  /**
   * Normalizes a performance.
   */
  normalizePerformance(call, callback) {
    this.executeNormalizePerformance(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeNormalizePerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'NormalizePerformance', request);
    this.logger.info('normalize_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsorship_deal: {
        audit: {
          created_by: (request?.performance?.audit?.created_by ?? ''),
          updated_at: request?.performance?.audit?.updated_at,
          updated_by: (request?.performance?.audit?.updated_by ?? ''),
        },
        billing_address: { lines: [...(request?.performance?.location?.lines ?? [])] },
      },
    };
    const method1 = 'checkSponsorshipDealEligibility';
    const reply1 = await call((done) => this.clients.sponsorshipsSponsorshipDealValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      lineup: {
        audit: { created_by: (reply1?.sponsorship_deal?.audit?.created_by ?? '') },
        billing_address: {
          country_code: (reply1?.sponsorship_deal?.billing_address?.country_code ?? ''),
        },
      },
    };
    const client2 = this.clients.registry.get('acme.shows.v1.LineupCalculationService');
    const reply2 = await call((done) => client2.calculateLineup(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.sponsorship_deal?.audit?.created_at,
        owner_id: (request?.performance?.organizer?.user_id ?? ''),
        region: (reply2?.lineup?.billing_address?.region ?? ''),
      },
    };
    const send3 = this.clients.metricsMetricSeriesQueryClient.suggestMetricSerieses.bind(this.clients.metricsMetricSeriesQueryClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        performance: {
          location: { locality: (reply1?.sponsorship_deal?.billing_address?.locality ?? '') },
        },
      },
      {
        performance: { audit: { updated_by: (reply2?.lineup?.audit?.updated_by ?? '') } },
      },
      {
        outcome: { message: (reply3?.result?.message ?? '') },
        result: { message: (reply3?.result?.message ?? '') },
      },
      {
        performance: {
          organizer: request?.performance?.organizer,
          performance_id: (request?.performance_id ?? ''),
        },
      },
    );
  }

  /**
   * Previews a performance.
   */
  previewPerformance(call, callback) {
    this.executePreviewPerformance(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePreviewPerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'PreviewPerformance', request);
    this.logger.info('preview_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      search_index: {
        address: { locality: (request?.performance?.location?.locality ?? '') },
        owner: { user_id: (request?.performance?.organizer?.user_id ?? '') },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.searchSearchIndexClient.createSearchIndex(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      show: {
        location: {
          postal_code: ((reply1?.search_index?.locator === 'address' ? reply1?.search_index.address : undefined)?.postal_code ?? ''),
        },
        participants: [{
          contact: {
            preferred_channel: (input2?.details?.contact?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const reply2 = await call((done) => this.clients.showsShowCalculationClient.compareShowOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      show: {
        daily: {
          interval: ((request?.performance?.recurrence === 'daily' ? request?.performance.daily : undefined)?.interval ?? 0),
        },
        location: { lines: [...(request?.performance?.location?.lines ?? [])] },
        participants: [{
          contact: {
            email: (request?.details?.contact?.email ?? ''),
            phone: (request?.details?.contact?.phone ?? ''),
          },
        }],
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => this.clients.showsShowCalculationClient.calculateShow(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = {
      activation: {
        assignee: { display_name: (reply3?.show?.organizer?.display_name ?? '') },
        requester: { email: (reply2?.show?.organizer?.email ?? '') },
      },
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.performance?.location?.country_code ?? ''),
          postal_code: (reply2?.show?.location?.postal_code ?? ''),
        },
      },
    };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => this.clients.sponsorshipsActivationCalculationClient.recalculateActivations(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        performance: { audit: { created_at: reply2?.show?.audit?.created_at } },
      },
      { outcome: { message: (reply3?.result?.message ?? '') } },
      {
        performance: { organizer: { email: (reply4?.activation?.requester?.email ?? '') } },
      },
      {
        performance: {
          metadata: (request?.performance?.metadata ?? {}),
          performance_id: (request?.performance_id ?? ''),
        },
      },
    );
  }

  /**
   * Scores a performance.
   */
  scorePerformance(call, callback) {
    this.executeScorePerformance(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScorePerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'ScorePerformance', request);
    this.logger.info('score_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.venuesVenueExportClient.scheduleVenueExport(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        performance: {
          performance_id: (request?.performance_id ?? ''),
          status: (request?.performance?.status ?? ''),
        },
      },
    );
  }

  /**
   * Validates a performance.
   */
  validatePerformance(call, callback) {
    this.executeValidatePerformance(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeValidatePerformance(request) {
    validateContext(request.context);
    if (request.performance) {
      await this.store.save(this.store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await this.store.record(request.context.tenant_id, 'ValidatePerformance', request);
    this.logger.info('validate_performance', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.performance?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        show: {
          participants: [{
            contact: { email: (item?.contact?.email ?? '') },
            required: (item?.required ?? false),
          }],
        },
      };
      const reply1 = await call((done) => this.clients.showsShowValidationClient.checkShowEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      performance: {
        organizer: request?.performance?.organizer,
        performance_id: (request?.performance_id ?? ''),
      },
    };
  }
}

module.exports = { PerformanceValidationService };
