'use strict';

/**
 * Computes the figures of a replay clip, such as totals, taxes, and estimates.
 */

const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve ReplayClipCalculationService, recording every operation of the broadcasts team.
 */
class ReplayClipCalculationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'ReplayClipCalculationService' });
  }

  /**
   * Calculates a replay clip.
   */
  calculateReplayClip(call, callback) {
    this.executeCalculateReplayClip(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCalculateReplayClip(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'CalculateReplayClip', request);
    this.logger.info('calculate_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          region: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.region ?? ''),
        },
      },
      stream_key: {
        audit: { updated_by: (request?.replay_clip?.audit?.updated_by ?? '') },
        holder: { display_name: (request?.replay_clip?.owner?.display_name ?? '') },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.calculateStreamKey(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      notification: {
        assignee: { user_id: (request?.replay_clip?.owner?.user_id ?? '') },
        audit: { created_by: (request?.replay_clip?.audit?.created_by ?? '') },
        requester: {
          phone: (reply1?.stream_key?.contact?.phone ?? ''),
          preferred_channel: (reply1?.stream_key?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.notificationsNotificationValidationClient.validateNotification(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.stream_key?.audit?.updated_at,
        labels: { ...(reply1?.stream_key?.labels ?? {}) },
        owner_id: (reply1?.stream_key?.holder?.show_id ?? ''),
        region: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.region ?? ''),
      },
    };
    const task3 = deferred(() => call((done) => this.clients.venuesVenueExportClient.listVenueExports(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    const request4 = {
      context: request?.context,
      stream_key: {
        audit: { updated_at: request?.replay_clip?.audit?.updated_at },
        card: {
          network: ((reply1?.stream_key?.funding === 'card' ? reply1?.stream_key.card : undefined)?.network ?? ''),
        },
      },
    };
    const reply4 = await this.bus.request('acme.broadcasts.v1.ReplayClipCalculationService/CalculateReplayClip:3', request4);
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        replay_clip: { audit: { created_at: reply1?.stream_key?.audit?.created_at } },
      },
      {
        replay_clip: { owner: { email: (reply2?.notification?.requester?.email ?? '') } },
        result: { message: (reply2?.result?.message ?? '') },
      },
      {
        result: {
          message: (reply3?.result?.message ?? ''),
          status: (reply3?.result?.status ?? ''),
        },
      },
      {
        breakdown: { currency_code: (reply4?.breakdown?.currency_code ?? '') },
        replay_clip: { owner: { email: (reply4?.stream_key?.contact?.email ?? '') } },
      },
      {
        replay_clip: {
          name: (request?.replay_clip?.name ?? ''),
          replay_clip_id: (request?.replay_clip_id ?? ''),
        },
      },
    );
  }

  /**
   * Compares a replay clip.
   */
  compareReplayClipOptions(call, callback) {
    this.executeCompareReplayClipOptions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCompareReplayClipOptions(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'CompareReplayClipOptions', request);
    this.logger.info('compare_replay_clip_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: {
        audit: { updated_by: (request?.replay_clip?.audit?.updated_by ?? '') },
        contact: { email: (request?.replay_clip?.owner?.email ?? '') },
        holder: { display_name: (request?.replay_clip?.owner?.display_name ?? '') },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.broadcastsStreamKeyCalculationClient.explainStreamKeyCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: {
        destination: { postal_code: (request?.details?.destination?.postal_code ?? '') },
      },
      stream_key: { contact: { phone: (reply1?.stream_key?.contact?.phone ?? '') } },
    };
    const reply2 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.quoteStreamKey(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        replay_clip: { audit: { created_at: reply1?.stream_key?.audit?.created_at } },
      },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      {
        replay_clip: {
          replay_clip_id: (request?.replay_clip_id ?? ''),
          specs: (request?.replay_clip?.specs ?? {}),
        },
      },
    );
  }

  /**
   * Estimates a replay clip.
   */
  estimateReplayClip(call, callback) {
    this.executeEstimateReplayClip(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEstimateReplayClip(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'EstimateReplayClip', request);
    this.logger.info('estimate_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue: {
        address: { lines: [...(request?.details?.destination?.lines ?? [])] },
        dimensions: { unit: (request?.replay_clip?.dimensions?.unit ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.venuesVenueValidationClient.explainVenueDecision(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    let selected2_1;
    switch (reply1?.venue?.locator) {
      case 'address':
        selected2_1 = ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.locality ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      details: { destination: { locality: selected2_1 } },
      stream_key: {
        audit: {
          etag: (request?.replay_clip?.audit?.etag ?? ''),
          updated_at: request?.replay_clip?.audit?.updated_at,
        },
      },
    };
    const reply2 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.compareStreamKeyOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          status: (reply1?.outcome?.status ?? ''),
        },
      },
      {
        replay_clip: {
          audit: {
            created_at: reply2?.stream_key?.audit?.created_at,
            updated_at: reply2?.stream_key?.audit?.updated_at,
          },
        },
      },
      {
        replay_clip: {
          capacity: (request?.replay_clip?.capacity ?? '0'),
          replay_clip_id: (request?.replay_clip_id ?? ''),
        },
      },
    );
  }

  /**
   * Explains a replay clip.
   */
  explainReplayClipCalculation(call, callback) {
    this.executeExplainReplayClipCalculation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainReplayClipCalculation(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'ExplainReplayClipCalculation', request);
    this.logger.info('explain_replay_clip_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.replay_clip?.owner?.email ?? '') } },
      stream_key: {
        audit: {
          created_at: request?.replay_clip?.audit?.created_at,
          updated_by: (request?.replay_clip?.audit?.updated_by ?? ''),
        },
        holder: { display_name: (request?.replay_clip?.owner?.display_name ?? '') },
      },
    };
    const reply1 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.explainStreamKeyCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      stream_key: {
        bank: {
          account_holder: ((reply1?.stream_key?.funding === 'bank' ? reply1?.stream_key.bank : undefined)?.account_holder ?? ''),
          bic: ((reply1?.stream_key?.funding === 'bank' ? reply1?.stream_key.bank : undefined)?.bic ?? ''),
        },
        contact: {
          email: (request?.replay_clip?.owner?.email ?? ''),
          name: reply1?.stream_key?.contact?.name,
        },
      },
    };
    const method2 = 'recalculateStreamKeys';
    const reply2 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        replay_clip: {
          audit: { updated_at: reply1?.stream_key?.audit?.updated_at },
          owner: { display_name: (reply1?.stream_key?.holder?.display_name ?? '') },
        },
      },
      {
        replay_clip: {
          audit: { created_by: (reply2?.stream_key?.audit?.created_by ?? '') },
          owner: { email: (reply2?.stream_key?.contact?.email ?? '') },
        },
      },
      {
        replay_clip: {
          name: (request?.replay_clip?.name ?? ''),
          replay_clip_id: (request?.replay_clip_id ?? ''),
        },
      },
    );
  }

  /**
   * Quotes a replay clip.
   */
  quoteReplayClip(call, callback) {
    this.executeQuoteReplayClip(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeQuoteReplayClip(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'QuoteReplayClip', request);
    this.logger.info('quote_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: { region: (request?.details?.destination?.region ?? '') },
      },
    };
    const client1 = this.clients.registry.get('acme.broadcasts.v1.StreamKeyCalculationService');
    const reply1 = await call((done) => client1.calculateStreamKey(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        breakdown: { currency_code: (reply1?.stream_key?.currency_code ?? '') },
        replay_clip: { audit: { etag: (reply1?.stream_key?.audit?.etag ?? '') } },
      },
      {
        replay_clip: {
          name: (request?.replay_clip?.name ?? ''),
          replay_clip_id: (request?.replay_clip_id ?? ''),
        },
      },
    );
  }

  /**
   * Recalculates a replay clip.
   */
  recalculateReplayClips(call, callback) {
    this.executeRecalculateReplayClips(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecalculateReplayClips(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'RecalculateReplayClips', request);
    this.logger.info('recalculate_replay_clips', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.replay_clip?.audit?.created_at,
        labels: { ...(request?.replay_clip?.specs ?? {}) },
        owner_id: (request?.replay_clip?.components?.primary?.component_id ?? ''),
        region: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.region ?? ''),
      },
    };
    const send1 = this.clients.venuesVenueQueryClient.findVenues.bind(this.clients.venuesVenueQueryClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        replay_clip: {
          code: (request?.replay_clip?.code ?? ''),
          replay_clip_id: (request?.replay_clip_id ?? ''),
        },
      },
    );
  }

  /**
   * Simulates a replay clip.
   */
  simulateReplayClip(call, callback) {
    this.executeSimulateReplayClip(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSimulateReplayClip(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'SimulateReplayClip', request);
    this.logger.info('simulate_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.replay_clip?.owner?.email ?? '') } },
      notification: {
        assignee: { display_name: (request?.replay_clip?.owner?.display_name ?? '') },
        audit: { created_by: (request?.replay_clip?.audit?.created_by ?? '') },
        tags: [...(request?.replay_clip?.tags ?? [])],
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.notificationsNotificationValidationClient.checkNotificationEligibility(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      filter: { region: (input2?.details?.destination?.region ?? '') },
    };
    const reply2 = await call((done) => this.clients.documentsDocumentClient.listDocuments(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      stream_key: {
        audit: { created_at: request?.replay_clip?.audit?.created_at },
        contact: { email: (request?.replay_clip?.owner?.email ?? '') },
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.simulateStreamKey(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      stream_key: {
        bank: {
          iban: ((reply3?.stream_key?.funding === 'bank' ? reply3?.stream_key.bank : undefined)?.iban ?? ''),
        },
      },
    };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.calculateStreamKey(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      { result: { details: (reply2?.result?.details ?? {}) } },
      {
        result: {
          reference_ids: (reply3?.result?.reference_ids ?? []),
          status: (reply3?.result?.status ?? ''),
        },
      },
      {
        replay_clip: { owner: { email: (reply4?.stream_key?.contact?.email ?? '') } },
      },
      {
        replay_clip: {
          capacity: (request?.replay_clip?.capacity ?? '0'),
          replay_clip_id: (request?.replay_clip_id ?? ''),
        },
      },
    );
  }
}

module.exports = { ReplayClipCalculationService };
