'use strict';

/**
 * Checks broadcasts against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve BroadcastValidationService, recording every operation of the broadcasts team.
 */
class BroadcastValidationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'BroadcastValidationService' });
  }

  /**
   * Checks a broadcast.
   */
  checkBroadcastEligibility(call, callback) {
    this.executeCheckBroadcastEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCheckBroadcastEligibility(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'CheckBroadcastEligibility', request);
    this.logger.info('check_broadcast_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      subscriber: {
        email: (request?.details?.contact?.email ?? ''),
        name: request?.details?.contact?.name,
      },
    };
    const messages1 = await collect(this.clients.authorizationAccessPolicyClient.watchAccessPolicies(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      broadcast: {
        broadcast_id: (request?.broadcast_id ?? ''),
        organizer: request?.broadcast?.organizer,
      },
    };
  }

  /**
   * Explains a broadcast.
   */
  explainBroadcastDecision(call, callback) {
    this.executeExplainBroadcastDecision(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainBroadcastDecision(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'ExplainBroadcastDecision', request);
    this.logger.info('explain_broadcast_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.broadcast?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        stream_key: {
          contact: {
            email: (item?.contact?.email ?? ''),
            phone: (item?.contact?.phone ?? ''),
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply1 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.simulateStreamKey(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      details: { destination: { region: (request?.broadcast?.location?.region ?? '') } },
      stream_key: {
        audit: { updated_by: (reply1?.stream_key?.audit?.updated_by ?? '') },
        bank: {
          account_holder: ((reply1?.stream_key?.funding === 'bank' ? reply1?.stream_key.bank : undefined)?.account_holder ?? ''),
        },
        contact: {
          preferred_channel: (reply1?.stream_key?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.broadcastsStreamKeyCalculationClient.quoteStreamKey(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        broadcast: {
          audit: { created_at: reply2?.stream_key?.audit?.created_at },
          participants: [{ contact: { name: reply2?.stream_key?.contact?.name } }],
        },
      },
      {
        broadcast: {
          broadcast_id: (request?.broadcast_id ?? ''),
          title: (request?.broadcast?.title ?? ''),
        },
      },
    );
  }

  /**
   * Returns a broadcast.
   */
  getBroadcastRules(call, callback) {
    this.executeGetBroadcastRules(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetBroadcastRules(request) {
    validateContext(request.context);
    if (!request.broadcast_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'broadcast_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetBroadcastRules', request);
    this.logger.info('get_broadcast_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { broadcast: { broadcast_id: (request?.broadcast_id ?? '') } };
  }

  /**
   * Lints a broadcast.
   */
  lintBroadcasts(call, callback) {
    this.executeLintBroadcasts(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLintBroadcasts(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'LintBroadcasts', request);
    this.logger.info('lint_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.broadcast?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        details: {
          contact: {
            name: item?.contact?.name,
            phone: (item?.contact?.phone ?? ''),
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
        stream_key: { contact: { email: (item?.contact?.email ?? '') } },
      };
      const reply1 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.explainStreamKeyCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      broadcast: {
        broadcast_id: (request?.broadcast_id ?? ''),
        metadata: (request?.broadcast?.metadata ?? {}),
      },
    };
  }

  /**
   * Normalizes a broadcast.
   */
  normalizeBroadcast(call, callback) {
    this.executeNormalizeBroadcast(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeNormalizeBroadcast(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'NormalizeBroadcast', request);
    this.logger.info('normalize_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: { contact: { name: request?.details?.contact?.name } },
    };
    const reply1 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.calculateStreamKey(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    let selected2_1;
    switch (reply1?.stream_key?.funding) {
      case 'card':
        selected2_1 = ((reply1?.stream_key?.funding === 'card' ? reply1?.stream_key.card : undefined)?.holder_name ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      details: {
        destination: { lines: [...(request?.broadcast?.location?.lines ?? [])] },
      },
      stream_key: { card: { holder_name: selected2_1 } },
    };
    const reply2 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.quoteStreamKey(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        broadcast: {
          organizer: { display_name: (reply1?.stream_key?.holder?.display_name ?? '') },
        },
      },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      {
        broadcast: {
          broadcast_id: (request?.broadcast_id ?? ''),
          title: (request?.broadcast?.title ?? ''),
        },
      },
    );
  }

  /**
   * Previews a broadcast.
   */
  previewBroadcast(call, callback) {
    this.executePreviewBroadcast(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePreviewBroadcast(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'PreviewBroadcast', request);
    this.logger.info('preview_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: {
          created_by: (request?.broadcast?.audit?.created_by ?? ''),
          updated_at: request?.broadcast?.audit?.updated_at,
        },
        owner: {
          display_name: (request?.broadcast?.organizer?.display_name ?? ''),
          email: (request?.details?.contact?.email ?? ''),
        },
      },
    };
    const syncVenuePolicies1 = promisify(this.clients.venuesVenuePolicySyncClient.syncVenuePolicies.bind(this.clients.venuesVenuePolicySyncClient));
    const reply1 = await syncVenuePolicies1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      details: { destination: { region: (request?.broadcast?.location?.region ?? '') } },
    };
    const reply2 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.recalculateStreamKeys(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const results3 = [];
    for (const item of (request?.broadcast?.participants ?? [])) {
      const request3 = {
        context: request?.context,
        stream_key: {
          contact: {
            name: item?.contact?.name,
            phone: (item?.contact?.phone ?? ''),
          },
        },
      };
      const reply3 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.quoteStreamKey(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      results3.push(reply3);
    }
    const reply3 = results3.at(-1) ?? {};
    let selected4_1;
    switch (reply3?.stream_key?.funding) {
      case 'card':
        selected4_1 = ((reply3?.stream_key?.funding === 'card' ? reply3?.stream_key.card : undefined)?.last4 ?? '');
        break;
      default:
        selected4_1 = '';
    }
    const request4 = {
      context: request?.context,
      stream_key: { card: { last4: selected4_1 } },
    };
    const reply4 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.explainStreamKeyCalculation(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.outcome?.status ?? '') } },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        outcome: { status: (reply4?.result?.status ?? '') },
        result: { reference_ids: (reply4?.result?.reference_ids ?? []) },
      },
      {
        broadcast: {
          broadcast_id: (request?.broadcast_id ?? ''),
          location: request?.broadcast?.location,
        },
      },
    );
  }

  /**
   * Scores a broadcast.
   */
  scoreBroadcast(call, callback) {
    this.executeScoreBroadcast(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScoreBroadcast(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'ScoreBroadcast', request);
    this.logger.info('score_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.broadcast?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        filter: {
          owner_id: (item?.participant_id ?? ''),
          region: (request?.details?.destination?.region ?? ''),
        },
      };
      const reply1 = await call((done) => this.clients.venuesVenueQueryClient.findVenues(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      broadcast: {
        broadcast_id: (request?.broadcast_id ?? ''),
        window: request?.broadcast?.window,
      },
    };
  }

  /**
   * Validates a broadcast.
   */
  validateBroadcast(call, callback) {
    this.executeValidateBroadcast(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeValidateBroadcast(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'ValidateBroadcast', request);
    this.logger.info('validate_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.broadcast?.location?.region ?? '') } },
      stream_key: {
        audit: { etag: (request?.broadcast?.audit?.etag ?? '') },
        contact: {
          email: (request?.details?.contact?.email ?? ''),
          phone: (request?.details?.contact?.phone ?? ''),
        },
      },
    };
    const client1 = this.clients.registry.get('acme.broadcasts.v1.StreamKeyCalculationService');
    const reply1 = await call((done) => client1.simulateStreamKey(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        broadcast: { participants: [{ contact: reply1?.stream_key?.contact }] },
      },
      {
        broadcast: {
          broadcast_id: (request?.broadcast_id ?? ''),
          title: (request?.broadcast?.title ?? ''),
        },
      },
    );
  }
}

module.exports = { BroadcastValidationService };
