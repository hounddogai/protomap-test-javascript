'use strict';

/**
 * Checks stream keys against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of StreamKeyValidationService, over the dependencies the broadcasts team is given.
 */
function createStreamKeyValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks a stream key.
   */
  async function checkStreamKeyEligibility(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'CheckStreamKeyEligibility', request);
    logger.info('check_stream_key_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: {
        audit: {
          etag: (request?.stream_key?.audit?.etag ?? ''),
          updated_at: request?.stream_key?.audit?.updated_at,
        },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.broadcastsReplayClipCalculationClient.calculateReplayClip(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      replay_clip: {
        audit: { created_by: (reply1?.replay_clip?.audit?.created_by ?? '') },
        dimensions: {
          unit: (reply1?.replay_clip?.dimensions?.unit ?? ''),
          weight: (reply1?.replay_clip?.dimensions?.weight ?? 0),
        },
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.broadcastsReplayClipCalculationClient.explainReplayClipCalculation(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { details: (reply1?.result?.details ?? {}) },
        stream_key: { audit: { created_at: reply1?.replay_clip?.audit?.created_at } },
      },
      {
        stream_key: { audit: { etag: (reply2?.replay_clip?.audit?.etag ?? '') } },
      },
      {
        stream_key: {
          holder: request?.stream_key?.holder,
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  /**
   * Explains a stream key.
   */
  async function explainStreamKeyDecision(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'ExplainStreamKeyDecision', request);
    logger.info('explain_stream_key_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: {
        participants: [{ contact: { phone: (request?.stream_key?.contact?.phone ?? '') } }],
      },
      context: request?.context,
    };
    const reply1 = await withRetry(() => call((done) => clients.broadcastsBroadcastValidationClient.previewBroadcast(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { status: (reply1?.outcome?.status ?? '') },
        stream_key: { audit: { updated_by: (reply1?.broadcast?.audit?.updated_by ?? '') } },
      },
      {
        stream_key: {
          balance: request?.stream_key?.balance,
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a stream key.
   */
  async function getStreamKeyRules(request) {
    validateContext(request.context);
    if (!request.stream_key_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'stream_key_id is required');
    }
    await store.record(request.context.tenant_id, 'GetStreamKeyRules', request);
    logger.info('get_stream_key_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      replay_clip: { owner: { user_id: (request?.context?.actor?.user_id ?? '') } },
    };
    const task1 = deferred(() => call((done) => clients.broadcastsReplayClipCalculationClient.explainReplayClipCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      details: {
        destination: {
          country_code: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.country_code ?? ''),
        },
      },
      stream_key: { audit: { etag: (reply1?.replay_clip?.audit?.etag ?? '') } },
    };
    const reply2 = await bus.request('acme.broadcasts.v1.StreamKeyValidationService/GetStreamKeyRules:1', request2);
    return assemble(
      {
        stream_key: { contact: { email: (reply1?.replay_clip?.owner?.email ?? '') } },
      },
      {
        stream_key: { audit: { etag: (reply2?.stream_key?.audit?.etag ?? '') } },
      },
      { stream_key: { stream_key_id: (request?.stream_key_id ?? '') } },
    );
  }

  /**
   * Lints a stream key.
   */
  async function lintStreamKeys(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'LintStreamKeys', request);
    logger.info('lint_stream_keys', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      access_policy: { owner: { email: (request?.stream_key?.contact?.email ?? '') } },
      context: request?.context,
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.authorizationAccessPolicyValidationClient.lintAccessPolicies(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      venue: {
        audit: { created_by: (request?.stream_key?.audit?.created_by ?? '') },
        owner: {
          email: (reply1?.access_policy?.owner?.email ?? ''),
          user_id: (reply1?.access_policy?.owner?.user_id ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.venuesVenueClient.cloneVenue(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { next_steps: (reply1?.outcome?.next_steps ?? []) } },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        stream_key: {
          opened_at: request?.stream_key?.opened_at,
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  /**
   * Normalizes a stream key.
   */
  async function normalizeStreamKey(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'NormalizeStreamKey', request);
    logger.info('normalize_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { currency_code: (request?.stream_key?.currency_code ?? '') },
      replay_clip: {
        audit: {
          created_by: (request?.stream_key?.audit?.created_by ?? ''),
          updated_by: (request?.stream_key?.audit?.updated_by ?? ''),
        },
        owner: { email: (request?.details?.contact?.email ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.broadcastsReplayClipCalculationClient.quoteReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      venue_section: {
        children: [{
          audit: { updated_by: (request?.stream_key?.audit?.updated_by ?? '') },
          children: [{
            audit: { created_by: (request?.stream_key?.audit?.created_by ?? '') },
            manager: {
              email: (request?.details?.contact?.email ?? ''),
              user_id: (reply1?.replay_clip?.owner?.user_id ?? ''),
            },
          }],
        }],
      },
    };
    const validateVenueSection2 = promisify(clients.venuesVenueSectionValidationClient.validateVenueSection.bind(clients.venuesVenueSectionValidationClient));
    const reply2 = await validateVenueSection2(request2, metadataFor(request2.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        outcome: { reference_id: (reply2?.outcome?.reference_id ?? '') },
        stream_key: { limits: [{ name: (reply2?.venue_section?.name ?? '') }] },
      },
      {
        stream_key: {
          contact: request?.stream_key?.contact,
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  /**
   * Previews a stream key.
   */
  async function previewStreamKey(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'PreviewStreamKey', request);
    logger.info('preview_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const reply1 = await call((done) => clients.notificationsNotificationClient.batchGetNotifications(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          name: request?.details?.contact?.name,
          phone: (request?.details?.contact?.phone ?? ''),
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const method2 = 'lintBroadcasts';
    const reply2 = await call((done) => clients.broadcastsBroadcastValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      venue_policy: {
        audit: {
          created_by: (request?.stream_key?.audit?.created_by ?? ''),
          updated_at: request?.stream_key?.audit?.updated_at,
        },
        owner: {
          email: (request?.stream_key?.contact?.email ?? ''),
          user_id: (reply2?.broadcast?.organizer?.user_id ?? ''),
        },
      },
    };
    const client3 = clients.registry.get('acme.venues.v1.VenuePolicySyncService');
    const reply3 = await call((done) => client3.syncVenuePolicies(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      },
      {
        outcome: { effective_at: reply2?.outcome?.effective_at },
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
      },
      {
        outcome: {
          effective_at: reply3?.outcome?.effective_at,
          status: (reply3?.result?.status ?? ''),
        },
      },
      {
        stream_key: {
          contact: request?.stream_key?.contact,
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  /**
   * Scores a stream key.
   */
  async function scoreStreamKey(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'ScoreStreamKey', request);
    logger.info('score_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.stream_key?.contact?.email ?? '') } },
    };
    const send1 = clients.venuesVenuePolicyValidationClient.validateVenuePolicy.bind(clients.venuesVenuePolicyValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        destination: { country_code: (request?.details?.destination?.country_code ?? '') },
      },
      replay_clip: {
        audit: { created_by: (request?.stream_key?.audit?.created_by ?? '') },
        owner: { display_name: (reply1?.venue_policy?.owner?.display_name ?? '') },
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.broadcastsReplayClipCalculationClient.estimateReplayClip(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        stream_key: {
          audit: {
            created_at: reply1?.venue_policy?.audit?.created_at,
            created_by: (reply1?.venue_policy?.audit?.created_by ?? ''),
          },
        },
      },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        stream_key: {
          currency_code: (request?.details?.currency_code ?? ''),
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  /**
   * Validates a stream key.
   */
  async function validateStreamKey(request) {
    validateContext(request.context);
    if (request.stream_key) {
      await store.save(store.toStreamKeyRecord(request.context.tenant_id, request.stream_key));
    }
    await store.record(request.context.tenant_id, 'ValidateStreamKey', request);
    logger.info('validate_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.stream_key?.limits ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        replay_clip: { name: (item?.name ?? '') },
      };
      const reply1 = await call((done) => clients.broadcastsReplayClipCalculationClient.calculateReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    let selected2_1;
    switch (reply1?.replay_clip?.locator) {
      case 'address':
        selected2_1 = ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.locality ?? '');
        break;
      default:
        selected2_1 = '';
    }
    let selected2_4;
    switch (reply1?.replay_clip?.locator) {
      case 'address':
        selected2_4 = ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? '');
        break;
      default:
        selected2_4 = '';
    }
    const request2 = {
      context: request?.context,
      details: { destination: { locality: selected2_1 } },
      replay_clip: {
        address: { region: selected2_4 },
        audit: { etag: (request?.stream_key?.audit?.etag ?? '') },
        owner: { display_name: (request?.stream_key?.holder?.display_name ?? '') },
      },
    };
    const reply2 = await call((done) => clients.broadcastsReplayClipCalculationClient.simulateReplayClip(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: {
        destination: {
          recipient: ((reply2?.replay_clip?.locator === 'address' ? reply2?.replay_clip.address : undefined)?.recipient ?? ''),
        },
      },
      replay_clip: {
        address: {
          region: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? ''),
        },
        audit: { etag: (reply1?.replay_clip?.audit?.etag ?? '') },
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => clients.broadcastsReplayClipCalculationClient.quoteReplayClip(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = {
      broadcast: {
        participants: [{ contact: { phone: (request?.stream_key?.contact?.phone ?? '') } }],
      },
      context: request?.context,
    };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => clients.broadcastsBroadcastValidationClient.checkBroadcastEligibility(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        stream_key: {
          audit: { created_by: (reply3?.replay_clip?.audit?.created_by ?? '') },
          contact: { email: (reply3?.replay_clip?.owner?.email ?? '') },
        },
      },
      {
        result: {
          correlation_id: (reply4?.result?.correlation_id ?? ''),
          status: (reply4?.result?.status ?? ''),
        },
      },
      {
        stream_key: {
          currency_code: (request?.stream_key?.currency_code ?? ''),
          stream_key_id: (request?.stream_key_id ?? ''),
        },
      },
    );
  }

  return {
    checkStreamKeyEligibility: unary(checkStreamKeyEligibility),
    explainStreamKeyDecision: unary(explainStreamKeyDecision),
    getStreamKeyRules: unary(getStreamKeyRules),
    lintStreamKeys: unary(lintStreamKeys),
    normalizeStreamKey: unary(normalizeStreamKey),
    previewStreamKey: unary(previewStreamKey),
    scoreStreamKey: unary(scoreStreamKey),
    validateStreamKey: unary(validateStreamKey),
  };
}

module.exports = { createStreamKeyValidationHandlers };
