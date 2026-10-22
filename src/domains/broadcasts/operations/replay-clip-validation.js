'use strict';

/**
 * Checks replay clips against the rules before anything acts on them. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  auditAuditEventQueryClient,
  broadcastsBroadcastValidationClient,
  broadcastsReplayClipCalculationClient,
  ledgerLedgerAccountQueryClient,
  notificationsNotificationValidationClient,
  venuesVenuePolicyHistoryClient,
  venuesVenueValidationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Checks a replay clip.
 */
async function checkReplayClipEligibility(request) {
  validateContext(request.context);
  if (request.replay_clip) {
    await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
  }
  await store.record(request.context.tenant_id, 'CheckReplayClipEligibility', request);
  logger.info('check_replay_clip_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue: {
      components: { primary: {
        description: (request?.replay_clip?.components?.primary?.description ?? ''),
      } },
      owner: { display_name: (request?.replay_clip?.owner?.display_name ?? '') },
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => venuesVenueValidationClient.lintVenues(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = { context: input2?.context };
  const reply2 = await call((done) => ledgerLedgerAccountQueryClient.resolveLedgerAccounts(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    venue_policy: {
      audit: { created_at: reply1?.venue?.audit?.created_at },
      description: (reply1?.venue?.components?.primary?.description ?? ''),
      owner: { email: (reply1?.venue?.owner?.email ?? '') },
    },
  };
  const cloned3 = JSON.parse(JSON.stringify(request3));
  const reply3 = await call((done) => venuesVenuePolicyHistoryClient.recordVenuePolicyVersion(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
  const request4 = {
    broadcast: {
      participants: [{
        contact: {
          email: (reply1?.venue?.owner?.email ?? ''),
          name: request?.details?.contact?.name,
        },
      }],
    },
    context: request?.context,
  };
  const copied4 = structuredClone(request4);
  const reply4 = await call((done) => broadcastsBroadcastValidationClient.normalizeBroadcast(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      replay_clip: { dimensions: { length: (reply1?.venue?.dimensions?.length ?? 0) } },
    },
    {
      outcome: { status: (reply2?.result?.status ?? '') },
      result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
    },
    {
      replay_clip: {
        owner: { display_name: (reply3?.venue_policy?.owner?.display_name ?? '') },
      },
    },
    {
      outcome: { next_steps: (reply4?.outcome?.next_steps ?? []) },
      result: { status: (reply4?.outcome?.status ?? '') },
    },
    {
      replay_clip: {
        replay_clip_id: (request?.replay_clip_id ?? ''),
        specs: (request?.replay_clip?.specs ?? {}),
      },
    },
  );
}

/**
 * Explains a replay clip.
 */
async function explainReplayClipDecision(request) {
  validateContext(request.context);
  if (request.replay_clip) {
    await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
  }
  await store.record(request.context.tenant_id, 'ExplainReplayClipDecision', request);
  logger.info('explain_replay_clip_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    broadcast: {
      participants: [{
        contact: {
          email: (request?.details?.contact?.email ?? ''),
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      }],
    },
    context: request?.context,
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => broadcastsBroadcastValidationClient.checkBroadcastEligibility(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      replay_clip: {
        owner: { display_name: (reply1?.broadcast?.organizer?.display_name ?? '') },
      },
      result: { status: (reply1?.outcome?.status ?? '') },
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
 * Returns a replay clip.
 */
async function getReplayClipRules(request) {
  validateContext(request.context);
  if (!request.replay_clip_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'replay_clip_id is required');
  }
  await store.record(request.context.tenant_id, 'GetReplayClipRules', request);
  logger.info('get_replay_clip_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    replay_clip: {
      audit: { updated_at: request?.context?.deadline },
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
      replay_clip_id: (request?.replay_clip_id ?? ''),
    },
  };
  const reply1 = await withRetry(() => call((done) => broadcastsReplayClipCalculationClient.estimateReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  return assemble(
    {
      replay_clip: { dimensions: { unit: (reply1?.replay_clip?.dimensions?.unit ?? '') } },
      result: { status: (reply1?.result?.status ?? '') },
    },
    { replay_clip: { replay_clip_id: (request?.replay_clip_id ?? '') } },
  );
}

/**
 * Normalizes a replay clip.
 */
async function normalizeReplayClip(request) {
  validateContext(request.context);
  if (request.replay_clip) {
    await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
  }
  await store.record(request.context.tenant_id, 'NormalizeReplayClip', request);
  logger.info('normalize_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    broadcast: {
      participants: [{ contact: { email: (request?.details?.contact?.email ?? '') } }],
    },
    context: request?.context,
  };
  const task1 = deferred(() => call((done) => broadcastsBroadcastValidationClient.checkBroadcastEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      replay_clip: {
        owner: {
          email: (reply1?.broadcast?.organizer?.email ?? ''),
          user_id: (reply1?.broadcast?.organizer?.user_id ?? ''),
        },
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
 * Previews a replay clip.
 */
async function previewReplayClip(request) {
  validateContext(request.context);
  if (request.replay_clip) {
    await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
  }
  await store.record(request.context.tenant_id, 'PreviewReplayClip', request);
  logger.info('preview_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.replay_clip?.audit?.updated_at,
      region: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.region ?? ''),
    },
  };
  const messages1 = await collect(ledgerLedgerAccountQueryClient.exportLedgerAccounts(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    broadcast: {
      audit: { etag: (request?.replay_clip?.audit?.etag ?? '') },
      organizer: { email: (request?.replay_clip?.owner?.email ?? '') },
      participants: [{
        contact: {
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      }],
    },
    context: request?.context,
  };
  const reply2 = await bus.request('acme.broadcasts.v1.ReplayClipValidationService/PreviewReplayClip:1', request2);
  const request3 = {
    context: request?.context,
    filter: {
      created_after: reply2?.broadcast?.audit?.updated_at,
      labels: { ...(reply2?.result?.details ?? {}) },
      owner_id: (request?.replay_clip?.components?.primary?.component_id ?? ''),
      region: (reply2?.broadcast?.location?.region ?? ''),
    },
  };
  const reply3 = await runInContext(request.context, () => call((done) => auditAuditEventQueryClient.findAuditEvents(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { message: (reply2?.result?.message ?? '') } },
    { result: { message: (reply3?.result?.message ?? '') } },
    {
      replay_clip: {
        name: (request?.replay_clip?.name ?? ''),
        replay_clip_id: (request?.replay_clip_id ?? ''),
      },
    },
  );
}

/**
 * Validates a replay clip.
 */
async function validateReplayClip(request) {
  validateContext(request.context);
  if (request.replay_clip) {
    await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
  }
  await store.record(request.context.tenant_id, 'ValidateReplayClip', request);
  logger.info('validate_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    broadcast: {
      audit: {
        created_at: request?.replay_clip?.audit?.created_at,
        etag: (request?.replay_clip?.audit?.etag ?? ''),
      },
      organizer: { display_name: (request?.replay_clip?.owner?.display_name ?? '') },
    },
    context: request?.context,
    details: { contact: { phone: (request?.details?.contact?.phone ?? '') } },
  };
  const reply1 = await call((done) => clients.broadcastsBroadcastValidationClient.lintBroadcasts(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    broadcast: {
      audit: { updated_at: request?.replay_clip?.audit?.updated_at },
      organizer: { email: (reply1?.broadcast?.organizer?.email ?? '') },
      participants: [{ contact: { phone: (request?.details?.contact?.phone ?? '') } }],
    },
    context: request?.context,
    details: { destination: { region: (reply1?.broadcast?.location?.region ?? '') } },
  };
  const reply2 = await new Promise((resolve, reject) => {
    broadcastsBroadcastValidationClient.scoreBroadcast(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request3 = {
    context: request?.context,
    notification: { audit: { created_at: request?.replay_clip?.audit?.created_at } },
  };
  const validateNotification3 = promisify(notificationsNotificationValidationClient.validateNotification.bind(notificationsNotificationValidationClient));
  const reply3 = await validateNotification3(request3, metadataFor(request3.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      replay_clip: { audit: { created_by: (reply1?.broadcast?.audit?.created_by ?? '') } },
    },
    {
      replay_clip: {
        owner: { display_name: (reply2?.broadcast?.organizer?.display_name ?? '') },
      },
    },
    {
      replay_clip: {
        audit: { updated_by: (reply3?.notification?.audit?.updated_by ?? '') },
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

module.exports = { checkReplayClipEligibility, explainReplayClipDecision, getReplayClipRules, normalizeReplayClip, previewReplayClip, validateReplayClip };
