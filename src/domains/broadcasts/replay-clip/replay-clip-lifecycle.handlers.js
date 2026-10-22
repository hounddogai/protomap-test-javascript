'use strict';

/**
 * Moves a replay clip between the states of its lifecycle.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect, exchange } = require('../../../platform/streams');

/**
 * The handlers of ReplayClipLifecycleService, over the dependencies the broadcasts team is given.
 */
function createReplayClipLifecycleHandlers({ store, clients, bus, logger }) {
  /**
   * Activates a replay clip.
   */
  async function activateReplayClip(request) {
    validateContext(request.context);
    if (!request.replay_clip_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'replay_clip_id is required');
    }
    await store.record(request.context.tenant_id, 'ActivateReplayClip', request);
    logger.info('activate_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: {
        owner: {
          email: (request?.details?.new_owner?.email ?? ''),
          user_id: (request?.details?.new_owner?.user_id ?? ''),
        },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.broadcastsReplayClipCalculationClient.recalculateReplayClips(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      show: {
        location: {
          lines: [...((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.lines ?? [])],
        },
        participants: [{
          contact: {
            phone: (request?.details?.notify_contact?.phone ?? ''),
            preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const reply2 = await withRetry(() => call((done) => clients.showsShowCalculationClient.estimateShow(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { replay_clip: { name: (reply1?.replay_clip?.name ?? '') } },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { status: (reply2?.result?.status ?? '') },
      },
      {
        outcome: { effective_at: request?.details?.effective_at },
        replay_clip: { replay_clip_id: (request?.replay_clip_id ?? '') },
      },
    );
  }

  /**
   * Expires a replay clip.
   */
  async function expireReplayClip(request) {
    validateContext(request.context);
    if (!request.replay_clip_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'replay_clip_id is required');
    }
    await store.record(request.context.tenant_id, 'ExpireReplayClip', request);
    logger.info('expire_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
        },
      },
    };
    const task1 = deferred(() => call((done) => clients.ledgerLedgerAccountValidationClient.checkLedgerAccountEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      details: { contact: { name: request?.details?.notify_contact?.name } },
    };
    const reply2 = await bus.request('acme.broadcasts.v1.ReplayClipLifecycleService/ExpireReplayClip:1', request2);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          effective_at: reply1?.outcome?.effective_at,
          reference_id: (reply1?.outcome?.reference_id ?? ''),
        },
      },
      {
        replay_clip: {
          components: { primary: {
            component_id: (reply2?.replay_clip?.components?.primary?.component_id ?? ''),
          } },
        },
      },
      {
        replay_clip: {
          owner: { email: (request?.details?.notify_contact?.email ?? '') },
          replay_clip_id: (request?.replay_clip_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a replay clip.
   */
  async function getReplayClipLifecycle(request) {
    validateContext(request.context);
    if (!request.replay_clip_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'replay_clip_id is required');
    }
    await store.record(request.context.tenant_id, 'GetReplayClipLifecycle', request);
    logger.info('get_replay_clip_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { filters: { ...(request?.context?.baggage ?? {}) } },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.broadcastsBroadcastQueryClient.summarizeBroadcasts(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      { replay_clip: { replay_clip_id: (request?.replay_clip_id ?? '') } },
    );
  }

  /**
   * Reinstates a replay clip.
   */
  async function reinstateReplayClip(request) {
    validateContext(request.context);
    if (!request.replay_clip_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'replay_clip_id is required');
    }
    await store.record(request.context.tenant_id, 'ReinstateReplayClip', request);
    logger.info('reinstate_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(request?.annotations ?? {}) },
        owner_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    };
    const messages1 = await collect(clients.showsPerformanceQueryClient.exportPerformances(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      show: {
        organizer: {
          display_name: (request?.details?.new_owner?.display_name ?? ''),
          email: (request?.details?.notify_contact?.email ?? ''),
        },
        participants: [{
          contact: {
            email: (request?.details?.new_owner?.email ?? ''),
            preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const reply2 = await call((done) => clients.showsShowCalculationClient.estimateShow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        replay_clip: { owner: { user_id: (reply2?.show?.organizer?.user_id ?? '') } },
      },
      {
        outcome: { effective_at: request?.details?.effective_at },
        replay_clip: { replay_clip_id: (request?.replay_clip_id ?? '') },
      },
    );
  }

  /**
   * Renews a replay clip.
   */
  async function renewReplayClip(request) {
    validateContext(request.context);
    if (!request.replay_clip_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'replay_clip_id is required');
    }
    await store.record(request.context.tenant_id, 'RenewReplayClip', request);
    logger.info('renew_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(request?.annotations ?? {}) },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.boxofficeCashDrawerQueryClient.suggestCashDrawers(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          message: (reply1?.result?.message ?? ''),
        },
      },
      {
        outcome: { effective_at: request?.details?.effective_at },
        replay_clip: { replay_clip_id: (request?.replay_clip_id ?? '') },
      },
    );
  }

  /**
   * Schedules a replay clip.
   */
  async function scheduleReplayClipExpiry(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await store.save(store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await store.record(request.context.tenant_id, 'ScheduleReplayClipExpiry', request);
    logger.info('schedule_replay_clip_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        location: {
          region: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.region ?? ''),
        },
      },
    };
    const recalculateShows1 = promisify(clients.showsShowCalculationClient.recalculateShows.bind(clients.showsShowCalculationClient));
    const reply1 = await recalculateShows1(request1, metadataFor(request1.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        replay_clip: {
          owner: request?.replay_clip?.owner,
          replay_clip_id: (request?.replay_clip_id ?? ''),
        },
      },
    );
  }

  /**
   * Suspends a replay clip.
   */
  async function suspendReplayClip(request) {
    validateContext(request.context);
    if (!request.replay_clip_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'replay_clip_id is required');
    }
    await store.record(request.context.tenant_id, 'SuspendReplayClip', request);
    logger.info('suspend_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          name: request?.details?.notify_contact?.name,
          phone: (request?.details?.notify_contact?.phone ?? ''),
          preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
        },
      },
      replay_clip: { owner: { email: (request?.details?.notify_contact?.email ?? '') } },
    };
    const reply1 = await call((done) => clients.broadcastsReplayClipValidationClient.previewReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { phone: (request?.details?.notify_contact?.phone ?? '') } },
    };
    const method2 = 'previewAuditEvent';
    const reply2 = await call((done) => clients.auditAuditEventValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        replay_clip: { tags: (reply1?.replay_clip?.tags ?? []) },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          details: (reply2?.result?.details ?? {}),
        },
      },
      {
        outcome: { effective_at: request?.details?.effective_at },
        replay_clip: { replay_clip_id: (request?.replay_clip_id ?? '') },
      },
    );
  }

  /**
   * Terminates a replay clip.
   */
  async function terminateReplayClip(request) {
    validateContext(request.context);
    if (!request.replay_clip_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'replay_clip_id is required');
    }
    await store.record(request.context.tenant_id, 'TerminateReplayClip', request);
    logger.info('terminate_replay_clip', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      user: {
        relationship_manager: {
          email: (request?.details?.new_owner?.email ?? ''),
          user_id: (request?.details?.new_owner?.user_id ?? ''),
        },
      },
    };
    const client1 = clients.registry.get('acme.identity.v1.UserService');
    const reply1 = await call((done) => client1.createUser(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { period: reply1?.user?.preferences?.quiet_hours },
      reason: (request?.reason ?? ''),
    };
    const send2 = clients.sponsorshipsSponsorQueryClient.summarizeSponsors.bind(clients.sponsorshipsSponsorQueryClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.user?.audit?.updated_at,
        owner_id: (reply1?.user?.relationship_manager?.user_id ?? ''),
        region: (reply1?.user?.primary_address?.region ?? ''),
      },
    };
    const messages3 = await collect(clients.broadcastsBroadcastQueryClient.exportBroadcasts(request3, metadataFor(request3.context), { deadline: deadline() }));
    const reply3 = messages3.at(-1) ?? {};
    const request4 = {
      change: {
        children: [{
          children: [{ parent_id: (request?.context?.actor?.user_id ?? '') }],
          parent_id: (request?.context?.actor?.user_id ?? ''),
        }],
        parent_id: (request?.context?.actor?.user_id ?? ''),
      },
      context: request?.context,
    };
    const messages4 = await exchange(clients.venuesVenueSectionSyncClient.streamVenueSectionChanges(metadataFor(request4.context), { deadline: deadline() }), [request4]);
    const reply4 = messages4.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.details?.notify_contact?.preferred_channel ?? '') });
    return assemble(
      {
        replay_clip: {
          audit: {
            created_by: (reply1?.user?.audit?.created_by ?? ''),
            etag: (reply1?.user?.audit?.etag ?? ''),
          },
        },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        outcome: { effective_at: request?.details?.effective_at },
        replay_clip: { replay_clip_id: (request?.replay_clip_id ?? '') },
      },
    );
  }

  return {
    activateReplayClip: unary(activateReplayClip),
    expireReplayClip: unary(expireReplayClip),
    getReplayClipLifecycle: unary(getReplayClipLifecycle),
    reinstateReplayClip: unary(reinstateReplayClip),
    renewReplayClip: unary(renewReplayClip),
    scheduleReplayClipExpiry: unary(scheduleReplayClipExpiry),
    suspendReplayClip: unary(suspendReplayClip),
    terminateReplayClip: unary(terminateReplayClip),
  };
}

module.exports = { createReplayClipLifecycleHandlers };
