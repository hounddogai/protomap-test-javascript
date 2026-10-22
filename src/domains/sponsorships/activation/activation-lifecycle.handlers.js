'use strict';

/**
 * Moves an activation between the states of its lifecycle.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of ActivationLifecycleService, over the dependencies the sponsorships team is given.
 */
function createActivationLifecycleHandlers({ store, clients, bus, logger }) {
  /**
   * Activates an activation.
   */
  async function activateActivation(request) {
    validateContext(request.context);
    if (!request.activation_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'activation_id is required');
    }
    await store.record(request.context.tenant_id, 'ActivateActivation', request);
    logger.info('activate_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        contact: { name: request?.details?.notify_contact?.name },
        relationship_manager: {
          email: (request?.details?.new_owner?.email ?? ''),
          user_id: (request?.details?.new_owner?.user_id ?? ''),
        },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.sponsorshipsSponsorClient.cloneSponsor(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { phone: (reply1?.sponsor?.contact?.phone ?? '') } },
      metric_series: {
        audit: { created_by: (reply1?.sponsor?.audit?.created_by ?? '') },
        owner: {
          email: (request?.details?.new_owner?.email ?? ''),
          user_id: (request?.details?.new_owner?.user_id ?? ''),
        },
      },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.metricsMetricSeriesValidationClient.normalizeMetricSeries(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        activation: {
          audit: { updated_by: (reply1?.sponsor?.audit?.updated_by ?? '') },
          requester: { email: (reply1?.sponsor?.relationship_manager?.email ?? '') },
        },
      },
      {
        activation: { audit: { created_at: reply2?.metric_series?.audit?.created_at } },
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
      },
      {
        activation: { activation_id: (request?.activation_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Expires an activation.
   */
  async function expireActivation(request) {
    validateContext(request.context);
    if (!request.activation_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'activation_id is required');
    }
    await store.record(request.context.tenant_id, 'ExpireActivation', request);
    logger.info('expire_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          email: (request?.details?.notify_contact?.email ?? ''),
          phone: (request?.details?.notify_contact?.phone ?? ''),
        },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.sponsorshipsSponsorshipDealValidationClient.lintSponsorshipDeals(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') } },
      {
        activation: { activation_id: (request?.activation_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Returns an activation.
   */
  async function getActivationLifecycle(request) {
    validateContext(request.context);
    if (!request.activation_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'activation_id is required');
    }
    await store.record(request.context.tenant_id, 'GetActivationLifecycle', request);
    logger.info('get_activation_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      collection_case: { assignee: { user_id: (request?.context?.actor?.user_id ?? '') } },
      context: request?.context,
    };
    const reply1 = await withRetry(() => call((done) => clients.collectionsCollectionCaseClient.updateCollectionCase(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    return assemble(
      { activation: { subject: (reply1?.collection_case?.subject ?? '') } },
      { activation: { activation_id: (request?.activation_id ?? '') } },
    );
  }

  /**
   * Reinstates an activation.
   */
  async function reinstateActivation(request) {
    validateContext(request.context);
    if (!request.activation_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'activation_id is required');
    }
    await store.record(request.context.tenant_id, 'ReinstateActivation', request);
    logger.info('reinstate_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      activation_id: (request?.activation_id ?? ''),
      context: request?.context,
    };
    const task1 = deferred(() => call((done) => clients.sponsorshipsActivationCalculationClient.getActivationRates(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      activation: {
        audit: { updated_by: (reply1?.activation?.audit?.updated_by ?? '') },
        channel: {
          address: ((reply1?.activation?.origin === 'channel' ? reply1?.activation.channel : undefined)?.address ?? ''),
        },
        sla: {
          due_at: reply1?.activation?.sla?.due_at,
          policy_id: (reply1?.activation?.sla?.policy_id ?? ''),
        },
      },
      context: request?.context,
    };
    const reply2 = await bus.request('acme.sponsorships.v1.ActivationLifecycleService/ReinstateActivation:1', request2);
    const request3 = {
      context: request?.context,
      details: {
        approver: {
          display_name: (reply1?.activation?.assignee?.display_name ?? ''),
          email: (reply1?.activation?.requester?.email ?? ''),
        },
      },
      replay_clip: { owner: { user_id: (request?.details?.new_owner?.user_id ?? '') } },
    };
    const reply3 = await runInContext(request.context, () => call((done) => clients.broadcastsReplayClipApprovalClient.delegateReplayClipApproval(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        activation: { sla: { paused_for: reply1?.activation?.sla?.paused_for } },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        activation: { requester: { email: (reply3?.replay_clip?.owner?.email ?? '') } },
      },
      {
        activation: { activation_id: (request?.activation_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Renews an activation.
   */
  async function renewActivation(request) {
    validateContext(request.context);
    if (!request.activation_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'activation_id is required');
    }
    await store.record(request.context.tenant_id, 'RenewActivation', request);
    logger.info('renew_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { approver: { email: (request?.details?.notify_contact?.email ?? '') } },
      replay_clip: {
        owner: {
          email: (request?.details?.new_owner?.email ?? ''),
          user_id: (request?.details?.new_owner?.user_id ?? ''),
        },
      },
    };
    const reply1 = await call((done) => clients.broadcastsReplayClipApprovalClient.escalateReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        activation: { comments: [{ created_at: reply1?.replay_clip?.audit?.created_at }] },
      },
      {
        activation: { activation_id: (request?.activation_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Schedules an activation.
   */
  async function scheduleActivationExpiry(request) {
    validateContext(request.context);
    if (request.activation) {
      await store.save(store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await store.record(request.context.tenant_id, 'ScheduleActivationExpiry', request);
    logger.info('schedule_activation_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        relationship_manager: { email: (request?.activation?.requester?.email ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.sponsorshipsSponsorValidationClient.scoreSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const settled2 = await Promise.allSettled((request?.activation?.comments ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        venue: { audit: { created_at: item?.created_at } },
      };
      const reply2 = await call((done) => clients.venuesVenueHistoryClient.annotateVenueVersion(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      sponsor: {
        primary_address: { postal_code: (reply1?.sponsor?.primary_address?.postal_code ?? '') },
      },
    };
    const previewSponsor3 = promisify(clients.sponsorshipsSponsorValidationClient.previewSponsor.bind(clients.sponsorshipsSponsorValidationClient));
    const reply3 = await previewSponsor3(request3, metadataFor(request3.context), { deadline: deadline() });
    const request4 = {
      activation: { requester: { phone: (request?.details?.notify?.phone ?? '') } },
      context: request?.context,
      details: { contact: { name: request?.activation?.requester?.name } },
    };
    const reply4 = await call((done) => clients.sponsorshipsActivationValidationClient.validateActivation(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { activation: { requester: { name: reply1?.sponsor?.name } } },
      {
        activation: { audit: { updated_at: reply3?.sponsor?.audit?.updated_at } },
        outcome: { message: (reply3?.result?.message ?? '') },
      },
      {
        activation: { assignee: { email: (reply4?.activation?.assignee?.email ?? '') } },
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
   * Suspends an activation.
   */
  async function suspendActivation(request) {
    validateContext(request.context);
    if (!request.activation_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'activation_id is required');
    }
    await store.record(request.context.tenant_id, 'SuspendActivation', request);
    logger.info('suspend_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { created_after: request?.details?.effective_at },
    };
    const method1 = 'countFeatureFlags';
    const reply1 = await call((done) => clients.featureflagsFeatureFlagQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    };
    const messages2 = await collect(clients.sponsorshipsSponsorshipDealQueryClient.exportSponsorshipDeals(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        activation: { activation_id: (request?.activation_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Terminates an activation.
   */
  async function terminateActivation(request) {
    validateContext(request.context);
    if (!request.activation_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'activation_id is required');
    }
    await store.record(request.context.tenant_id, 'TerminateActivation', request);
    logger.info('terminate_activation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      activation: {
        assignee: { email: (request?.details?.notify_contact?.email ?? '') },
        requester: { email: (request?.details?.new_owner?.email ?? '') },
      },
      context: request?.context,
    };
    const client1 = clients.registry.get('acme.sponsorships.v1.ActivationCalculationService');
    const reply1 = await call((done) => client1.calculateActivation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        activation: { activation_id: (reply1?.activation?.activation_id ?? '') },
      },
      {
        activation: {
          assignee: { display_name: (request?.details?.new_owner?.display_name ?? '') },
        },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  return {
    activateActivation: unary(activateActivation),
    expireActivation: unary(expireActivation),
    getActivationLifecycle: unary(getActivationLifecycle),
    reinstateActivation: unary(reinstateActivation),
    renewActivation: unary(renewActivation),
    scheduleActivationExpiry: unary(scheduleActivationExpiry),
    suspendActivation: unary(suspendActivation),
    terminateActivation: unary(terminateActivation),
  };
}

module.exports = { createActivationLifecycleHandlers };
