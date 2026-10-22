'use strict';

/**
 * Runs the long-lived workflow that takes a broadcast from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { upload } = require('../../../platform/streams');

/**
 * The handlers of BroadcastWorkflowService, over the dependencies the broadcasts team is given.
 */
function createBroadcastWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a broadcast.
   */
  async function advanceBroadcastWorkflow(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await store.save(store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await store.record(request.context.tenant_id, 'AdvanceBroadcastWorkflow', request);
    logger.info('advance_broadcast_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        participants: [{
          contact: {
            preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
          },
        }],
        weekly: {
          interval: ((request?.broadcast?.recurrence === 'daily' ? request?.broadcast.daily : undefined)?.interval ?? 0),
        },
      },
    };
    const send1 = clients.showsShowCalculationClient.recalculateShows.bind(clients.showsShowCalculationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const settled2 = await Promise.allSettled((request?.broadcast?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        details: { approver: { email: (item?.contact?.email ?? '') } },
        stream_key: {
          contact: {
            name: item?.contact?.name,
            phone: (item?.contact?.phone ?? ''),
          },
        },
      };
      const reply2 = await call((done) => clients.broadcastsStreamKeyApprovalClient.commentStreamKeyApproval(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        broadcast: {
          broadcast_id: (request?.broadcast_id ?? ''),
          location: request?.broadcast?.location,
        },
      },
    );
  }

  /**
   * Cancels a broadcast.
   */
  async function cancelBroadcastWorkflow(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await store.save(store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await store.record(request.context.tenant_id, 'CancelBroadcastWorkflow', request);
    logger.info('cancel_broadcast_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { phone: (request?.details?.notify?.phone ?? '') } },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.featureflagsFeatureFlagValidationClient.validateFeatureFlag(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const results2 = [];
    for (const item of (request?.broadcast?.participants ?? [])) {
      const request2 = {
        context: request?.context,
        performance: {
          participants: [{
            contact: {
              email: (item?.contact?.email ?? ''),
              phone: (item?.contact?.phone ?? ''),
            },
          }],
        },
      };
      const reply2 = await call((done) => clients.showsPerformanceCalculationClient.explainPerformanceCalculation(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      results2.push(reply2);
    }
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.outcome?.message ?? '') },
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      },
      {
        broadcast: {
          broadcast_id: (request?.broadcast_id ?? ''),
          window: request?.broadcast?.window,
        },
      },
    );
  }

  /**
   * Returns a broadcast.
   */
  async function inspectBroadcastWorkflow(request) {
    validateContext(request.context);
    if (!request.broadcast_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'broadcast_id is required');
    }
    await store.record(request.context.tenant_id, 'InspectBroadcastWorkflow', request);
    logger.info('inspect_broadcast_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      filter: {
        created_after: input1?.context?.deadline,
        labels: { ...(input1?.context?.baggage ?? {}) },
        owner_id: (input1?.context?.actor?.user_id ?? ''),
      },
    };
    const reply1 = await call((done) => clients.showsShowQueryClient.suggestShows(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      { broadcast: { broadcast_id: (request?.broadcast_id ?? '') } },
    );
  }

  /**
   * Pauses a broadcast.
   */
  async function pauseBroadcastWorkflow(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await store.save(store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await store.record(request.context.tenant_id, 'PauseBroadcastWorkflow', request);
    logger.info('pause_broadcast_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          email: (request?.details?.notify?.email ?? ''),
          name: request?.details?.notify?.name,
        },
      },
      stream_key: {
        audit: { updated_by: (request?.broadcast?.audit?.updated_by ?? '') },
        contact: {
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.broadcastsStreamKeyValidationClient.normalizeStreamKey(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      activation: {
        audit: { updated_at: request?.broadcast?.audit?.updated_at },
        requester: { name: request?.details?.notify?.name },
      },
      context: request?.context,
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.sponsorshipsActivationCalculationClient.estimateActivation(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        broadcast: { audit: { created_at: reply1?.stream_key?.audit?.created_at } },
        result: { status: (reply1?.outcome?.status ?? '') },
      },
      {
        broadcast: {
          participants: [{ contact: { name: reply2?.activation?.requester?.name } }],
        },
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
   * Resumes a broadcast.
   */
  async function resumeBroadcastWorkflow(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await store.save(store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await store.record(request.context.tenant_id, 'ResumeBroadcastWorkflow', request);
    logger.info('resume_broadcast_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.broadcast?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        details: {
          contact: {
            email: (item?.contact?.email ?? ''),
            name: item?.contact?.name,
            phone: (item?.contact?.phone ?? ''),
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply1 = await call((done) => clients.showsLineupValidationClient.checkLineupEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      stream_key: {
        contact: { email: (request?.details?.notify?.email ?? '') },
        holder: { display_name: (reply1?.lineup?.counterparty?.display_name ?? '') },
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.broadcastsStreamKeyApprovalClient.commentStreamKeyApproval(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.broadcast?.audit?.updated_at,
        labels: { ...(reply2?.stream_key?.labels ?? {}) },
        region: (request?.broadcast?.location?.region ?? ''),
      },
    };
    const reply3 = await withRetry(() => call((done) => clients.showsShowClient.searchShows(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const settled4 = await Promise.allSettled((request?.broadcast?.participants ?? []).map(async (item) => {
      const request4 = {
        context: request?.context,
        stream_key: {
          contact: {
            email: (item?.contact?.email ?? ''),
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply4 = await call((done) => clients.broadcastsStreamKeyApprovalClient.recallStreamKey(request4, metadataFor(request4.context), { deadline: deadline() }, done));
      return reply4;
    }));
    const results4 = settled4.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply4 = results4.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        broadcast: { audit: { updated_by: (reply2?.stream_key?.audit?.updated_by ?? '') } },
      },
      {
        outcome: { status: (reply3?.result?.status ?? '') },
        result: { status: (reply3?.result?.status ?? '') },
      },
      {
        broadcast: {
          broadcast_id: (request?.broadcast_id ?? ''),
          metadata: (request?.broadcast?.metadata ?? {}),
        },
      },
    );
  }

  /**
   * Signals a broadcast.
   */
  async function signalBroadcastWorkflow(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await store.save(store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await store.record(request.context.tenant_id, 'SignalBroadcastWorkflow', request);
    logger.info('signal_broadcast_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.broadcast?.location?.region ?? '') },
    };
    const task1 = deferred(() => call((done) => clients.broadcastsStreamKeyQueryClient.findStreamKeys(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      replay_clip: {
        address: { lines: [...(request?.broadcast?.location?.lines ?? [])] },
        owner: { display_name: (request?.broadcast?.organizer?.display_name ?? '') },
      },
    };
    const reply2 = await bus.request('acme.broadcasts.v1.BroadcastWorkflowService/SignalBroadcastWorkflow:1', request2);
    const request3 = {
      broadcast: {
        location: {
          country_code: ((reply2?.replay_clip?.locator === 'address' ? reply2?.replay_clip.address : undefined)?.country_code ?? ''),
        },
        participants: [{
          contact: {
            preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
          },
        }],
      },
      context: request?.context,
    };
    const reply3 = await runInContext(request.context, () => call((done) => clients.broadcastsBroadcastApprovalClient.delegateBroadcastApproval(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        broadcast: { audit: { created_at: reply2?.replay_clip?.audit?.created_at } },
      },
      { result: { message: (reply3?.outcome?.message ?? '') } },
      {
        broadcast: {
          broadcast_id: (request?.broadcast_id ?? ''),
          organizer: request?.broadcast?.organizer,
        },
      },
    );
  }

  /**
   * Starts a broadcast.
   */
  async function startBroadcastWorkflow(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await store.save(store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await store.record(request.context.tenant_id, 'StartBroadcastWorkflow', request);
    logger.info('start_broadcast_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          email: (request?.details?.notify?.email ?? ''),
          phone: (request?.details?.notify?.phone ?? ''),
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await call((done) => clients.admissionsGateScanCalculationClient.explainGateScanCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      header: {
        source: (request?.broadcast?.broadcast_id ?? ''),
        submitted_by: request?.details?.notify,
      },
    };
    const reply2 = await upload(
      (done) => clients.broadcastsBroadcastClient.batchCreateBroadcasts(metadataFor(request2.context), { deadline: deadline() }, done),
      [request2],
    );
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        broadcast: {
          organizer: { display_name: (reply1?.gate_scan?.subject?.display_name ?? '') },
        },
        outcome: { status: (reply1?.outcome?.status ?? '') },
      },
      {
        broadcast: {
          broadcast_id: (request?.broadcast_id ?? ''),
          metadata: (request?.broadcast?.metadata ?? {}),
        },
      },
    );
  }

  return {
    advanceBroadcastWorkflow: unary(advanceBroadcastWorkflow),
    cancelBroadcastWorkflow: unary(cancelBroadcastWorkflow),
    inspectBroadcastWorkflow: unary(inspectBroadcastWorkflow),
    pauseBroadcastWorkflow: unary(pauseBroadcastWorkflow),
    resumeBroadcastWorkflow: unary(resumeBroadcastWorkflow),
    signalBroadcastWorkflow: unary(signalBroadcastWorkflow),
    startBroadcastWorkflow: unary(startBroadcastWorkflow),
  };
}

module.exports = { createBroadcastWorkflowHandlers };
