'use strict';

/**
 * Runs the long-lived workflow that takes a replay clip from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { exchange, upload } = require('../../platform/streams');

/**
 * Serve ReplayClipWorkflowService, recording every operation of the broadcasts team.
 */
class ReplayClipWorkflowService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'ReplayClipWorkflowService' });
  }

  /**
   * Advances a replay clip.
   */
  advanceReplayClipWorkflow(call, callback) {
    this.executeAdvanceReplayClipWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeAdvanceReplayClipWorkflow(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'AdvanceReplayClipWorkflow', request);
    this.logger.info('advance_replay_clip_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_2;
    switch (request?.replay_clip?.locator) {
      case 'address':
        selected1_2 = ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.locality ?? '');
        break;
      default:
        selected1_2 = '';
    }
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
      lineup: { billing_address: { locality: selected1_2 } },
    };
    const reply1 = await call((done) => this.clients.showsLineupValidationClient.checkLineupEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { effective_at: reply1?.outcome?.effective_at } },
      {
        replay_clip: {
          replay_clip_id: (request?.replay_clip_id ?? ''),
          status: (request?.replay_clip?.status ?? ''),
        },
      },
    );
  }

  /**
   * Cancels a replay clip.
   */
  cancelReplayClipWorkflow(call, callback) {
    this.executeCancelReplayClipWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCancelReplayClipWorkflow(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'CancelReplayClipWorkflow', request);
    this.logger.info('cancel_replay_clip_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        recipient: {
          email: (request?.replay_clip?.owner?.email ?? ''),
          name: request?.details?.notify?.name,
          phone: (request?.details?.notify?.phone ?? ''),
        },
      },
    };
    const method1 = 'subscribeVenuePolicyUpdates';
    const reply1 = await call((done) => this.clients.venuesVenuePolicyNotificationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      document: {
        audit: {
          created_at: request?.replay_clip?.audit?.created_at,
          updated_by: (reply1?.venue_policy?.audit?.updated_by ?? ''),
        },
        lines: [{
          tax: {
            amount: (reply1?.venue_policy?.action === 'discount' ? reply1?.venue_policy.discount : undefined)?.amount,
          },
        }],
      },
    };
    const client2 = this.clients.registry.get('acme.documents.v1.DocumentService');
    const reply2 = await call((done) => client2.createDocument(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    let selected3_2;
    switch (reply2?.document?.settlement) {
      case 'card':
        selected3_2 = ((reply2?.document?.settlement === 'card' ? reply2?.document.card : undefined)?.holder_name ?? '');
        break;
      default:
        selected3_2 = '';
    }
    const request3 = {
      cash_drawer: {
        audit: {
          etag: (reply1?.venue_policy?.audit?.etag ?? ''),
          updated_by: (request?.replay_clip?.audit?.updated_by ?? ''),
        },
        card: { holder_name: selected3_2 },
        contact: {
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
      context: request?.context,
    };
    const reply3 = await call((done) => this.clients.boxofficeCashDrawerValidationClient.validateCashDrawer(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        replay_clip: { audit: { created_at: reply1?.venue_policy?.audit?.created_at } },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        replay_clip: {
          audit: { updated_by: (reply2?.document?.audit?.updated_by ?? '') },
          owner: { display_name: (reply2?.document?.counterparty?.display_name ?? '') },
        },
      },
      { result: { message: (reply3?.outcome?.message ?? '') } },
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
  inspectReplayClipWorkflow(call, callback) {
    this.executeInspectReplayClipWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeInspectReplayClipWorkflow(request) {
    validateContext(request.context);
    if (!request.replay_clip_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'replay_clip_id is required');
    }
    await this.store.record(request.context.tenant_id, 'InspectReplayClipWorkflow', request);
    this.logger.info('inspect_replay_clip_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip_id: (request?.replay_clip_id ?? ''),
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.broadcastsReplayClipApprovalClient.delegateReplayClipApproval(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      broadcast: {
        audit: { created_by: (reply1?.replay_clip?.audit?.created_by ?? '') },
        location: {
          region: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? ''),
        },
        organizer: { display_name: (reply1?.replay_clip?.owner?.display_name ?? '') },
      },
      context: input2?.context,
    };
    const reply2 = await call((done) => this.clients.broadcastsBroadcastWorkflowClient.cancelBroadcastWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      header: {
        source: (request?.context?.actor?.user_id ?? ''),
        submitted_by: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const reply3 = await upload(
      (done) => this.clients.performersPerformerClient.batchCreatePerformers(metadataFor(request3.context), { deadline: deadline() }, done),
      [request3],
    );
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      { result: { details: (reply2?.result?.details ?? {}) } },
      { replay_clip: { replay_clip_id: (request?.replay_clip_id ?? '') } },
    );
  }

  /**
   * Lists replay clips a page at a time.
   */
  listReplayClipWorkflows(call, callback) {
    this.executeListReplayClipWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListReplayClipWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListReplayClipWorkflows', request);
    this.logger.info('list_replay_clip_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Pauses a replay clip.
   */
  pauseReplayClipWorkflow(call, callback) {
    this.executePauseReplayClipWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePauseReplayClipWorkflow(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'PauseReplayClipWorkflow', request);
    this.logger.info('pause_replay_clip_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.replay_clip?.locator) {
      case 'address':
        selected1_1 = ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.country_code ?? '');
        break;
      default:
        selected1_1 = '';
    }
    const request1 = {
      context: request?.context,
      details: { destination: { country_code: selected1_1 } },
    };
    const reply1 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.quoteStreamKey(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        replay_clip: { owner: { email: (reply1?.stream_key?.contact?.email ?? '') } },
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
   * Resumes a replay clip.
   */
  resumeReplayClipWorkflow(call, callback) {
    this.executeResumeReplayClipWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResumeReplayClipWorkflow(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'ResumeReplayClipWorkflow', request);
    this.logger.info('resume_replay_clip_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: {
        participants: [{
          contact: {
            email: (request?.replay_clip?.owner?.email ?? ''),
            name: request?.details?.notify?.name,
            preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
          },
        }],
      },
      context: request?.context,
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.broadcastsBroadcastWorkflowClient.advanceBroadcastWorkflow(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      stream_key: {
        audit: { etag: (request?.replay_clip?.audit?.etag ?? '') },
        contact: { phone: (request?.details?.notify?.phone ?? '') },
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.broadcastsStreamKeyWorkflowClient.resumeStreamKeyWorkflow(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: { notify_contact: { email: (reply2?.stream_key?.contact?.email ?? '') } },
      replay_clip_id: (request?.replay_clip?.replay_clip_id ?? ''),
    };
    const reply3 = await withRetry(() => call((done) => this.clients.broadcastsReplayClipLifecycleClient.reinstateReplayClip(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const request4 = {
      context: request?.context,
      performance: {
        participants: [{
          contact: {
            name: reply2?.stream_key?.contact?.name,
            preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const task4 = deferred(() => call((done) => this.clients.showsPerformanceCalculationClient.estimatePerformance(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    const reply4 = await task4();
    const request5 = {
      change: {
        children: [{ name: (request?.replay_clip?.name ?? '') }],
        name: (request?.replay_clip?.name ?? ''),
        parent_id: (request?.context?.actor?.user_id ?? ''),
      },
      context: request?.context,
    };
    const messages5 = await exchange(this.clients.venuesVenueSectionSyncClient.streamVenueSectionChanges(metadataFor(request5.context), { deadline: deadline() }), [request5]);
    const reply5 = messages5.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        replay_clip: { owner: { user_id: (reply1?.broadcast?.organizer?.user_id ?? '') } },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.result?.status ?? ''),
        },
      },
      { result: { status: (reply3?.result?.status ?? '') } },
      { result: { reference_ids: (reply4?.result?.reference_ids ?? []) } },
      {
        replay_clip: {
          replay_clip_id: (request?.replay_clip_id ?? ''),
          specs: (request?.replay_clip?.specs ?? {}),
        },
      },
    );
  }

  /**
   * Starts a replay clip.
   */
  startReplayClipWorkflow(call, callback) {
    this.executeStartReplayClipWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeStartReplayClipWorkflow(request) {
    validateContext(request.context);
    if (request.replay_clip) {
      await this.store.save(this.store.toReplayClipRecord(request.context.tenant_id, request.replay_clip));
    }
    await this.store.record(request.context.tenant_id, 'StartReplayClipWorkflow', request);
    this.logger.info('start_replay_clip_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          lines: [...((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.lines ?? [])],
          locality: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.locality ?? ''),
          postal_code: ((request?.replay_clip?.locator === 'address' ? request?.replay_clip.address : undefined)?.postal_code ?? ''),
        },
      },
      gate_scan: {
        measurements: [{ unit: (request?.replay_clip?.dimensions?.unit ?? '') }],
      },
    };
    const reply1 = await this.bus.request('acme.broadcasts.v1.ReplayClipWorkflowService/StartReplayClipWorkflow:0', request1);
    const request2 = {
      context: request?.context,
      performance: { organizer: { email: (request?.replay_clip?.owner?.email ?? '') } },
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.showsPerformanceValidationClient.checkPerformanceEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.replay_clip?.audit?.created_at,
        region: (reply2?.performance?.location?.region ?? ''),
      },
    };
    const reply3 = await call((done) => this.clients.broadcastsStreamKeyQueryClient.browseStreamKeys(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      broadcast: {
        daily: {
          interval: ((reply2?.performance?.recurrence === 'weekly' ? reply2?.performance.weekly : undefined)?.interval ?? 0),
        },
        location: { recipient: (reply2?.performance?.location?.recipient ?? '') },
        participants: [{ contact: { email: (request?.details?.notify?.email ?? '') } }],
      },
      context: request?.context,
    };
    const reply4 = await new Promise((resolve, reject) => {
      this.clients.broadcastsBroadcastValidationClient.lintBroadcasts(request4, metadataFor(request4.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        outcome: { status: (reply2?.outcome?.status ?? '') },
        replay_clip: { audit: { updated_at: reply2?.performance?.audit?.updated_at } },
      },
      { result: { message: (reply3?.result?.message ?? '') } },
      {
        outcome: { reference_id: (reply4?.outcome?.reference_id ?? '') },
        replay_clip: { audit: { updated_at: reply4?.broadcast?.audit?.updated_at } },
      },
      {
        replay_clip: {
          replay_clip_id: (request?.replay_clip_id ?? ''),
          specs: (request?.replay_clip?.specs ?? {}),
        },
      },
    );
  }
}

module.exports = { ReplayClipWorkflowService };
