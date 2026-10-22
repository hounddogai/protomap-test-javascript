'use strict';

/**
 * Routes broadcasts through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect, exchange } = require('../../platform/streams');

/**
 * Serve BroadcastApprovalService, recording every operation of the broadcasts team.
 */
class BroadcastApprovalService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'BroadcastApprovalService' });
  }

  /**
   * Approves a broadcast.
   */
  approveBroadcast(call, callback) {
    this.executeApproveBroadcast(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeApproveBroadcast(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'ApproveBroadcast', request);
    this.logger.info('approve_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: {
        filters: { ...(request?.broadcast?.metadata ?? {}) },
        period: request?.broadcast?.window,
      },
      reason: (request?.reason ?? ''),
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.showsShowQueryClient.summarizeShows(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      filter: {
        created_after: input2?.broadcast?.audit?.updated_at,
        region: (input2?.broadcast?.location?.region ?? ''),
      },
    };
    const reply2 = await call((done) => this.clients.broadcastsBroadcastClient.searchBroadcasts(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      replay_clip: {
        address: { locality: (request?.broadcast?.location?.locality ?? '') },
        owner: { display_name: (request?.broadcast?.organizer?.display_name ?? '') },
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => this.clients.broadcastsReplayClipValidationClient.normalizeReplayClip(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      replay_clip: {
        address: { recipient: (request?.broadcast?.location?.recipient ?? '') },
        owner: { email: (reply3?.replay_clip?.owner?.email ?? '') },
      },
    };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => this.clients.broadcastsReplayClipValidationClient.checkReplayClipEligibility(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        outcome: { status: (reply2?.result?.status ?? '') },
        result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
      },
      { result: { correlation_id: (reply3?.result?.correlation_id ?? '') } },
      {
        broadcast: {
          audit: { updated_at: reply4?.replay_clip?.audit?.updated_at },
          location: {
            lines: ((reply4?.replay_clip?.locator === 'address' ? reply4?.replay_clip.address : undefined)?.lines ?? []),
          },
        },
      },
      {
        broadcast: {
          broadcast_id: (request?.broadcast_id ?? ''),
          organizer: request?.broadcast?.organizer,
        },
      },
    );
  }

  /**
   * Comments a broadcast.
   */
  commentBroadcastApproval(call, callback) {
    this.executeCommentBroadcastApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCommentBroadcastApproval(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'CommentBroadcastApproval', request);
    this.logger.info('comment_broadcast_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.broadcast?.organizer?.email ?? '') } },
      replay_clip: {
        address: { region: (request?.broadcast?.location?.region ?? '') },
        audit: { updated_at: request?.broadcast?.audit?.updated_at },
        owner: { user_id: (request?.details?.approver?.user_id ?? '') },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.broadcastsReplayClipValidationClient.previewReplayClip(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      show: { audit: { updated_at: request?.broadcast?.audit?.updated_at } },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.showsShowCalculationClient.calculateShow(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.replay_clip?.audit?.updated_at,
        labels: { ...(reply2?.show?.metadata ?? {}) },
        owner_id: (reply1?.replay_clip?.owner?.user_id ?? ''),
        region: (request?.broadcast?.location?.region ?? ''),
      },
    };
    const messages3 = await collect(this.clients.broadcastsStreamKeyQueryClient.exportStreamKeys(request3, metadataFor(request3.context), { deadline: deadline() }));
    const reply3 = messages3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.outcome?.message ?? '') } },
      {
        broadcast: { audit: { created_by: (reply2?.show?.audit?.created_by ?? '') } },
        outcome: { message: (reply2?.result?.message ?? '') },
      },
      {
        broadcast: { broadcast_id: (request?.broadcast_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Delegates a broadcast.
   */
  delegateBroadcastApproval(call, callback) {
    this.executeDelegateBroadcastApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDelegateBroadcastApproval(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'DelegateBroadcastApproval', request);
    this.logger.info('delegate_broadcast_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const task1 = deferred(() => call((done) => this.clients.performersPerformerContractQueryClient.resolvePerformerContracts(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const results2 = await Promise.all((request?.broadcast?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        details: {
          destination: { lines: [...(request?.broadcast?.location?.lines ?? [])] },
        },
        replay_clip: {
          owner: {
            email: (item?.contact?.email ?? ''),
            user_id: (request?.broadcast?.organizer?.user_id ?? ''),
          },
        },
      };
      const reply2 = await call((done) => this.clients.broadcastsReplayClipCalculationClient.quoteReplayClip(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
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
   * Escalates a broadcast.
   */
  escalateBroadcast(call, callback) {
    this.executeEscalateBroadcast(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEscalateBroadcast(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'EscalateBroadcast', request);
    this.logger.info('escalate_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.broadcast?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        show: {
          organizer: { email: (item?.contact?.email ?? '') },
          participants: [{ contact: { phone: (item?.contact?.phone ?? '') } }],
        },
      };
      const reply1 = await call((done) => this.clients.showsShowClient.createShow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      stream_key: { contact: { email: (request?.broadcast?.organizer?.email ?? '') } },
    };
    const reply2 = await this.bus.request('acme.broadcasts.v1.BroadcastApprovalService/EscalateBroadcast:1', request2);
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        broadcast: {
          participants: [{
            contact: {
              preferred_channel: (reply2?.stream_key?.contact?.preferred_channel ?? ''),
            },
          }],
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
   * Returns a broadcast.
   */
  getBroadcastApprovalStatus(call, callback) {
    this.executeGetBroadcastApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetBroadcastApprovalStatus(request) {
    validateContext(request.context);
    if (!request.broadcast_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'broadcast_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetBroadcastApprovalStatus', request);
    this.logger.info('get_broadcast_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: {
        audit: { created_at: request?.context?.deadline },
        contact: { email: (request?.context?.actor?.email ?? '') },
        external_account_id: (request?.context?.actor?.user_id ?? ''),
        labels: { ...(request?.context?.baggage ?? {}) },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.broadcastsStreamKeyCalculationClient.quoteStreamKey(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    return assemble(
      {
        broadcast: { participants: [{ contact: reply1?.stream_key?.contact }] },
      },
      { broadcast: { broadcast_id: (request?.broadcast_id ?? '') } },
    );
  }

  /**
   * Lists broadcasts a page at a time.
   */
  listPendingBroadcasts(call, callback) {
    this.executeListPendingBroadcasts(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListPendingBroadcasts(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListPendingBroadcasts', request);
    this.logger.info('list_pending_broadcasts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.filter?.owner_id ?? '') },
    };
    const messages1 = await collect(this.clients.identityUserClient.watchUsers(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      show: {
        location: { region: (request?.filter?.region ?? '') },
        organizer: { user_id: (request?.context?.actor?.user_id ?? '') },
      },
    };
    const reply2 = await call((done) => this.clients.showsShowCalculationClient.compareShowOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      change: {
        block: { notify_email: (request?.context?.actor?.email ?? '') },
        parameters: { ...(request?.context?.baggage ?? {}) },
        scopes: [...(request?.context?.actor?.roles ?? [])],
      },
      context: request?.context,
    };
    const messages3 = await exchange(this.clients.venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request3.context), { deadline: deadline() }), [request3]);
    const reply3 = messages3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      broadcasts: [{ audit: { created_by: (reply2?.show?.audit?.created_by ?? '') } }],
    };
  }

  /**
   * Rejects a broadcast.
   */
  rejectBroadcast(call, callback) {
    this.executeRejectBroadcast(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRejectBroadcast(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'RejectBroadcast', request);
    this.logger.info('reject_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: {
        address: { country_code: (request?.broadcast?.location?.country_code ?? '') },
        audit: { updated_at: request?.broadcast?.audit?.updated_at },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.broadcastsReplayClipValidationClient.explainReplayClipDecision(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      details: { contact: { email: (request?.broadcast?.organizer?.email ?? '') } },
    };
    const explainOnboardingPlanCalculation2 = promisify(this.clients.onboardingOnboardingPlanCalculationClient.explainOnboardingPlanCalculation.bind(this.clients.onboardingOnboardingPlanCalculationClient));
    const reply2 = await explainOnboardingPlanCalculation2(request2, metadataFor(request2.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.outcome?.message ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        broadcast: {
          organizer: {
            display_name: (reply2?.onboarding_plan?.counterparty?.display_name ?? ''),
          },
        },
      },
      {
        broadcast: {
          broadcast_id: (request?.broadcast_id ?? ''),
          status: (request?.broadcast?.status ?? ''),
        },
      },
    );
  }

  /**
   * Submits a broadcast.
   */
  submitBroadcast(call, callback) {
    this.executeSubmitBroadcast(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSubmitBroadcast(request) {
    validateContext(request.context);
    if (request.broadcast) {
      await this.store.save(this.store.toBroadcastRecord(request.context.tenant_id, request.broadcast));
    }
    await this.store.record(request.context.tenant_id, 'SubmitBroadcast', request);
    this.logger.info('submit_broadcast', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.broadcast?.location?.region ?? '') },
    };
    const reply1 = await call((done) => this.clients.broadcastsStreamKeyQueryClient.findStreamKeys(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      broadcast: {
        location: { region: (request?.broadcast?.location?.region ?? '') },
        weekly: {
          interval: ((request?.broadcast?.recurrence === 'daily' ? request?.broadcast.daily : undefined)?.interval ?? 0),
        },
      },
      context: request?.context,
    };
    const method2 = 'createBroadcast';
    const reply2 = await call((done) => this.clients.broadcastsBroadcastClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      change: {
        block: {
          notify_email: (request?.broadcast?.organizer?.email ?? ''),
          reason: (request?.reason ?? ''),
        },
        effective: request?.broadcast?.window,
      },
      context: request?.context,
    };
    const messages3 = await exchange(this.clients.venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request3.context), { deadline: deadline() }), [request3]);
    const reply3 = messages3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        broadcast: { audit: { updated_by: (reply2?.broadcast?.audit?.updated_by ?? '') } },
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

module.exports = { BroadcastApprovalService };
