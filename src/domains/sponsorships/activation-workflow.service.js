'use strict';

/**
 * Runs the long-lived workflow that takes an activation from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve ActivationWorkflowService, recording every operation of the sponsorships team.
 */
class ActivationWorkflowService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'ActivationWorkflowService' });
  }

  /**
   * Advances an activation.
   */
  advanceActivationWorkflow(call, callback) {
    this.executeAdvanceActivationWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeAdvanceActivationWorkflow(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'AdvanceActivationWorkflow', request);
    this.logger.info('advance_activation_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { notify: { phone: (request?.details?.notify?.phone ?? '') } },
      sponsorship_deal: { counterparty: { email: (request?.activation?.assignee?.email ?? '') } },
    };
    const client1 = this.clients.registry.get('acme.sponsorships.v1.SponsorshipDealWorkflowService');
    const reply1 = await call((done) => client1.advanceSponsorshipDealWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { filters: { ...(request?.activation?.custom_fields ?? {}) } },
      reason: (request?.reason ?? ''),
    };
    const send2 = this.clients.venuesVenueSectionQueryClient.summarizeVenueSections.bind(this.clients.venuesVenueSectionQueryClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
    return assemble(
      {
        activation: { sla: { due_at: reply1?.sponsorship_deal?.due_at } },
        outcome: { status: (reply1?.result?.status ?? '') },
      },
      { result: { details: (reply2?.result?.details ?? {}) } },
      {
        activation: {
          activation_id: (request?.activation_id ?? ''),
          status: (request?.activation?.status ?? ''),
        },
      },
    );
  }

  /**
   * Cancels an activation.
   */
  cancelActivationWorkflow(call, callback) {
    this.executeCancelActivationWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCancelActivationWorkflow(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'CancelActivationWorkflow', request);
    this.logger.info('cancel_activation_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.sponsorshipsSponsorshipDealValidationClient.lintSponsorshipDeals(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      details: {
        channel: ((input2?.activation?.origin === 'channel' ? input2?.activation.channel : undefined)?.channel ?? ''),
        recipient: {
          email: (input2?.activation?.assignee?.email ?? ''),
          name: input2?.activation?.requester?.name,
        },
      },
    };
    const reply2 = await call((done) => this.clients.venuesVenuePolicyNotificationClient.notifyVenuePolicyWatchers(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      sponsor: {
        audit: { updated_by: (reply1?.sponsorship_deal?.audit?.updated_by ?? '') },
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => this.clients.sponsorshipsSponsorApprovalClient.commentSponsorApproval(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        activation: {
          audit: { created_by: (reply1?.sponsorship_deal?.audit?.created_by ?? '') },
        },
        outcome: { message: (reply1?.result?.message ?? '') },
      },
      {
        result: {
          message: (reply2?.outcome?.message ?? ''),
          status: (reply2?.outcome?.status ?? ''),
        },
      },
      {
        activation: { comments: [{ created_at: reply3?.sponsor?.audit?.created_at }] },
      },
      {
        activation: {
          activation_id: (request?.activation_id ?? ''),
          assignee: request?.activation?.assignee,
        },
      },
    );
  }

  /**
   * Returns an activation.
   */
  inspectActivationWorkflow(call, callback) {
    this.executeInspectActivationWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeInspectActivationWorkflow(request) {
    validateContext(request.context);
    if (!request.activation_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'activation_id is required');
    }
    await this.store.record(request.context.tenant_id, 'InspectActivationWorkflow', request);
    this.logger.info('inspect_activation_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { activation: { activation_id: (request?.activation_id ?? '') } };
  }

  /**
   * Lists activations a page at a time.
   */
  listActivationWorkflows(call, callback) {
    this.executeListActivationWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListActivationWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListActivationWorkflows', request);
    this.logger.info('list_activation_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { locale: (request?.context?.locale ?? '') },
      sponsorship_deal: {
        audit: { updated_at: request?.filter?.created_after },
        billing_address: { region: (request?.filter?.region ?? '') },
        counterparty: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.sponsorshipsSponsorshipDealValidationClient.checkSponsorshipDealEligibility(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        notify: { email: (reply1?.sponsorship_deal?.counterparty?.email ?? '') },
      },
      sponsorship_deal: {
        audit: {
          created_by: (reply1?.sponsorship_deal?.audit?.created_by ?? ''),
          etag: (reply1?.sponsorship_deal?.audit?.etag ?? ''),
        },
        bank: {
          bic: ((reply1?.sponsorship_deal?.settlement === 'bank' ? reply1?.sponsorship_deal.bank : undefined)?.bic ?? ''),
        },
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.sponsorshipsSponsorshipDealWorkflowClient.advanceSponsorshipDealWorkflow(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        activations: [{
          audit: { created_at: reply1?.sponsorship_deal?.audit?.created_at },
          requester: { email: (reply1?.sponsorship_deal?.counterparty?.email ?? '') },
        }],
      },
      {
        activations: [{
          requester: { email: (reply2?.sponsorship_deal?.counterparty?.email ?? '') },
        }],
        result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
      },
    );
  }

  /**
   * Pauses an activation.
   */
  pauseActivationWorkflow(call, callback) {
    this.executePauseActivationWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePauseActivationWorkflow(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'PauseActivationWorkflow', request);
    this.logger.info('pause_activation_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { approver: { email: (request?.details?.notify?.email ?? '') } },
      sponsorship_deal: {
        audit: { etag: (request?.activation?.audit?.etag ?? '') },
        counterparty: { email: (request?.activation?.assignee?.email ?? '') },
        due_at: request?.activation?.sla?.due_at,
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.sponsorshipsSponsorshipDealApprovalClient.submitSponsorshipDeal(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        activation: {
          audit: { updated_by: (reply1?.sponsorship_deal?.audit?.updated_by ?? '') },
        },
        outcome: { message: (reply1?.outcome?.message ?? '') },
      },
      {
        activation: {
          activation_id: (request?.activation_id ?? ''),
          subject: (request?.activation?.subject ?? ''),
        },
      },
    );
  }

  /**
   * Resumes an activation.
   */
  resumeActivationWorkflow(call, callback) {
    this.executeResumeActivationWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResumeActivationWorkflow(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'ResumeActivationWorkflow', request);
    this.logger.info('resume_activation_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: {
        audit: { etag: (request?.activation?.audit?.etag ?? '') },
        owner: {
          display_name: (request?.activation?.assignee?.display_name ?? ''),
          email: (request?.activation?.assignee?.email ?? ''),
        },
      },
    };
    const task1 = deferred(() => call((done) => this.clients.broadcastsReplayClipWorkflowClient.cancelReplayClipWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        activation: {
          assignee: { display_name: (reply1?.replay_clip?.owner?.display_name ?? '') },
        },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        activation: {
          activation_id: (request?.activation_id ?? ''),
          subject: (request?.activation?.subject ?? ''),
        },
      },
    );
  }

  /**
   * Retrys an activation.
   */
  retryActivationWorkflow(call, callback) {
    this.executeRetryActivationWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRetryActivationWorkflow(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'RetryActivationWorkflow', request);
    this.logger.info('retry_activation_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.activation?.comments ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        sponsorship_deal: {
          audit: {
            created_at: item?.created_at,
            created_by: (request?.activation?.audit?.created_by ?? ''),
            updated_by: (request?.activation?.audit?.updated_by ?? ''),
          },
          counterparty: { email: (item?.author_email ?? '') },
        },
      };
      const reply1 = await call((done) => this.clients.sponsorshipsSponsorshipDealValidationClient.explainSponsorshipDealDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      cash_drawer: {
        audit: { updated_by: (reply1?.sponsorship_deal?.audit?.updated_by ?? '') },
        bank: {
          bic: ((reply1?.sponsorship_deal?.settlement === 'bank' ? reply1?.sponsorship_deal.bank : undefined)?.bic ?? ''),
        },
      },
      context: request?.context,
      details: {
        contact: {
          email: (request?.activation?.assignee?.email ?? ''),
          preferred_channel: (request?.activation?.requester?.preferred_channel ?? ''),
        },
      },
    };
    const reply2 = await this.bus.request('acme.sponsorships.v1.ActivationWorkflowService/RetryActivationWorkflow:1', request2);
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        activation: { audit: { updated_by: (reply2?.cash_drawer?.audit?.updated_by ?? '') } },
      },
      {
        activation: {
          activation_id: (request?.activation_id ?? ''),
          subject: (request?.activation?.subject ?? ''),
        },
      },
    );
  }

  /**
   * Signals an activation.
   */
  signalActivationWorkflow(call, callback) {
    this.executeSignalActivationWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSignalActivationWorkflow(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'SignalActivationWorkflow', request);
    this.logger.info('signal_activation_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_assignment: {
        audit: { created_by: (request?.activation?.audit?.created_by ?? '') },
        comments: [{ created_at: request?.activation?.audit?.created_at }],
        sla: { paused_for: request?.activation?.sla?.paused_for },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.seatingSeatAssignmentCalculationClient.recalculateSeatAssignments(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      broadcast: {
        participants: [{ contact: { email: (request?.activation?.requester?.email ?? '') } }],
      },
      context: request?.context,
    };
    const reply2 = await call((done) => this.clients.broadcastsBroadcastApprovalClient.delegateBroadcastApproval(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      fixed_asset: {
        audit: { updated_by: (request?.activation?.audit?.updated_by ?? '') },
        owner: { display_name: (reply1?.seat_assignment?.assignee?.display_name ?? '') },
      },
    };
    const reply3 = await new Promise((resolve, reject) => {
      this.clients.assetsFixedAssetCalculationClient.calculateFixedAsset(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request4 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
    };
    const summarizeLedgerAccounts4 = promisify(this.clients.ledgerLedgerAccountQueryClient.summarizeLedgerAccounts.bind(this.clients.ledgerLedgerAccountQueryClient));
    const reply4 = await summarizeLedgerAccounts4(request4, metadataFor(request4.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      {
        activation: { audit: { created_by: (reply3?.fixed_asset?.audit?.created_by ?? '') } },
      },
      {
        outcome: { message: (reply4?.result?.message ?? '') },
        result: { status: (reply4?.result?.status ?? '') },
      },
      {
        activation: {
          activation_id: (request?.activation_id ?? ''),
          subject: (request?.activation?.subject ?? ''),
        },
      },
    );
  }

  /**
   * Starts an activation.
   */
  startActivationWorkflow(call, callback) {
    this.executeStartActivationWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeStartActivationWorkflow(request) {
    validateContext(request.context);
    if (request.activation) {
      await this.store.save(this.store.toActivationRecord(request.context.tenant_id, request.activation));
    }
    await this.store.record(request.context.tenant_id, 'StartActivationWorkflow', request);
    this.logger.info('start_activation_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.activation?.comments ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        stream_key: { audit: { created_at: item?.created_at } },
      };
      const reply1 = await call((done) => this.clients.broadcastsStreamKeyApprovalClient.delegateStreamKeyApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      activation: {
        activation_id: (request?.activation_id ?? ''),
        requester: request?.activation?.requester,
      },
    };
  }
}

module.exports = { ActivationWorkflowService };
