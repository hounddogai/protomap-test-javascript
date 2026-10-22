'use strict';

/**
 * Routes concession orders through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve ConcessionOrderApprovalService, recording every operation of the concessions team.
 */
class ConcessionOrderApprovalService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'ConcessionOrderApprovalService' });
  }

  /**
   * Approves a concession order.
   */
  approveConcessionOrder(call, callback) {
    this.executeApproveConcessionOrder(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeApproveConcessionOrder(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'ApproveConcessionOrder', request);
    this.logger.info('approve_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.concession_order?.issued_at,
        region: (request?.concession_order?.billing_address?.region ?? ''),
      },
      subscriber: { email: (request?.details?.approver?.email ?? '') },
    };
    const messages1 = await collect(this.clients.concessionsMenuItemReportingClient.streamMenuItemMetrics(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      menu_item: { audit: { created_at: input2?.concession_order?.audit?.created_at } },
    };
    const reply2 = await call((done) => this.clients.concessionsMenuItemCalculationClient.compareMenuItemOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      concession_stand: {
        address: {
          country_code: (request?.concession_order?.billing_address?.country_code ?? ''),
        },
        audit: { etag: (request?.concession_order?.audit?.etag ?? '') },
      },
      context: request?.context,
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => this.clients.concessionsConcessionStandApprovalClient.rejectConcessionStand(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    let selected4_3;
    switch (reply3?.concession_stand?.locator) {
      case 'address':
        selected4_3 = ((reply3?.concession_stand?.locator === 'address' ? reply3?.concession_stand.address : undefined)?.lines ?? []);
        break;
      default:
        selected4_3 = [];
    }
    const request4 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: {
              updated_at: request?.concession_order?.audit?.updated_at,
              updated_by: (request?.concession_order?.audit?.updated_by ?? ''),
            },
            location: { lines: selected4_3 },
          }],
        }],
      },
    };
    const reply4 = await call((done) => this.clients.seatingSeatMapCalculationClient.estimateSeatMap(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_order: {
          billing_address: {
            lines: ((reply2?.menu_item?.locator === 'address' ? reply2?.menu_item.address : undefined)?.lines ?? []),
          },
        },
      },
      {
        concession_order: {
          billing_address: {
            locality: ((reply3?.concession_stand?.locator === 'address' ? reply3?.concession_stand.address : undefined)?.locality ?? ''),
          },
          lines: [{
            quantity: (reply3?.concession_stand?.components?.primary?.quantity ?? '0'),
          }],
        },
      },
      {
        concession_order: {
          billing_address: { region: (reply4?.seat_map?.location?.region ?? '') },
        },
      },
      {
        concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Comments a concession order.
   */
  commentConcessionOrderApproval(call, callback) {
    this.executeCommentConcessionOrderApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCommentConcessionOrderApproval(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'CommentConcessionOrderApproval', request);
    this.logger.info('comment_concession_order_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.concession_order?.lines ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        stream_key: { limits: [{ amount: item?.tax?.amount }] },
      };
      const reply1 = await call((done) => this.clients.broadcastsStreamKeyApprovalClient.submitStreamKey(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      concession_stand: {
        audit: {
          created_at: request?.concession_order?.audit?.created_at,
          updated_at: request?.concession_order?.audit?.updated_at,
        },
      },
      context: request?.context,
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.concessionsConcessionStandApprovalClient.approveConcessionStand(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { effective_at: reply2?.outcome?.effective_at },
        result: { message: (reply2?.result?.message ?? '') },
      },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          total: request?.concession_order?.total,
        },
      },
    );
  }

  /**
   * Delegates a concession order.
   */
  delegateConcessionOrderApproval(call, callback) {
    this.executeDelegateConcessionOrderApproval(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDelegateConcessionOrderApproval(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'DelegateConcessionOrderApproval', request);
    this.logger.info('delegate_concession_order_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.concession_order?.audit?.created_at,
        labels: { ...(request?.annotations ?? {}) },
        owner_id: (request?.concession_order?.counterparty?.menu_item_id ?? ''),
        region: (request?.concession_order?.billing_address?.region ?? ''),
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.concessionsConcessionStandQueryClient.browseConcessionStands(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply1?.result?.status ?? '') } },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          due_at: request?.concession_order?.due_at,
        },
      },
    );
  }

  /**
   * Escalates a concession order.
   */
  escalateConcessionOrder(call, callback) {
    this.executeEscalateConcessionOrder(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEscalateConcessionOrder(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'EscalateConcessionOrder', request);
    this.logger.info('escalate_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.concession_order?.due_at,
        region: (request?.concession_order?.billing_address?.region ?? ''),
      },
    };
    const task1 = deferred(() => call((done) => this.clients.venuesVenueSectionNotificationClient.listVenueSectionSubscriptions(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      stream_key: {
        card: {
          last4: ((request?.concession_order?.settlement === 'card' ? request?.concession_order.card : undefined)?.last4 ?? ''),
        },
      },
    };
    const reply2 = await this.bus.request('acme.concessions.v1.ConcessionOrderApprovalService/EscalateConcessionOrder:1', request2);
    const request3 = {
      context: request?.context,
      details: {
        recipient: {
          email: (reply2?.stream_key?.contact?.email ?? ''),
          name: reply2?.stream_key?.contact?.name,
          phone: (reply2?.stream_key?.contact?.phone ?? ''),
          preferred_channel: (reply2?.stream_key?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply3 = await runInContext(request.context, () => call((done) => this.clients.venuesVenueSectionNotificationClient.notifyVenueSectionWatchers(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { status: (reply1?.result?.status ?? '') },
      },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      {
        concession_order: {
          audit: { updated_by: (reply3?.venue_section?.audit?.updated_by ?? '') },
        },
        result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
      },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          counterparty: request?.concession_order?.counterparty,
        },
      },
    );
  }

  /**
   * Returns a concession order.
   */
  getConcessionOrderApprovalStatus(call, callback) {
    this.executeGetConcessionOrderApprovalStatus(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetConcessionOrderApprovalStatus(request) {
    validateContext(request.context);
    if (!request.concession_order_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetConcessionOrderApprovalStatus', request);
    this.logger.info('get_concession_order_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.concession_order_id ?? '') },
    };
    const reply1 = await call((done) => this.clients.sponsorshipsActivationApprovalClient.listPendingActivations(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      },
    );
  }

  /**
   * Recalls a concession order.
   */
  recallConcessionOrder(call, callback) {
    this.executeRecallConcessionOrder(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecallConcessionOrder(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'RecallConcessionOrder', request);
    this.logger.info('recall_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: {
        address: {
          country_code: (request?.concession_order?.billing_address?.country_code ?? ''),
        },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.broadcastsReplayClipWorkflowClient.startReplayClipWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.concession_order?.audit?.created_at,
        region: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? ''),
      },
    };
    const listMenuItemReports2 = promisify(this.clients.concessionsMenuItemReportingClient.listMenuItemReports.bind(this.clients.concessionsMenuItemReportingClient));
    const reply2 = await listMenuItemReports2(request2, metadataFor(request2.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { message: (reply1?.outcome?.message ?? '') },
      },
      { outcome: { status: (reply2?.result?.status ?? '') } },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          notes: (request?.concession_order?.notes ?? ''),
        },
      },
    );
  }

  /**
   * Rejects a concession order.
   */
  rejectConcessionOrder(call, callback) {
    this.executeRejectConcessionOrder(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRejectConcessionOrder(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'RejectConcessionOrder', request);
    this.logger.info('reject_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: {
        billing_address: {
          postal_code: (request?.concession_order?.billing_address?.postal_code ?? ''),
        },
      },
      context: request?.context,
    };
    const reply1 = await call((done) => this.clients.concessionsConcessionOrderLifecycleClient.scheduleConcessionOrderExpiry(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      activation: { audit: { updated_at: request?.concession_order?.audit?.updated_at } },
      context: request?.context,
      details: { approver: { email: (request?.details?.approver?.email ?? '') } },
    };
    const method2 = 'commentActivationApproval';
    const reply2 = await call((done) => this.clients.sponsorshipsActivationApprovalClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      menu_item: {
        address: { lines: [...(request?.concession_order?.billing_address?.lines ?? [])] },
        audit: { created_by: (reply1?.concession_order?.audit?.created_by ?? '') },
        owner: {
          display_name: (reply2?.activation?.assignee?.display_name ?? ''),
          email: (request?.details?.approver?.email ?? ''),
        },
      },
    };
    const client3 = this.clients.registry.get('acme.concessions.v1.MenuItemCalculationService');
    const reply3 = await call((done) => client3.explainMenuItemCalculation(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_order: {
          billing_address: reply1?.concession_order?.billing_address,
          concession_order_id: (reply1?.concession_order?.concession_order_id ?? ''),
        },
      },
      {
        concession_order: {
          counterparty: { display_name: (reply2?.activation?.assignee?.display_name ?? '') },
        },
        outcome: { reference_id: (reply2?.outcome?.reference_id ?? '') },
      },
      {
        concession_order: { counterparty: { code: (reply3?.menu_item?.code ?? '') } },
      },
      {
        concession_order: {
          issued_at: request?.concession_order?.issued_at,
          status: (request?.concession_order?.status ?? ''),
        },
      },
    );
  }

  /**
   * Submits a concession order.
   */
  submitConcessionOrder(call, callback) {
    this.executeSubmitConcessionOrder(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSubmitConcessionOrder(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await this.store.save(this.store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await this.store.record(request.context.tenant_id, 'SubmitConcessionOrder', request);
    this.logger.info('submit_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.concession_order?.due_at,
        owner_id: (request?.details?.approver?.user_id ?? ''),
        region: (request?.concession_order?.billing_address?.region ?? ''),
      },
    };
    const send1 = this.clients.pipelinesPipelineQueryClient.findPipelines.bind(this.clients.pipelinesPipelineQueryClient);
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
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          status: (request?.concession_order?.status ?? ''),
        },
      },
    );
  }
}

module.exports = { ConcessionOrderApprovalService };
