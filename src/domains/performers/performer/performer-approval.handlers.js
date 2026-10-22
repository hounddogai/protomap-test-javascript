'use strict';

/**
 * Routes performers through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of PerformerApprovalService, over the dependencies the performers team is given.
 */
function createPerformerApprovalHandlers({ store, clients, bus, logger }) {
  /**
   * Approves a performer.
   */
  async function approvePerformer(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'ApprovePerformer', request);
    logger.info('approve_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: {
        audit: { updated_by: (request?.performer?.audit?.updated_by ?? '') },
        organizer: { user_id: (request?.performer?.relationship_manager?.user_id ?? '') },
        participants: [{ contact: { email: (request?.details?.approver?.email ?? '') } }],
      },
      context: request?.context,
      details: {
        approver: {
          display_name: (request?.performer?.relationship_manager?.display_name ?? ''),
        },
      },
    };
    const task1 = deferred(() => call((done) => clients.broadcastsBroadcastApprovalClient.commentBroadcastApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: {
        filters: { ...(reply1?.result?.details ?? {}) },
        period: request?.performer?.preferences?.quiet_hours,
      },
      reason: (request?.reason ?? ''),
    };
    const reply2 = await bus.request('acme.performers.v1.PerformerApprovalService/ApprovePerformer:1', request2);
    const request3 = {
      context: request?.context,
      filter: {
        created_after: (request?.performer?.identity_document === 'passport' ? request?.performer.passport : undefined)?.expires_at,
        region: (request?.performer?.primary_address?.region ?? ''),
      },
    };
    const messages3 = await collect(clients.warehousingWarehouseQueryClient.exportWarehouses(request3, metadataFor(request3.context), { deadline: deadline() }));
    const reply3 = messages3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.details?.approver?.display_name ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        performer: {
          performer_id: (request?.performer_id ?? ''),
          primary_address: request?.performer?.primary_address,
        },
      },
    );
  }

  /**
   * Comments a performer.
   */
  async function commentPerformerApproval(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'CommentPerformerApproval', request);
    logger.info('comment_performer_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.performer?.primary_address?.region ?? '') },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.performersPerformerContractQueryClient.findPerformerContracts(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    let selected2_1;
    switch (request?.performer?.identity_document) {
      case 'passport':
        selected2_1 = ((request?.performer?.identity_document === 'passport' ? request?.performer.passport : undefined)?.number ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      performer: {
        audit: { updated_at: request?.performer?.audit?.updated_at },
        national_id: { number: selected2_1 },
        relationship_manager: {
          display_name: (request?.details?.approver?.display_name ?? ''),
          email: (request?.performer?.contact?.email ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.performersPerformerClient.createPerformer(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      { performer: { contact: reply2?.performer?.contact } },
      {
        outcome: { effective_at: request?.details?.effective_at },
        performer: { performer_id: (request?.performer_id ?? '') },
      },
    );
  }

  /**
   * Delegates a performer.
   */
  async function delegatePerformerApproval(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'DelegatePerformerApproval', request);
    logger.info('delegate_performer_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      performer: {
        contact: { email: (request?.performer?.relationship_manager?.email ?? '') },
        passport: {
          number: ((request?.performer?.identity_document === 'passport' ? request?.performer.passport : undefined)?.number ?? ''),
        },
        relationship_manager: { display_name: (request?.details?.approver?.display_name ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.performersPerformerClient.clonePerformer(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        performer: {
          performer_id: (request?.performer_id ?? ''),
          primary_address: request?.performer?.primary_address,
        },
      },
    );
  }

  /**
   * Escalates a performer.
   */
  async function escalatePerformer(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'EscalatePerformer', request);
    logger.info('escalate_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.performer?.audit?.created_at,
        labels: { ...(request?.performer?.attributes ?? {}) },
        owner_id: (request?.performer?.relationship_manager?.user_id ?? ''),
        region: (request?.performer?.primary_address?.region ?? ''),
      },
    };
    const messages1 = await collect(clients.identityUserQueryClient.exportUsers(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      details: {
        notify: {
          preferred_channel: (request?.performer?.contact?.preferred_channel ?? ''),
        },
      },
      wristband: {
        owner: { display_name: (request?.details?.approver?.display_name ?? '') },
      },
    };
    const retryWristbandWorkflow2 = promisify(clients.admissionsWristbandWorkflowClient.retryWristbandWorkflow.bind(clients.admissionsWristbandWorkflowClient));
    const reply2 = await retryWristbandWorkflow2(request2, metadataFor(request2.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply2?.result?.details ?? {}) } },
      {
        performer: {
          name: request?.performer?.name,
          performer_id: (request?.performer_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a performer.
   */
  async function getPerformerApprovalStatus(request) {
    validateContext(request.context);
    if (!request.performer_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performer_id is required');
    }
    await store.record(request.context.tenant_id, 'GetPerformerApprovalStatus', request);
    logger.info('get_performer_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.performer_id ?? ''),
      },
    };
    const reply1 = await call((done) => clients.venuesVenueSectionQueryClient.findVenueSections(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      { performer: { performer_id: (request?.performer_id ?? '') } },
    );
  }

  /**
   * Recalls a performer.
   */
  async function recallPerformer(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'RecallPerformer', request);
    logger.info('recall_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      wristband: {
        audit: { created_by: (request?.performer?.audit?.created_by ?? '') },
        owner: { email: (request?.performer?.contact?.email ?? '') },
      },
    };
    const method1 = 'pauseWristbandWorkflow';
    const reply1 = await call((done) => clients.admissionsWristbandWorkflowClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const client2 = clients.registry.get('acme.performers.v1.PerformerContractQueryService');
    const reply2 = await call((done) => client2.resolvePerformerContracts(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.performer?.audit?.updated_at,
        labels: { ...(reply1?.wristband?.specs ?? {}) },
        owner_id: (reply1?.wristband?.components?.primary?.component_id ?? ''),
        region: (request?.performer?.primary_address?.region ?? ''),
      },
    };
    const send3 = clients.performersPerformerContractQueryClient.findPerformerContracts.bind(clients.performersPerformerContractQueryClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      details: { approver: { email: (reply1?.wristband?.owner?.email ?? '') } },
    };
    const spread4 = {
      ...request4,
      context: request4.context ? { ...request4.context, baggage: { ...request4.context.baggage } } : null,
    };
    const reply4 = await call((done) => clients.concessionsConcessionOrderApprovalClient.approveConcessionOrder(spread4, metadataFor(spread4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { next_steps: (reply1?.outcome?.next_steps ?? []) } },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { status: (reply2?.result?.status ?? '') },
      },
      { result: { reference_ids: (reply3?.result?.reference_ids ?? []) } },
      {
        outcome: {
          message: (reply4?.outcome?.message ?? ''),
          next_steps: (reply4?.outcome?.next_steps ?? []),
        },
      },
      {
        performer: {
          performer_id: (request?.performer_id ?? ''),
          status: (request?.performer?.status ?? ''),
        },
      },
    );
  }

  /**
   * Rejects a performer.
   */
  async function rejectPerformer(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'RejectPerformer', request);
    logger.info('reject_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      filter: {
        created_after: input1?.performer?.audit?.updated_at,
        labels: { ...(input1?.annotations ?? {}) },
        owner_id: (input1?.performer_id ?? ''),
        region: (input1?.performer?.primary_address?.region ?? ''),
      },
    };
    const reply1 = await call((done) => clients.performersPerformerContractQueryClient.findPerformerContracts(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      replay_clip: {
        address: {
          country_code: (request?.performer?.primary_address?.country_code ?? ''),
        },
        owner: { email: (request?.details?.approver?.email ?? '') },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.broadcastsReplayClipWorkflowClient.resumeReplayClipWorkflow(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply1?.result?.status ?? '') } },
      {
        performer: {
          audit: { created_by: (reply2?.replay_clip?.audit?.created_by ?? '') },
          contact: { email: (reply2?.replay_clip?.owner?.email ?? '') },
        },
      },
      {
        performer: {
          performer_id: (request?.performer_id ?? ''),
          status: (request?.performer?.status ?? ''),
        },
      },
    );
  }

  /**
   * Submits a performer.
   */
  async function submitPerformer(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'SubmitPerformer', request);
    logger.info('submit_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: (request?.performer?.identity_document === 'passport' ? request?.performer.passport : undefined)?.expires_at,
        owner_id: (request?.details?.approver?.user_id ?? ''),
        region: (request?.performer?.primary_address?.region ?? ''),
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.performersPerformerContractQueryClient.findPerformerContracts(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.auditAuditEventClient.batchGetAuditEvents(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { message: (reply2?.result?.message ?? '') },
      },
      {
        performer: {
          performer_id: (request?.performer_id ?? ''),
          status: (request?.performer?.status ?? ''),
        },
      },
    );
  }

  return {
    approvePerformer: unary(approvePerformer),
    commentPerformerApproval: unary(commentPerformerApproval),
    delegatePerformerApproval: unary(delegatePerformerApproval),
    escalatePerformer: unary(escalatePerformer),
    getPerformerApprovalStatus: unary(getPerformerApprovalStatus),
    recallPerformer: unary(recallPerformer),
    rejectPerformer: unary(rejectPerformer),
    submitPerformer: unary(submitPerformer),
  };
}

module.exports = { createPerformerApprovalHandlers };
