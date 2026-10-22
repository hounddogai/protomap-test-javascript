'use strict';

/**
 * Routes concession stands through approval, with escalation and delegation.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of ConcessionStandApprovalService, over the dependencies the concessions team is given.
 */
function createConcessionStandApprovalHandlers({ store, clients, bus, logger }) {
  /**
   * Approves a concession stand.
   */
  async function approveConcessionStand(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'ApproveConcessionStand', request);
    logger.info('approve_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      details: {
        destination: {
          recipient: ((input1?.concession_stand?.locator === 'address' ? input1?.concession_stand.address : undefined)?.recipient ?? ''),
        },
      },
    };
    const reply1 = await call((done) => clients.concessionsConcessionStandCalculationClient.estimateConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          name: (request?.concession_stand?.name ?? ''),
        },
      },
    );
  }

  /**
   * Delegates a concession stand.
   */
  async function delegateConcessionStandApproval(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'DelegateConcessionStandApproval', request);
    logger.info('delegate_concession_stand_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { filters: { ...(request?.concession_stand?.specs ?? {}) } },
      reason: (request?.reason ?? ''),
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.metricsMetricSeriesQueryClient.summarizeMetricSerieses(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_stand: { owner: { user_id: (request?.concession_stand?.owner?.user_id ?? '') } },
      context: request?.context,
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.concessionsConcessionStandCalculationClient.recalculateConcessionStands(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      {
        concession_stand: {
          dimensions: { length: (reply2?.concession_stand?.dimensions?.length ?? 0) },
        },
      },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          name: (request?.concession_stand?.name ?? ''),
        },
      },
    );
  }

  /**
   * Escalates a concession stand.
   */
  async function escalateConcessionStand(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'EscalateConcessionStand', request);
    logger.info('escalate_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      form: {
        billing_address: {
          lines: [...((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.lines ?? [])],
        },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.formsFormClient.cloneForm(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.form?.audit?.updated_at,
        labels: { ...(reply1?.form?.metadata ?? {}) },
        owner_id: (request?.concession_stand?.owner?.user_id ?? ''),
        region: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.region ?? ''),
      },
    };
    const reply2 = await withRetry(() => call((done) => clients.concessionsConcessionOrderQueryClient.countConcessionOrders(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      venue_policy: {
        audit: {
          created_by: (reply1?.form?.audit?.created_by ?? ''),
          etag: (request?.concession_stand?.audit?.etag ?? ''),
        },
        owner: { display_name: (reply1?.form?.counterparty?.display_name ?? '') },
      },
    };
    const task3 = deferred(() => call((done) => clients.venuesVenuePolicyHistoryClient.compareVenuePolicyVersions(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: {
          audit: { created_by: (reply1?.form?.audit?.created_by ?? '') },
          owner: { display_name: (reply1?.form?.counterparty?.display_name ?? '') },
        },
      },
      { outcome: { message: (reply2?.result?.message ?? '') } },
      {
        outcome: { message: (reply3?.result?.message ?? '') },
        result: { correlation_id: (reply3?.result?.correlation_id ?? '') },
      },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          name: (request?.concession_stand?.name ?? ''),
        },
      },
    );
  }

  /**
   * Returns a concession stand.
   */
  async function getConcessionStandApprovalStatus(request) {
    validateContext(request.context);
    if (!request.concession_stand_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
    }
    await store.record(request.context.tenant_id, 'GetConcessionStandApprovalStatus', request);
    logger.info('get_concession_stand_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: { created_at: request?.context?.deadline },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const reply1 = await bus.request('acme.concessions.v1.ConcessionStandApprovalService/GetConcessionStandApprovalStatus:0', request1);
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
      },
    );
  }

  /**
   * Lists concession stands a page at a time.
   */
  async function listPendingConcessionStands(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListPendingConcessionStands', request);
    logger.info('list_pending_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Recalls a concession stand.
   */
  async function recallConcessionStand(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'RecallConcessionStand', request);
    logger.info('recall_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: {
        participants: [{ contact: { email: (request?.details?.approver?.email ?? '') } }],
      },
      context: request?.context,
      details: { notify: { email: (request?.concession_stand?.owner?.email ?? '') } },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.broadcastsBroadcastWorkflowClient.advanceBroadcastWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      concession_stand_id: (request?.concession_stand_id ?? ''),
      context: request?.context,
      details: {
        new_owner: {
          email: (request?.details?.approver?.email ?? ''),
          user_id: (request?.details?.approver?.user_id ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.concessionsConcessionStandLifecycleClient.expireConcessionStand(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: { audit: { updated_by: (reply1?.broadcast?.audit?.updated_by ?? '') } },
      },
      {
        concession_stand: { status: (reply2?.concession_stand?.status ?? '') },
      },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          owner: request?.concession_stand?.owner,
        },
      },
    );
  }

  /**
   * Rejects a concession stand.
   */
  async function rejectConcessionStand(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'RejectConcessionStand', request);
    logger.info('reject_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          recipient: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.recipient ?? ''),
        },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.boxofficeCashDrawerValidationClient.scoreCashDrawer(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      concession_stand_id: (request?.concession_stand?.concession_stand_id ?? ''),
      context: request?.context,
    };
    const lookupConcessionStand2 = promisify(clients.concessionsConcessionStandQueryClient.lookupConcessionStand.bind(clients.concessionsConcessionStandQueryClient));
    const reply2 = await lookupConcessionStand2(request2, metadataFor(request2.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: { audit: { created_by: (reply1?.cash_drawer?.audit?.created_by ?? '') } },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        concession_stand: { specs: (reply2?.concession_stand?.specs ?? {}) },
        outcome: { message: (reply2?.result?.message ?? '') },
      },
      {
        concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Submits a concession stand.
   */
  async function submitConcessionStand(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'SubmitConcessionStand', request);
    logger.info('submit_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.concession_stand?.locator) {
      case 'address':
        selected1_1 = ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.region ?? '');
        break;
      default:
        selected1_1 = '';
    }
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(request?.concession_stand?.specs ?? {}) },
        owner_id: (request?.concession_stand?.owner?.user_id ?? ''),
        region: selected1_1,
      },
    };
    const reply1 = await call((done) => clients.sponsorshipsActivationApprovalClient.listPendingActivations(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      sponsor: {
        audit: { updated_by: (request?.concession_stand?.audit?.updated_by ?? '') },
      },
    };
    const method2 = 'signalSponsorWorkflow';
    const reply2 = await call((done) => clients.sponsorshipsSponsorWorkflowClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      { result: { message: (reply2?.outcome?.message ?? '') } },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          name: (request?.concession_stand?.name ?? ''),
        },
      },
    );
  }

  return {
    approveConcessionStand: unary(approveConcessionStand),
    delegateConcessionStandApproval: unary(delegateConcessionStandApproval),
    escalateConcessionStand: unary(escalateConcessionStand),
    getConcessionStandApprovalStatus: unary(getConcessionStandApprovalStatus),
    listPendingConcessionStands: unary(listPendingConcessionStands),
    recallConcessionStand: unary(recallConcessionStand),
    rejectConcessionStand: unary(rejectConcessionStand),
    submitConcessionStand: unary(submitConcessionStand),
  };
}

module.exports = { createConcessionStandApprovalHandlers };
