'use strict';

/**
 * Runs the long-lived workflow that takes a concession stand from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of ConcessionStandWorkflowService, over the dependencies the concessions team is given.
 */
function createConcessionStandWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a concession stand.
   */
  async function advanceConcessionStandWorkflow(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'AdvanceConcessionStandWorkflow', request);
    logger.info('advance_concession_stand_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: { audit: { updated_at: request?.concession_stand?.audit?.updated_at } },
      context: request?.context,
    };
    const send1 = clients.concessionsConcessionStandClient.cloneConcessionStand.bind(clients.concessionsConcessionStandClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    let selected2_1;
    switch (reply1?.concession_stand?.locator) {
      case 'address':
        selected2_1 = ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.region ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.concession_stand?.audit?.updated_at,
        region: selected2_1,
      },
    };
    const reply2 = await call((done) => clients.venuesVenueQueryClient.findVenues(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: { audit: { updated_at: reply1?.concession_stand?.audit?.updated_at } },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          owner: request?.concession_stand?.owner,
        },
      },
    );
  }

  /**
   * Cancels a concession stand.
   */
  async function cancelConcessionStandWorkflow(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'CancelConcessionStandWorkflow', request);
    logger.info('cancel_concession_stand_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      reason: (input1?.reason ?? ''),
    };
    const reply1 = await call((done) => clients.searchSearchIndexQueryClient.summarizeSearchIndexes(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { period: reply1?.period },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.concessionsMenuItemReportingClient.scheduleMenuItemReport(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          status: (request?.concession_stand?.status ?? ''),
        },
      },
    );
  }

  /**
   * Returns a concession stand.
   */
  async function inspectConcessionStandWorkflow(request) {
    validateContext(request.context);
    if (!request.concession_stand_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
    }
    await store.record(request.context.tenant_id, 'InspectConcessionStandWorkflow', request);
    logger.info('inspect_concession_stand_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        concession_stand_id: (request?.concession_stand_id ?? ''),
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
      context: request?.context,
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.concessionsConcessionStandCalculationClient.compareConcessionStandOptions(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    return assemble(
      {
        concession_stand: { category_path: (reply1?.concession_stand?.category_path ?? []) },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
      },
    );
  }

  /**
   * Lists concession stands a page at a time.
   */
  async function listConcessionStandWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListConcessionStandWorkflows', request);
    logger.info('list_concession_stand_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      notification: {
        assignee: { user_id: (request?.context?.actor?.user_id ?? '') },
        sla: { due_at: request?.filter?.created_after },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.notificationsNotificationValidationClient.previewNotification(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { details: (reply1?.result?.details ?? {}) } };
  }

  /**
   * Pauses a concession stand.
   */
  async function pauseConcessionStandWorkflow(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'PauseConcessionStandWorkflow', request);
    logger.info('pause_concession_stand_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: { etag: (request?.concession_stand?.audit?.etag ?? '') },
        owner: { user_id: (request?.concession_stand?.owner?.user_id ?? '') },
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.venuesVenuePolicyHistoryClient.recordVenuePolicyVersion(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        concession_stand: {
          capacity: (request?.concession_stand?.capacity ?? '0'),
          concession_stand_id: (request?.concession_stand_id ?? ''),
        },
      },
    );
  }

  /**
   * Resumes a concession stand.
   */
  async function resumeConcessionStandWorkflow(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'ResumeConcessionStandWorkflow', request);
    logger.info('resume_concession_stand_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
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
        created_after: request?.concession_stand?.audit?.created_at,
        region: selected1_1,
      },
    };
    const reply1 = await call((done) => clients.sponsorshipsSponsorWorkflowClient.listSponsorWorkflows(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      sponsorship_deal: {
        audit: { etag: (request?.concession_stand?.audit?.etag ?? '') },
        billing_address: {
          recipient: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.recipient ?? ''),
          region: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.region ?? ''),
        },
      },
    };
    const reply2 = await bus.request('acme.concessions.v1.ConcessionStandWorkflowService/ResumeConcessionStandWorkflow:1', request2);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      { outcome: { status: (reply2?.result?.status ?? '') } },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          status: (request?.concession_stand?.status ?? ''),
        },
      },
    );
  }

  /**
   * Retrys a concession stand.
   */
  async function retryConcessionStandWorkflow(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'RetryConcessionStandWorkflow', request);
    logger.info('retry_concession_stand_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: {
        billing_address: {
          region: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.region ?? ''),
        },
      },
      context: request?.context,
      details: { notify: { email: (request?.concession_stand?.owner?.email ?? '') } },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.concessionsConcessionOrderLifecycleClient.scheduleConcessionOrderExpiry(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    let selected2_2;
    switch (request?.concession_stand?.locator) {
      case 'address':
        selected2_2 = ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.region ?? '');
        break;
      default:
        selected2_2 = '';
    }
    const request2 = {
      context: request?.context,
      details: { approver: { email: (request?.concession_stand?.owner?.email ?? '') } },
      replay_clip: {
        address: { region: selected2_2 },
        audit: { updated_by: (request?.concession_stand?.audit?.updated_by ?? '') },
        owner: { display_name: (request?.concession_stand?.owner?.display_name ?? '') },
      },
    };
    const reply2 = await call((done) => clients.broadcastsReplayClipApprovalClient.submitReplayClip(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        concession_stand: {
          audit: { etag: (reply2?.replay_clip?.audit?.etag ?? '') },
          dimensions: { unit: (reply2?.replay_clip?.dimensions?.unit ?? '') },
        },
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
   * Signals a concession stand.
   */
  async function signalConcessionStandWorkflow(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'SignalConcessionStandWorkflow', request);
    logger.info('signal_concession_stand_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: {
        audit: {
          created_at: request?.concession_stand?.audit?.created_at,
          etag: (request?.concession_stand?.audit?.etag ?? ''),
        },
        contact: { name: request?.details?.notify?.name },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.broadcastsStreamKeyWorkflowClient.pauseStreamKeyWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: { audit: { created_at: reply1?.stream_key?.audit?.created_at } },
        outcome: { effective_at: reply1?.outcome?.effective_at },
      },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          specs: (request?.concession_stand?.specs ?? {}),
        },
      },
    );
  }

  /**
   * Starts a concession stand.
   */
  async function startConcessionStandWorkflow(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await store.save(store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await store.record(request.context.tenant_id, 'StartConcessionStandWorkflow', request);
    logger.info('start_concession_stand_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        address: {
          region: ((request?.concession_stand?.locator === 'address' ? request?.concession_stand.address : undefined)?.region ?? ''),
        },
      },
      context: request?.context,
    };
    const estimateConcessionStand1 = promisify(clients.concessionsConcessionStandCalculationClient.estimateConcessionStand.bind(clients.concessionsConcessionStandCalculationClient));
    const reply1 = await estimateConcessionStand1(request1, metadataFor(request1.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: {
          dimensions: { unit: (reply1?.concession_stand?.dimensions?.unit ?? '') },
          tags: (reply1?.concession_stand?.tags ?? []),
        },
      },
      {
        concession_stand: {
          code: (request?.concession_stand?.code ?? ''),
          concession_stand_id: (request?.concession_stand_id ?? ''),
        },
      },
    );
  }

  return {
    advanceConcessionStandWorkflow: unary(advanceConcessionStandWorkflow),
    cancelConcessionStandWorkflow: unary(cancelConcessionStandWorkflow),
    inspectConcessionStandWorkflow: unary(inspectConcessionStandWorkflow),
    listConcessionStandWorkflows: unary(listConcessionStandWorkflows),
    pauseConcessionStandWorkflow: unary(pauseConcessionStandWorkflow),
    resumeConcessionStandWorkflow: unary(resumeConcessionStandWorkflow),
    retryConcessionStandWorkflow: unary(retryConcessionStandWorkflow),
    signalConcessionStandWorkflow: unary(signalConcessionStandWorkflow),
    startConcessionStandWorkflow: unary(startConcessionStandWorkflow),
  };
}

module.exports = { createConcessionStandWorkflowHandlers };
