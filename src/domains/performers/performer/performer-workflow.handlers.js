'use strict';

/**
 * Runs the long-lived workflow that takes a performer from start to finish.
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
 * The handlers of PerformerWorkflowService, over the dependencies the performers team is given.
 */
function createPerformerWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a performer.
   */
  async function advancePerformerWorkflow(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'AdvancePerformerWorkflow', request);
    logger.info('advance_performer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      performer_id: (request?.performer?.performer_id ?? ''),
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.performersPerformerQueryClient.summarizePerformers(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { notify: { phone: (request?.performer?.contact?.phone ?? '') } },
    };
    const reply2 = await withRetry(() => call((done) => clients.admissionsWristbandWorkflowClient.cancelWristbandWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      wristband: {
        address: { lines: [...(request?.performer?.primary_address?.lines ?? [])] },
        audit: { updated_at: request?.performer?.audit?.updated_at },
        owner: {
          email: (reply2?.wristband?.owner?.email ?? ''),
          user_id: (reply2?.wristband?.owner?.user_id ?? ''),
        },
      },
    };
    const task3 = deferred(() => call((done) => clients.admissionsWristbandWorkflowClient.signalWristbandWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        performer: {
          relationship_manager: { display_name: (reply2?.wristband?.owner?.display_name ?? '') },
        },
      },
      {
        performer: {
          audit: { created_by: (reply3?.wristband?.audit?.created_by ?? '') },
          primary_address: {
            locality: ((reply3?.wristband?.locator === 'address' ? reply3?.wristband.address : undefined)?.locality ?? ''),
          },
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
   * Cancels a performer.
   */
  async function cancelPerformerWorkflow(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'CancelPerformerWorkflow', request);
    logger.info('cancel_performer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.performer?.primary_address?.region ?? '') },
    };
    const messages1 = await collect(clients.performersPerformerQueryClient.exportPerformers(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      filter: { region: (request?.performer?.primary_address?.region ?? '') },
    };
    const reply2 = await bus.request('acme.performers.v1.PerformerWorkflowService/CancelPerformerWorkflow:1', request2);
    const request3 = {
      context: request?.context,
      etag: (request?.performer?.audit?.etag ?? ''),
      performer_id: (request?.performer_id ?? ''),
      reason: (request?.reason ?? ''),
    };
    const reply3 = await runInContext(request.context, () => call((done) => clients.performersPerformerClient.deletePerformer(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const request4 = {
      context: request?.context,
      venue: {
        address: {
          country_code: (request?.performer?.primary_address?.country_code ?? ''),
        },
      },
    };
    const reply4 = await call((done) => clients.venuesVenueValidationClient.validateVenue(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
    return assemble(
      { outcome: { status: (reply2?.result?.status ?? '') } },
      { result: { details: (reply3?.result?.details ?? {}) } },
      {
        performer: { audit: { created_by: (reply4?.venue?.audit?.created_by ?? '') } },
      },
      {
        performer: {
          contact: { name: request?.performer?.name },
          performer_id: (request?.performer_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a performer.
   */
  async function inspectPerformerWorkflow(request) {
    validateContext(request.context);
    if (!request.performer_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performer_id is required');
    }
    await store.record(request.context.tenant_id, 'InspectPerformerWorkflow', request);
    logger.info('inspect_performer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { performer: { performer_id: (request?.performer_id ?? '') } };
  }

  /**
   * Lists performers a page at a time.
   */
  async function listPerformerWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListPerformerWorkflows', request);
    logger.info('list_performer_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Retrys a performer.
   */
  async function retryPerformerWorkflow(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'RetryPerformerWorkflow', request);
    logger.info('retry_performer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.admissionsWristbandReportingClient.scheduleWristbandReport(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      details: {
        approver: {
          display_name: (request?.performer?.relationship_manager?.display_name ?? ''),
        },
      },
      menu_item: {
        address: {
          locality: (request?.performer?.primary_address?.locality ?? ''),
          recipient: (request?.performer?.primary_address?.recipient ?? ''),
        },
      },
    };
    const rejectMenuItem2 = promisify(clients.concessionsMenuItemApprovalClient.rejectMenuItem.bind(clients.concessionsMenuItemApprovalClient));
    const reply2 = await rejectMenuItem2(request2, metadataFor(request2.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      { outcome: { message: (reply2?.outcome?.message ?? '') } },
      {
        performer: {
          contact: request?.performer?.contact,
          performer_id: (request?.performer_id ?? ''),
        },
      },
    );
  }

  /**
   * Signals a performer.
   */
  async function signalPerformerWorkflow(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'SignalPerformerWorkflow', request);
    logger.info('signal_performer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      performer_id: (request?.performer?.performer_id ?? ''),
    };
    const reply1 = await call((done) => clients.performersPerformerClient.getPerformer(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const method2 = 'batchGetPerformers';
    const reply2 = await call((done) => clients.performersPerformerClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        performer: {
          primary_address: { region: (reply1?.performer?.primary_address?.region ?? '') },
        },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        performer: {
          contact: request?.performer?.contact,
          performer_id: (request?.performer_id ?? ''),
        },
      },
    );
  }

  /**
   * Starts a performer.
   */
  async function startPerformerWorkflow(request) {
    validateContext(request.context);
    if (request.performer) {
      await store.save(store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await store.record(request.context.tenant_id, 'StartPerformerWorkflow', request);
    logger.info('start_performer_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      secret: {
        audit: { created_by: (request?.performer?.audit?.created_by ?? '') },
        contact: {
          name: request?.details?.notify?.name,
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
    };
    const client1 = clients.registry.get('acme.secrets.v1.SecretService');
    const reply1 = await call((done) => client1.updateSecret(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      performer_id: (request?.performer?.performer_id ?? ''),
    };
    const send2 = clients.performersPerformerClient.getPerformer.bind(clients.performersPerformerClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { performer: { name: reply1?.secret?.contact?.name } },
      {
        performer: {
          audit: { etag: (reply2?.performer?.audit?.etag ?? '') },
          primary_address: { locality: (reply2?.performer?.primary_address?.locality ?? '') },
        },
      },
      {
        performer: {
          contact: { name: request?.performer?.name },
          performer_id: (request?.performer_id ?? ''),
        },
      },
    );
  }

  return {
    advancePerformerWorkflow: unary(advancePerformerWorkflow),
    cancelPerformerWorkflow: unary(cancelPerformerWorkflow),
    inspectPerformerWorkflow: unary(inspectPerformerWorkflow),
    listPerformerWorkflows: unary(listPerformerWorkflows),
    retryPerformerWorkflow: unary(retryPerformerWorkflow),
    signalPerformerWorkflow: unary(signalPerformerWorkflow),
    startPerformerWorkflow: unary(startPerformerWorkflow),
  };
}

module.exports = { createPerformerWorkflowHandlers };
