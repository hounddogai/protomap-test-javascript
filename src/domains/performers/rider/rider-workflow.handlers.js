'use strict';

/**
 * Runs the long-lived workflow that takes a rider from start to finish.
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
 * The handlers of RiderWorkflowService, over the dependencies the performers team is given.
 */
function createRiderWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a rider.
   */
  async function advanceRiderWorkflow(request) {
    validateContext(request.context);
    if (request.rider) {
      await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
    }
    await store.record(request.context.tenant_id, 'AdvanceRiderWorkflow', request);
    logger.info('advance_rider_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        approver: {
          display_name: (request?.rider?.owner?.display_name ?? ''),
          email: (request?.details?.notify?.email ?? ''),
          user_id: (request?.rider?.owner?.user_id ?? ''),
        },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.admissionsGateScanApprovalClient.delegateGateScanApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: {
        notify: {
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
      performer_contract: {
        audit: { created_by: (request?.rider?.audit?.created_by ?? '') },
        lines: [{
          tax: {
            amount: (request?.rider?.action === 'discount' ? request?.rider.discount : undefined)?.amount,
          },
        }],
      },
    };
    const reply2 = await call((done) => clients.performersPerformerContractWorkflowClient.startPerformerContractWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.rider?.audit?.created_at,
        region: (reply2?.performer_contract?.billing_address?.region ?? ''),
      },
    };
    const reply3 = await new Promise((resolve, reject) => {
      clients.performersPerformerQueryClient.suggestPerformers(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      { result: { reference_ids: (reply3?.result?.reference_ids ?? []) } },
      {
        rider: {
          parameters: (request?.rider?.parameters ?? {}),
          rider_id: (request?.rider_id ?? ''),
        },
      },
    );
  }

  /**
   * Cancels a rider.
   */
  async function cancelRiderWorkflow(request) {
    validateContext(request.context);
    if (request.rider) {
      await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
    }
    await store.record(request.context.tenant_id, 'CancelRiderWorkflow', request);
    logger.info('cancel_rider_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      subscriber: {
        email: (request?.rider?.owner?.email ?? ''),
        name: request?.details?.notify?.name,
      },
    };
    const messages1 = await collect(clients.travelTripClient.watchTrips(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      rider: {
        name: (request?.rider?.name ?? ''),
        rider_id: (request?.rider_id ?? ''),
      },
    };
  }

  /**
   * Returns a rider.
   */
  async function inspectRiderWorkflow(request) {
    validateContext(request.context);
    if (!request.rider_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'rider_id is required');
    }
    await store.record(request.context.tenant_id, 'InspectRiderWorkflow', request);
    logger.info('inspect_rider_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { approver: { user_id: (request?.context?.actor?.user_id ?? '') } },
      rider: { rider_id: (request?.rider_id ?? '') },
    };
    const recallRider1 = promisify(clients.performersRiderApprovalClient.recallRider.bind(clients.performersRiderApprovalClient));
    const reply1 = await recallRider1(request1, metadataFor(request1.context), { deadline: deadline() });
    return assemble(
      { rider: { owner: { user_id: (reply1?.rider?.owner?.user_id ?? '') } } },
      { rider: { rider_id: (request?.rider_id ?? '') } },
    );
  }

  /**
   * Lists riders a page at a time.
   */
  async function listRiderWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListRiderWorkflows', request);
    logger.info('list_rider_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { created_after: request?.filter?.created_after },
    };
    const messages1 = await collect(clients.venuesVenueSectionExportClient.streamVenueSectionExport(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Pauses a rider.
   */
  async function pauseRiderWorkflow(request) {
    validateContext(request.context);
    if (request.rider) {
      await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
    }
    await store.record(request.context.tenant_id, 'PauseRiderWorkflow', request);
    logger.info('pause_rider_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      performer: {
        audit: {
          etag: (request?.rider?.audit?.etag ?? ''),
          updated_at: request?.rider?.audit?.updated_at,
        },
        relationship_manager: {
          display_name: (request?.rider?.owner?.display_name ?? ''),
          email: (request?.details?.notify?.email ?? ''),
        },
      },
    };
    const reply1 = await call((done) => clients.performersPerformerApprovalClient.submitPerformer(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.rider?.audit?.updated_at,
        owner_id: (reply1?.performer?.performer_id ?? ''),
        region: (reply1?.performer?.primary_address?.region ?? ''),
      },
    };
    const method2 = 'listMenuItemReports';
    const reply2 = await call((done) => clients.concessionsMenuItemReportingClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      performer: {
        contact: {
          name: request?.details?.notify?.name,
          phone: (reply1?.performer?.contact?.phone ?? ''),
        },
        relationship_manager: {
          display_name: (reply1?.performer?.relationship_manager?.display_name ?? ''),
        },
      },
    };
    const client3 = clients.registry.get('acme.performers.v1.PerformerWorkflowService');
    const reply3 = await call((done) => client3.cancelPerformerWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = { context: request?.context };
    const send4 = clients.performersPerformerContractQueryClient.resolvePerformerContracts.bind(clients.performersPerformerContractQueryClient);
    const reply4 = await call((done) => send4(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.outcome?.message ?? '') },
        result: { status: (reply1?.result?.status ?? '') },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        rider: {
          audit: {
            created_by: (reply3?.performer?.audit?.created_by ?? ''),
            updated_at: reply3?.performer?.audit?.updated_at,
          },
        },
      },
      {
        outcome: { message: (reply4?.result?.message ?? '') },
        result: { details: (reply4?.result?.details ?? {}) },
      },
      {
        rider: {
          parameters: (request?.details?.parameters ?? {}),
          rider_id: (request?.rider_id ?? ''),
        },
      },
    );
  }

  /**
   * Retrys a rider.
   */
  async function retryRiderWorkflow(request) {
    validateContext(request.context);
    if (request.rider) {
      await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
    }
    await store.record(request.context.tenant_id, 'RetryRiderWorkflow', request);
    logger.info('retry_rider_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.rider?.action) {
      case 'block':
        selected1_1 = ((request?.rider?.action === 'block' ? request?.rider.block : undefined)?.reason ?? '');
        break;
      default:
        selected1_1 = '';
    }
    const request1 = {
      context: request?.context,
      reason: selected1_1,
    };
    const reply1 = await call((done) => clients.documentsDocumentQueryClient.summarizeDocuments(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      details: { approver: { user_id: (input2?.rider?.owner?.user_id ?? '') } },
      sponsorship_deal: {
        counterparty: {
          display_name: (input2?.rider?.owner?.display_name ?? ''),
          email: (input2?.details?.notify?.email ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.sponsorshipsSponsorshipDealApprovalClient.rejectSponsorshipDeal(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply1?.result?.status ?? '') } },
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        rider: {
          name: (request?.rider?.name ?? ''),
          rider_id: (request?.rider_id ?? ''),
        },
      },
    );
  }

  /**
   * Signals a rider.
   */
  async function signalRiderWorkflow(request) {
    validateContext(request.context);
    if (request.rider) {
      await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
    }
    await store.record(request.context.tenant_id, 'SignalRiderWorkflow', request);
    logger.info('signal_rider_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      performer: {
        audit: { created_at: request?.rider?.audit?.created_at },
        contact: { phone: (request?.details?.notify?.phone ?? '') },
        relationship_manager: {
          email: (request?.rider?.owner?.email ?? ''),
          user_id: (request?.rider?.owner?.user_id ?? ''),
        },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.performersPerformerClient.clonePerformer(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: { region: (reply1?.performer?.primary_address?.region ?? '') },
    };
    const messages2 = await collect(clients.fulfillmentFulfillmentQueryClient.exportFulfillments(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    let selected3_2;
    switch (reply1?.performer?.identity_document) {
      case 'national_id':
        selected3_2 = ((reply1?.performer?.identity_document === 'national_id' ? reply1?.performer.national_id : undefined)?.issuing_country ?? '');
        break;
      default:
        selected3_2 = '';
    }
    let selected3_4;
    switch (reply1?.performer?.identity_document) {
      case 'passport':
        selected3_4 = (reply1?.performer?.identity_document === 'passport' ? reply1?.performer.passport : undefined)?.expires_at;
        break;
      default:
        selected3_4 = null;
    }
    const request3 = {
      context: request?.context,
      performer: {
        audit: { created_by: (request?.rider?.audit?.created_by ?? '') },
        passport: {
          expires_at: selected3_4,
          issuing_country: selected3_2,
        },
        relationship_manager: { email: (request?.rider?.owner?.email ?? '') },
      },
    };
    const reply3 = await call((done) => clients.performersPerformerApprovalClient.recallPerformer(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = { context: request?.context };
    const merged4 = Object.assign({}, request4, { context: request4.context });
    const reply4 = await call((done) => clients.performersPerformerContractQueryClient.resolvePerformerContracts(merged4, metadataFor(merged4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        rider: { owner: { email: (reply1?.performer?.contact?.email ?? '') } },
      },
      { outcome: { reference_id: (reply3?.outcome?.reference_id ?? '') } },
      { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
      {
        rider: {
          parameters: (request?.details?.parameters ?? {}),
          rider_id: (request?.rider_id ?? ''),
        },
      },
    );
  }

  /**
   * Starts a rider.
   */
  async function startRiderWorkflow(request) {
    validateContext(request.context);
    if (request.rider) {
      await store.save(store.toRiderRecord(request.context.tenant_id, request.rider));
    }
    await store.record(request.context.tenant_id, 'StartRiderWorkflow', request);
    logger.info('start_rider_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        approver: {
          email: (request?.details?.notify?.email ?? ''),
          user_id: (request?.rider?.owner?.user_id ?? ''),
        },
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.admissionsAdmissionApprovalClient.submitAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: { recipient: { name: request?.details?.notify?.name } },
    };
    const task2 = deferred(() => call((done) => clients.venuesVenuePolicyNotificationClient.unsubscribeVenuePolicyUpdates(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { next_steps: (reply1?.outcome?.next_steps ?? []) },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        outcome: { reference_id: (reply2?.outcome?.reference_id ?? '') },
        rider: { version: (reply2?.venue_policy?.version ?? '0') },
      },
      {
        rider: {
          parameters: (request?.rider?.parameters ?? {}),
          rider_id: (request?.rider_id ?? ''),
        },
      },
    );
  }

  return {
    advanceRiderWorkflow: unary(advanceRiderWorkflow),
    cancelRiderWorkflow: unary(cancelRiderWorkflow),
    inspectRiderWorkflow: unary(inspectRiderWorkflow),
    listRiderWorkflows: unary(listRiderWorkflows),
    pauseRiderWorkflow: unary(pauseRiderWorkflow),
    retryRiderWorkflow: unary(retryRiderWorkflow),
    signalRiderWorkflow: unary(signalRiderWorkflow),
    startRiderWorkflow: unary(startRiderWorkflow),
  };
}

module.exports = { createRiderWorkflowHandlers };
