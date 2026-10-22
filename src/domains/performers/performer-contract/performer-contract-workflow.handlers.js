'use strict';

/**
 * Runs the long-lived workflow that takes a performer contract from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect, exchange, upload } = require('../../../platform/streams');

/**
 * The handlers of PerformerContractWorkflowService, over the dependencies the performers team is given.
 */
function createPerformerContractWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a performer contract.
   */
  async function advancePerformerContractWorkflow(request) {
    validateContext(request.context);
    if (request.performer_contract) {
      await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
    }
    await store.record(request.context.tenant_id, 'AdvancePerformerContractWorkflow', request);
    logger.info('advance_performer_contract_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.performer_contract?.lines ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        filter: {
          owner_id: (item?.line_id ?? ''),
          region: (request?.performer_contract?.billing_address?.region ?? ''),
        },
      };
      const messages1 = await collect(clients.performersPerformerContractQueryClient.exportPerformerContracts(request1, metadataFor(request1.context), { deadline: deadline() }));
      const reply1 = messages1.at(-1) ?? {};
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      details: { approver: { email: (input2?.details?.notify?.email ?? '') } },
      performer: {
        audit: {
          updated_at: input2?.performer_contract?.audit?.updated_at,
          updated_by: (input2?.performer_contract?.audit?.updated_by ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.performersPerformerApprovalClient.rejectPerformer(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        performer_contract: {
          billing_address: {
            locality: (reply2?.performer?.primary_address?.locality ?? ''),
            recipient: (reply2?.performer?.primary_address?.recipient ?? ''),
          },
        },
      },
      {
        performer_contract: {
          due_at: request?.performer_contract?.due_at,
          performer_contract_id: (request?.performer_contract_id ?? ''),
        },
      },
    );
  }

  /**
   * Cancels a performer contract.
   */
  async function cancelPerformerContractWorkflow(request) {
    validateContext(request.context);
    if (request.performer_contract) {
      await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
    }
    await store.record(request.context.tenant_id, 'CancelPerformerContractWorkflow', request);
    logger.info('cancel_performer_contract_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          name: request?.details?.notify?.name,
          phone: (request?.details?.notify?.phone ?? ''),
        },
        timeout: request?.details?.timeout,
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.admissionsAdmissionLifecycleClient.scheduleAdmissionExpiry(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: { region: (request?.performer_contract?.billing_address?.region ?? '') },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.toursTourStopApprovalClient.listPendingTourStops(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      sponsor: {
        audit: { etag: (request?.performer_contract?.audit?.etag ?? '') },
        contact: {
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
        primary_address: {
          lines: [...(request?.performer_contract?.billing_address?.lines ?? [])],
          locality: (request?.performer_contract?.billing_address?.locality ?? ''),
        },
      },
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => clients.sponsorshipsSponsorApprovalClient.recallSponsor(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      filter: {
        created_after: request?.performer_contract?.due_at,
        owner_id: (reply1?.result?.correlation_id ?? ''),
        region: (request?.performer_contract?.billing_address?.region ?? ''),
      },
    };
    const reply4 = await withRetry(() => call((done) => clients.performersPerformerContractApprovalClient.listPendingPerformerContracts(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.outcome?.status ?? ''),
        },
      },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { details: (reply2?.result?.details ?? {}) },
      },
      {
        performer_contract: {
          number: ((reply3?.sponsor?.identity_document === 'passport' ? reply3?.sponsor.passport : undefined)?.number ?? ''),
        },
      },
      {
        outcome: { message: (reply4?.result?.message ?? '') },
        result: { message: (reply4?.result?.message ?? '') },
      },
      {
        performer_contract: {
          notes: (request?.performer_contract?.notes ?? ''),
          performer_contract_id: (request?.performer_contract_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a performer contract.
   */
  async function inspectPerformerContractWorkflow(request) {
    validateContext(request.context);
    if (!request.performer_contract_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performer_contract_id is required');
    }
    await store.record(request.context.tenant_id, 'InspectPerformerContractWorkflow', request);
    logger.info('inspect_performer_contract_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: {
        audit: { updated_at: request?.context?.deadline },
        organizer: { user_id: (request?.context?.actor?.user_id ?? '') },
        participants: [{
          contact: { email: (request?.context?.actor?.email ?? '') },
          participant_id: (request?.performer_contract_id ?? ''),
        }],
      },
      context: request?.context,
    };
    const task1 = deferred(() => call((done) => clients.broadcastsBroadcastWorkflowClient.startBroadcastWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    return assemble(
      {
        performer_contract: {
          billing_address: { locality: (reply1?.broadcast?.location?.locality ?? '') },
        },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        performer_contract: { performer_contract_id: (request?.performer_contract_id ?? '') },
      },
    );
  }

  /**
   * Lists performer contracts a page at a time.
   */
  async function listPerformerContractWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListPerformerContractWorkflows', request);
    logger.info('list_performer_contract_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: { location: { region: (request?.filter?.region ?? '') } },
      context: request?.context,
    };
    const reply1 = await bus.request('acme.performers.v1.PerformerContractWorkflowService/ListPerformerContractWorkflows:0', request1);
    const request2 = {
      context: request?.context,
      header: {
        source: (request?.context?.actor?.user_id ?? ''),
        submitted_by: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const reply2 = await upload(
      (done) => clients.performersPerformerClient.batchCreatePerformers(metadataFor(request2.context), { deadline: deadline() }, done),
      [request2],
    );
    const request3 = {
      change: {
        children: [{
          children: [{ parent_id: (request?.context?.actor?.user_id ?? '') }],
          parent_id: (request?.context?.actor?.user_id ?? ''),
        }],
        parent_id: (request?.context?.actor?.user_id ?? ''),
      },
      context: request?.context,
    };
    const messages3 = await exchange(clients.venuesVenueSectionSyncClient.streamVenueSectionChanges(metadataFor(request3.context), { deadline: deadline() }), [request3]);
    const reply3 = messages3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } };
  }

  /**
   * Pauses a performer contract.
   */
  async function pausePerformerContractWorkflow(request) {
    validateContext(request.context);
    if (request.performer_contract) {
      await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
    }
    await store.record(request.context.tenant_id, 'PausePerformerContractWorkflow', request);
    logger.info('pause_performer_contract_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      search_index: {
        address: {
          postal_code: (request?.performer_contract?.billing_address?.postal_code ?? ''),
        },
        audit: { updated_at: request?.performer_contract?.audit?.updated_at },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.searchSearchIndexValidationClient.checkSearchIndexEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      sponsor: {
        relationship_manager: { email: (request?.details?.notify?.email ?? '') },
      },
    };
    const reply2 = await call((done) => clients.sponsorshipsSponsorApprovalClient.delegateSponsorApproval(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        performer_contract: {
          billing_address: {
            recipient: ((reply1?.search_index?.locator === 'address' ? reply1?.search_index.address : undefined)?.recipient ?? ''),
          },
        },
      },
      {
        outcome: { status: (reply2?.outcome?.status ?? '') },
        performer_contract: {
          counterparty: {
            display_name: (reply2?.sponsor?.relationship_manager?.display_name ?? ''),
          },
        },
      },
      {
        performer_contract: {
          metadata: (request?.performer_contract?.metadata ?? {}),
          performer_contract_id: (request?.performer_contract_id ?? ''),
        },
      },
    );
  }

  /**
   * Resumes a performer contract.
   */
  async function resumePerformerContractWorkflow(request) {
    validateContext(request.context);
    if (request.performer_contract) {
      await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
    }
    await store.record(request.context.tenant_id, 'ResumePerformerContractWorkflow', request);
    logger.info('resume_performer_contract_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      performer: {
        audit: { updated_by: (request?.performer_contract?.audit?.updated_by ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.performersPerformerWorkflowClient.retryPerformerWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      change: {
        discount: { amount: request?.performer_contract?.total },
        parameters: { ...(request?.details?.parameters ?? {}) },
        scopes: [...(request?.context?.actor?.roles ?? [])],
      },
      context: request?.context,
    };
    const messages2 = await exchange(clients.venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request2.context), { deadline: deadline() }), [request2]);
    const reply2 = messages2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        performer_contract: { audit: { etag: (reply1?.performer?.audit?.etag ?? '') } },
      },
      {
        performer_contract: {
          metadata: (request?.performer_contract?.metadata ?? {}),
          performer_contract_id: (request?.performer_contract_id ?? ''),
        },
      },
    );
  }

  /**
   * Retrys a performer contract.
   */
  async function retryPerformerContractWorkflow(request) {
    validateContext(request.context);
    if (request.performer_contract) {
      await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
    }
    await store.record(request.context.tenant_id, 'RetryPerformerContractWorkflow', request);
    logger.info('retry_performer_contract_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const batchGetAccessPolicies1 = promisify(clients.authorizationAccessPolicyClient.batchGetAccessPolicies.bind(clients.authorizationAccessPolicyClient));
    const reply1 = await batchGetAccessPolicies1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      broadcast: {
        audit: { updated_by: (request?.performer_contract?.audit?.updated_by ?? '') },
        participants: [{
          contact: {
            email: (request?.details?.notify?.email ?? ''),
            phone: (request?.details?.notify?.phone ?? ''),
            preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
          },
        }],
      },
      context: request?.context,
    };
    const reply2 = await call((done) => clients.broadcastsBroadcastWorkflowClient.signalBroadcastWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.performer_contract?.audit?.created_at,
        region: (reply2?.broadcast?.location?.region ?? ''),
      },
    };
    const method3 = 'listVenuePolicyConflicts';
    const reply3 = await call((done) => clients.venuesVenuePolicySyncClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      details: {
        destination: {
          country_code: (reply2?.broadcast?.location?.country_code ?? ''),
          lines: [...(reply2?.broadcast?.location?.lines ?? [])],
          region: (reply2?.broadcast?.location?.region ?? ''),
        },
      },
    };
    const client4 = clients.registry.get('acme.performers.v1.RiderValidationService');
    const reply4 = await call((done) => client4.scoreRider(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      },
      {
        performer_contract: { audit: { created_at: reply2?.broadcast?.audit?.created_at } },
      },
      { result: { details: (reply3?.result?.details ?? {}) } },
      {
        outcome: { message: (reply4?.result?.message ?? '') },
        result: { details: (reply4?.result?.details ?? {}) },
      },
      {
        performer_contract: {
          due_at: request?.performer_contract?.due_at,
          performer_contract_id: (request?.performer_contract_id ?? ''),
        },
      },
    );
  }

  /**
   * Signals a performer contract.
   */
  async function signalPerformerContractWorkflow(request) {
    validateContext(request.context);
    if (request.performer_contract) {
      await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
    }
    await store.record(request.context.tenant_id, 'SignalPerformerContractWorkflow', request);
    logger.info('signal_performer_contract_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      performer_contract_id: (request?.performer_contract?.performer_contract_id ?? ''),
      reason: (request?.reason ?? ''),
    };
    const send1 = clients.performersPerformerContractQueryClient.summarizePerformerContracts.bind(clients.performersPerformerContractQueryClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      search_index: {
        audit: {
          created_by: (request?.performer_contract?.audit?.created_by ?? ''),
          updated_at: request?.performer_contract?.audit?.updated_at,
        },
        owner: { email: (request?.details?.notify?.email ?? '') },
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.searchSearchIndexValidationClient.normalizeSearchIndex(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        performer_contract: { audit: { created_at: reply2?.search_index?.audit?.created_at } },
        result: { details: (reply2?.result?.details ?? {}) },
      },
      {
        performer_contract: {
          due_at: request?.performer_contract?.due_at,
          performer_contract_id: (request?.performer_contract_id ?? ''),
        },
      },
    );
  }

  /**
   * Starts a performer contract.
   */
  async function startPerformerContractWorkflow(request) {
    validateContext(request.context);
    if (request.performer_contract) {
      await store.save(store.toPerformerContractRecord(request.context.tenant_id, request.performer_contract));
    }
    await store.record(request.context.tenant_id, 'StartPerformerContractWorkflow', request);
    logger.info('start_performer_contract_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.performer_contract?.attachments ?? [])) {
      const request1 = {
        context: request?.context,
        details: { checksum: (item?.checksum ?? '') },
        venue_policy: {
          audit: {
            created_by: (request?.performer_contract?.audit?.created_by ?? ''),
            etag: (request?.performer_contract?.audit?.etag ?? ''),
          },
          owner: {
            display_name: (request?.performer_contract?.counterparty?.display_name ?? ''),
          },
        },
      };
      const reply1 = await call((done) => clients.venuesVenuePolicySyncClient.resolveVenuePolicyConflict(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      sponsor: {
        audit: { updated_by: (input2?.performer_contract?.audit?.updated_by ?? '') },
      },
    };
    const reply2 = await call((done) => clients.sponsorshipsSponsorApprovalClient.delegateSponsorApproval(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: {
        approver: { display_name: (reply1?.venue_policy?.owner?.display_name ?? '') },
      },
      performer: {
        audit: { created_by: (request?.performer_contract?.audit?.created_by ?? '') },
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => clients.performersPerformerApprovalClient.escalatePerformer(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = { context: request?.context };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => clients.shippingShipmentQueryClient.resolveShipments(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { reference_id: (reply2?.outcome?.reference_id ?? '') },
        performer_contract: {
          billing_address: { postal_code: (reply2?.sponsor?.primary_address?.postal_code ?? '') },
        },
      },
      {
        outcome: { next_steps: (reply3?.outcome?.next_steps ?? []) },
        performer_contract: {
          billing_address: {
            country_code: (reply3?.performer?.primary_address?.country_code ?? ''),
          },
        },
      },
      {
        outcome: { message: (reply4?.result?.message ?? '') },
        result: { reference_ids: (reply4?.result?.reference_ids ?? []) },
      },
      {
        performer_contract: {
          issued_at: request?.performer_contract?.issued_at,
          performer_contract_id: (request?.performer_contract_id ?? ''),
        },
      },
    );
  }

  return {
    advancePerformerContractWorkflow: unary(advancePerformerContractWorkflow),
    cancelPerformerContractWorkflow: unary(cancelPerformerContractWorkflow),
    inspectPerformerContractWorkflow: unary(inspectPerformerContractWorkflow),
    listPerformerContractWorkflows: unary(listPerformerContractWorkflows),
    pausePerformerContractWorkflow: unary(pausePerformerContractWorkflow),
    resumePerformerContractWorkflow: unary(resumePerformerContractWorkflow),
    retryPerformerContractWorkflow: unary(retryPerformerContractWorkflow),
    signalPerformerContractWorkflow: unary(signalPerformerContractWorkflow),
    startPerformerContractWorkflow: unary(startPerformerContractWorkflow),
  };
}

module.exports = { createPerformerContractWorkflowHandlers };
