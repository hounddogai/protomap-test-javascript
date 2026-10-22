'use strict';

/**
 * Runs the long-lived workflow that takes a wristband from start to finish. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionApprovalClient,
  admissionsAdmissionWorkflowClient,
  admissionsGateScanLifecycleClient,
  admissionsGateScanQueryClient,
  admissionsGateScanWorkflowClient,
  admissionsWristbandApprovalClient,
  admissionsWristbandReportingClient,
  auditAuditEventValidationClient,
  broadcastsStreamKeyWorkflowClient,
  itsmIncidentCalculationClient,
  ledgerLedgerAccountClient,
  metricsMetricSeriesQueryClient,
  pipelinesPipelineCalculationClient,
  sponsorshipsSponsorshipDealApprovalClient,
  venuesVenueSectionSyncClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect, exchange } = require('../../../platform/streams');
const store = require('../store');

/**
 * Advances a wristband.
 */
async function advanceWristbandWorkflow(request) {
  validateContext(request.context);
  if (request.wristband) {
    await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
  }
  await store.record(request.context.tenant_id, 'AdvanceWristbandWorkflow', request);
  logger.info('advance_wristband_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      owner_id: (request?.wristband?.owner?.user_id ?? ''),
      region: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.region ?? ''),
    },
  };
  const messages1 = await collect(admissionsGateScanQueryClient.exportGateScans(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    details: {
      notify: {
        email: (request?.details?.notify?.email ?? ''),
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
  };
  const method2 = 'scheduleGateScanExpiry';
  const reply2 = await call((done) => admissionsGateScanLifecycleClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    incident: {
      audit: { created_by: (request?.wristband?.audit?.created_by ?? '') },
      customer: { email: (request?.wristband?.owner?.email ?? '') },
    },
  };
  const client3 = clients.registry.get('acme.itsm.v1.IncidentCalculationService');
  const reply3 = await call((done) => client3.calculateIncident(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply2?.result?.status ?? '') } },
    { wristband: { tags: (reply3?.incident?.tags ?? []) } },
    {
      wristband: {
        name: (request?.wristband?.name ?? ''),
        wristband_id: (request?.wristband_id ?? ''),
      },
    },
  );
}

/**
 * Cancels a wristband.
 */
async function cancelWristbandWorkflow(request) {
  validateContext(request.context);
  if (request.wristband) {
    await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
  }
  await store.record(request.context.tenant_id, 'CancelWristbandWorkflow', request);
  logger.info('cancel_wristband_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    audit_event: {
      measurements: [{ unit: (request?.wristband?.dimensions?.unit ?? '') }],
      subject: { display_name: (request?.wristband?.owner?.display_name ?? '') },
    },
    context: request?.context,
    details: { contact: { email: (request?.details?.notify?.email ?? '') } },
  };
  const send1 = auditAuditEventValidationClient.normalizeAuditEvent.bind(auditAuditEventValidationClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      destination: {
        locality: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.locality ?? ''),
      },
    },
    pipeline: {
      location: {
        recipient: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.recipient ?? ''),
      },
      organizer: { display_name: (request?.wristband?.owner?.display_name ?? '') },
    },
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => pipelinesPipelineCalculationClient.recalculatePipelines(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
    },
    { wristband: { audit: { etag: (reply2?.pipeline?.audit?.etag ?? '') } } },
    {
      wristband: {
        status: (request?.wristband?.status ?? ''),
        wristband_id: (request?.wristband_id ?? ''),
      },
    },
  );
}

/**
 * Returns a wristband.
 */
async function inspectWristbandWorkflow(request) {
  validateContext(request.context);
  if (!request.wristband_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectWristbandWorkflow', request);
  logger.info('inspect_wristband_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    filter: {
      created_after: input1?.context?.deadline,
      owner_id: (input1?.wristband_id ?? ''),
    },
  };
  const reply1 = await call((done) => admissionsAdmissionWorkflowClient.listAdmissionWorkflows(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: { owner_id: (request?.wristband_id ?? '') },
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => admissionsWristbandApprovalClient.listPendingWristbands(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  return assemble(
    { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
    { result: { status: (reply2?.result?.status ?? '') } },
    { wristband: { wristband_id: (request?.wristband_id ?? '') } },
  );
}

/**
 * Lists wristbands a page at a time.
 */
async function listWristbandWorkflows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListWristbandWorkflows', request);
  logger.info('list_wristband_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Pauses a wristband.
 */
async function pauseWristbandWorkflow(request) {
  validateContext(request.context);
  if (request.wristband) {
    await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
  }
  await store.record(request.context.tenant_id, 'PauseWristbandWorkflow', request);
  logger.info('pause_wristband_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    admission: {
      measurements: [{ unit: (request?.wristband?.dimensions?.unit ?? '') }],
      subject: { display_name: (request?.wristband?.owner?.display_name ?? '') },
    },
    context: request?.context,
    details: {
      approver: {
        email: (request?.details?.notify?.email ?? ''),
        user_id: (request?.wristband?.owner?.user_id ?? ''),
      },
    },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => admissionsAdmissionApprovalClient.approveAdmission(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    ledger_account: {
      audit: {
        created_by: (request?.wristband?.audit?.created_by ?? ''),
        etag: (request?.wristband?.audit?.etag ?? ''),
      },
    },
  };
  const merged2 = Object.assign({}, request2, { context: request2.context });
  const reply2 = await call((done) => ledgerLedgerAccountClient.cloneLedgerAccount(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: {
        message: (reply1?.result?.message ?? ''),
        status: (reply1?.outcome?.status ?? ''),
      },
    },
    {
      outcome: { status: (reply2?.result?.status ?? '') },
      result: { message: (reply2?.result?.message ?? '') },
    },
    {
      wristband: {
        owner: request?.wristband?.owner,
        wristband_id: (request?.wristband_id ?? ''),
      },
    },
  );
}

/**
 * Resumes a wristband.
 */
async function resumeWristbandWorkflow(request) {
  validateContext(request.context);
  if (request.wristband) {
    await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
  }
  await store.record(request.context.tenant_id, 'ResumeWristbandWorkflow', request);
  logger.info('resume_wristband_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    wristband: {
      address: {
        country_code: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.country_code ?? ''),
      },
      components: { primary: {
        description: (request?.wristband?.components?.primary?.description ?? ''),
      } },
      dimensions: { length: (request?.wristband?.dimensions?.length ?? 0) },
    },
  };
  const reply1 = await withRetry(() => call((done) => admissionsWristbandApprovalClient.commentWristbandApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: { filters: { ...(request?.details?.parameters ?? {}) } },
    reason: (request?.reason ?? ''),
    wristband_id: (reply1?.wristband?.wristband_id ?? ''),
  };
  const task2 = deferred(() => call((done) => admissionsWristbandReportingClient.generateWristbandReport(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  const request3 = {
    context: request?.context,
    details: {
      notify: {
        phone: (request?.details?.notify?.phone ?? ''),
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
    gate_scan: {
      subject: { display_name: (request?.wristband?.owner?.display_name ?? '') },
    },
  };
  const reply3 = await bus.request('acme.admissions.v1.WristbandWorkflowService/ResumeWristbandWorkflow:2', request3);
  const request4 = {
    context: request?.context,
    details: {
      notify: {
        email: (request?.details?.notify?.email ?? ''),
        phone: (request?.details?.notify?.phone ?? ''),
      },
    },
    stream_key: { audit: { updated_by: (reply1?.wristband?.audit?.updated_by ?? '') } },
  };
  const reply4 = await runInContext(request.context, () => call((done) => broadcastsStreamKeyWorkflowClient.startStreamKeyWorkflow(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.details?.initiated_by ?? '') });
  return assemble(
    {
      wristband: { audit: { updated_at: reply1?.wristband?.audit?.updated_at } },
    },
    { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
    { outcome: { status: (reply3?.result?.status ?? '') } },
    {
      outcome: { message: (reply4?.outcome?.message ?? '') },
      result: { message: (reply4?.result?.message ?? '') },
    },
    {
      wristband: {
        status: (request?.wristband?.status ?? ''),
        wristband_id: (request?.wristband_id ?? ''),
      },
    },
  );
}

/**
 * Retrys a wristband.
 */
async function retryWristbandWorkflow(request) {
  validateContext(request.context);
  if (request.wristband) {
    await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
  }
  await store.record(request.context.tenant_id, 'RetryWristbandWorkflow', request);
  logger.info('retry_wristband_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      region: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.region ?? ''),
    },
  };
  const reply1 = await call((done) => clients.broadcastsStreamKeyApprovalClient.listPendingStreamKeys(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    change: {
      children: [{ name: (request?.wristband?.name ?? '') }],
      name: (request?.wristband?.name ?? ''),
      parent_id: (request?.context?.actor?.user_id ?? ''),
    },
    context: request?.context,
  };
  const messages2 = await exchange(venuesVenueSectionSyncClient.streamVenueSectionChanges(metadataFor(request2.context), { deadline: deadline() }), [request2]);
  const reply2 = messages2.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    },
    {
      wristband: {
        status: (request?.wristband?.status ?? ''),
        wristband_id: (request?.wristband_id ?? ''),
      },
    },
  );
}

/**
 * Signals a wristband.
 */
async function signalWristbandWorkflow(request) {
  validateContext(request.context);
  if (request.wristband) {
    await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
  }
  await store.record(request.context.tenant_id, 'SignalWristbandWorkflow', request);
  logger.info('signal_wristband_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    admission: {
      measurements: [{ unit: (request?.wristband?.dimensions?.unit ?? '') }],
    },
    context: request?.context,
  };
  const reply1 = await new Promise((resolve, reject) => {
    admissionsAdmissionApprovalClient.submitAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    context: request?.context,
    filter: {
      region: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.region ?? ''),
    },
  };
  const listPendingAdmissions2 = promisify(admissionsAdmissionApprovalClient.listPendingAdmissions.bind(admissionsAdmissionApprovalClient));
  const reply2 = await listPendingAdmissions2(request2, metadataFor(request2.context), { deadline: deadline() });
  const request3 = {
    context: request?.context,
    filter: {
      created_after: request?.wristband?.audit?.updated_at,
      owner_id: (request?.wristband?.wristband_id ?? ''),
      region: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.region ?? ''),
    },
  };
  const reply3 = await call((done) => metricsMetricSeriesQueryClient.suggestMetricSerieses(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    wristband_id: (request?.wristband?.wristband_id ?? ''),
  };
  const method4 = 'getWristbandApprovalStatus';
  const reply4 = await call((done) => admissionsWristbandApprovalClient[method4](request4, metadataFor(request4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        message: (reply1?.outcome?.message ?? ''),
        status: (reply1?.outcome?.status ?? ''),
      },
    },
    { outcome: { status: (reply2?.result?.status ?? '') } },
    { result: { message: (reply3?.result?.message ?? '') } },
    {
      wristband: {
        audit: {
          etag: (reply4?.wristband?.audit?.etag ?? ''),
          updated_at: reply4?.wristband?.audit?.updated_at,
        },
      },
    },
    {
      wristband: {
        specs: (request?.wristband?.specs ?? {}),
        wristband_id: (request?.wristband_id ?? ''),
      },
    },
  );
}

/**
 * Starts a wristband.
 */
async function startWristbandWorkflow(request) {
  validateContext(request.context);
  if (request.wristband) {
    await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
  }
  await store.record(request.context.tenant_id, 'StartWristbandWorkflow', request);
  logger.info('start_wristband_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      notify: {
        email: (request?.wristband?.owner?.email ?? ''),
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
    gate_scan: {
      measurements: [{ unit: (request?.wristband?.dimensions?.unit ?? '') }],
    },
  };
  const client1 = clients.registry.get('acme.admissions.v1.GateScanWorkflowService');
  const reply1 = await call((done) => client1.startGateScanWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      destination: {
        country_code: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.country_code ?? ''),
        lines: [...((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.lines ?? [])],
      },
    },
    incident: {
      audit: { created_at: request?.wristband?.audit?.created_at },
      requester: {
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
  };
  const send2 = itsmIncidentCalculationClient.compareIncidentOptions.bind(itsmIncidentCalculationClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    details: {
      approver: {
        email: (reply2?.incident?.requester?.email ?? ''),
        user_id: (request?.wristband?.owner?.user_id ?? ''),
      },
    },
    sponsorship_deal: { audit: { etag: (reply2?.incident?.audit?.etag ?? '') } },
  };
  const spread3 = {
    ...request3,
    context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
  };
  const reply3 = await call((done) => sponsorshipsSponsorshipDealApprovalClient.submitSponsorshipDeal(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: {
        next_steps: (reply1?.outcome?.next_steps ?? []),
        reference_id: (reply1?.outcome?.reference_id ?? ''),
      },
    },
    {
      wristband: { owner: { email: (reply2?.incident?.assignee?.email ?? '') } },
    },
    {
      outcome: { message: (reply3?.result?.message ?? '') },
      result: { status: (reply3?.outcome?.status ?? '') },
    },
    {
      wristband: {
        code: (request?.wristband?.code ?? ''),
        wristband_id: (request?.wristband_id ?? ''),
      },
    },
  );
}

module.exports = { advanceWristbandWorkflow, cancelWristbandWorkflow, inspectWristbandWorkflow, listWristbandWorkflows, pauseWristbandWorkflow, resumeWristbandWorkflow, retryWristbandWorkflow, signalWristbandWorkflow, startWristbandWorkflow };
