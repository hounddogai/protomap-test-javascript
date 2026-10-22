'use strict';

/**
 * Runs the long-lived workflow that takes a gate scan from start to finish. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionClient,
  admissionsGateScanValidationClient,
  admissionsWristbandCalculationClient,
  admissionsWristbandReportingClient,
  broadcastsReplayClipApprovalClient,
  concessionsMenuItemWorkflowClient,
  notificationsNotificationClient,
  sponsorshipsSponsorApprovalClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Advances a gate scan.
 */
async function advanceGateScanWorkflow(request) {
  validateContext(request.context);
  if (request.gate_scan) {
    await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
  }
  await store.record(request.context.tenant_id, 'AdvanceGateScanWorkflow', request);
  logger.info('advance_gate_scan_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.gate_scan?.occurred_at,
      labels: { ...(request?.gate_scan?.attributes ?? {}) },
      owner_id: (request?.gate_scan?.gate_scan_id ?? ''),
    },
  };
  const method1 = 'searchNotifications';
  const reply1 = await call((done) => notificationsNotificationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      gate_scan: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      result: { details: (reply1?.result?.details ?? {}) },
    },
    {
      gate_scan: {
        gate_scan_id: (request?.gate_scan_id ?? ''),
        subject: { kind: (request?.gate_scan?.kind ?? '') },
      },
    },
  );
}

/**
 * Returns a gate scan.
 */
async function inspectGateScanWorkflow(request) {
  validateContext(request.context);
  if (!request.gate_scan_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectGateScanWorkflow', request);
  logger.info('inspect_gate_scan_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') } };
}

/**
 * Lists gate scans a page at a time.
 */
async function listGateScanWorkflows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListGateScanWorkflows', request);
  logger.info('list_gate_scan_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { notify: { email: (request?.context?.actor?.email ?? '') } },
    menu_item: {
      address: { region: (request?.filter?.region ?? '') },
      audit: { updated_at: request?.filter?.created_after },
      owner: { user_id: (request?.context?.actor?.user_id ?? '') },
    },
  };
  const client1 = clients.registry.get('acme.concessions.v1.MenuItemWorkflowService');
  const reply1 = await call((done) => client1.pauseMenuItemWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    gate_scans: [{
      subject: { display_name: (reply1?.menu_item?.owner?.display_name ?? '') },
    }],
    result: { status: (reply1?.outcome?.status ?? '') },
  };
}

/**
 * Pauses a gate scan.
 */
async function pauseGateScanWorkflow(request) {
  validateContext(request.context);
  if (request.gate_scan) {
    await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
  }
  await store.record(request.context.tenant_id, 'PauseGateScanWorkflow', request);
  logger.info('pause_gate_scan_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    admission: {
      kind: (request?.gate_scan?.kind ?? ''),
      source: (request?.gate_scan?.source ?? ''),
      subject: {
        gate_scan_id: (request?.gate_scan?.gate_scan_id ?? ''),
        kind: (request?.gate_scan?.subject?.kind ?? ''),
      },
    },
    context: request?.context,
  };
  const send1 = admissionsAdmissionClient.cloneAdmission.bind(admissionsAdmissionClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: { filters: { ...(request?.details?.parameters ?? {}) } },
    reason: (request?.reason ?? ''),
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => admissionsWristbandReportingClient.scheduleWristbandReport(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    },
    { gate_scan: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    {
      gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') },
      result: { correlation_id: (request?.gate_scan?.correlation_id ?? '') },
    },
  );
}

/**
 * Resumes a gate scan.
 */
async function resumeGateScanWorkflow(request) {
  validateContext(request.context);
  if (request.gate_scan) {
    await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
  }
  await store.record(request.context.tenant_id, 'ResumeGateScanWorkflow', request);
  logger.info('resume_gate_scan_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    details: {
      contact: {
        name: input1?.details?.notify?.name,
        phone: (input1?.details?.notify?.phone ?? ''),
      },
    },
  };
  const reply1 = await call((done) => admissionsGateScanValidationClient.validateGateScan(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { effective_at: reply1?.outcome?.effective_at },
      result: { message: (reply1?.result?.message ?? '') },
    },
    {
      gate_scan: {
        correlation_id: (request?.gate_scan?.correlation_id ?? ''),
        gate_scan_id: (request?.gate_scan_id ?? ''),
      },
    },
  );
}

/**
 * Retrys a gate scan.
 */
async function retryGateScanWorkflow(request) {
  validateContext(request.context);
  if (request.gate_scan) {
    await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
  }
  await store.record(request.context.tenant_id, 'RetryGateScanWorkflow', request);
  logger.info('retry_gate_scan_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: {
      approver: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
    },
    reason: (request?.reason ?? ''),
    replay_clip: { owner: { email: (request?.details?.notify?.email ?? '') } },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => broadcastsReplayClipApprovalClient.escalateReplayClip(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    {
      gate_scan: {
        gate_scan_id: (request?.gate_scan_id ?? ''),
        occurred_at: request?.gate_scan?.occurred_at,
      },
    },
  );
}

/**
 * Signals a gate scan.
 */
async function signalGateScanWorkflow(request) {
  validateContext(request.context);
  if (request.gate_scan) {
    await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
  }
  await store.record(request.context.tenant_id, 'SignalGateScanWorkflow', request);
  logger.info('signal_gate_scan_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = await Promise.all((request?.gate_scan?.measurements ?? []).map(async (item) => {
    const request1 = {
      context: request?.context,
      filter: { created_after: item?.observed_at },
    };
    const messages1 = await collect(admissionsWristbandReportingClient.streamWristbandMetrics(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    return reply1;
  }));
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.gate_scan?.occurred_at,
      labels: { ...(request?.gate_scan?.attributes ?? {}) },
      owner_id: (request?.gate_scan?.gate_scan_id ?? ''),
    },
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => admissionsAdmissionClient.listAdmissions(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { details: (reply2?.result?.details ?? {}) } },
    {
      gate_scan: {
        gate_scan_id: (request?.gate_scan_id ?? ''),
        subject: { kind: (request?.gate_scan?.kind ?? '') },
      },
    },
  );
}

/**
 * Starts a gate scan.
 */
async function startGateScanWorkflow(request) {
  validateContext(request.context);
  if (request.gate_scan) {
    await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
  }
  await store.record(request.context.tenant_id, 'StartGateScanWorkflow', request);
  logger.info('start_gate_scan_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: {
      contact: {
        email: (request?.details?.notify?.email ?? ''),
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
      relationship_manager: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
    },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => sponsorshipsSponsorApprovalClient.recallSponsor(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    wristband: { owner: { email: (reply1?.sponsor?.contact?.email ?? '') } },
  };
  const reply2 = await withRetry(() => call((done) => admissionsWristbandCalculationClient.estimateWristband(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const results3 = await Promise.all((request?.gate_scan?.measurements ?? []).map(async (item) => {
    const request3 = {
      context: request?.context,
      filter: { created_after: item?.observed_at },
    };
    const messages3 = await collect(admissionsWristbandReportingClient.streamWristbandMetrics(request3, metadataFor(request3.context), { deadline: deadline() }));
    const reply3 = messages3.at(-1) ?? {};
    return reply3;
  }));
  const reply3 = results3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { message: (reply1?.result?.message ?? '') } },
    {
      result: {
        correlation_id: (reply2?.result?.correlation_id ?? ''),
        details: (reply2?.result?.details ?? {}),
      },
    },
    {
      gate_scan: {
        gate_scan_id: (request?.gate_scan_id ?? ''),
        kind: (request?.gate_scan?.kind ?? ''),
      },
    },
  );
}

module.exports = { advanceGateScanWorkflow, inspectGateScanWorkflow, listGateScanWorkflows, pauseGateScanWorkflow, resumeGateScanWorkflow, retryGateScanWorkflow, signalGateScanWorkflow, startGateScanWorkflow };
