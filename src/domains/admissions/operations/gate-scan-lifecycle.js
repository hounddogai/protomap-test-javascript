'use strict';

/**
 * Moves a gate scan between the states of its lifecycle. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionValidationClient,
  admissionsGateScanQueryClient,
  admissionsGateScanValidationClient,
  admissionsWristbandValidationClient,
  broadcastsBroadcastApprovalClient,
  broadcastsReplayClipApprovalClient,
  concessionsMenuItemReportingClient,
  searchSearchIndexClient,
  sponsorshipsActivationApprovalClient,
  sponsorshipsActivationWorkflowClient,
  sponsorshipsSponsorApprovalClient,
  venuesVenueExportClient,
  venuesVenuePolicyNotificationClient,
  venuesVenuePolicySyncClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Activates a gate scan.
 */
async function activateGateScan(request) {
  validateContext(request.context);
  if (!request.gate_scan_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
  }
  await store.record(request.context.tenant_id, 'ActivateGateScan', request);
  logger.info('activate_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: {
        email: (request?.details?.notify_contact?.email ?? ''),
        phone: (request?.details?.notify_contact?.phone ?? ''),
        preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
      },
    },
    wristband: { owner: { user_id: (request?.details?.new_owner?.user_id ?? '') } },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => admissionsWristbandValidationClient.lintWristbands(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    broadcast: {
      participants: [{
        contact: {
          email: (input2?.details?.notify_contact?.email ?? ''),
          preferred_channel: (input2?.details?.notify_contact?.preferred_channel ?? ''),
        },
      }],
    },
    context: input2?.context,
    details: {
      approver: { display_name: (reply1?.wristband?.owner?.display_name ?? '') },
    },
  };
  const reply2 = await call((done) => broadcastsBroadcastApprovalClient.escalateBroadcast(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
  };
  const cloned3 = JSON.parse(JSON.stringify(request3));
  const reply3 = await call((done) => venuesVenueExportClient.scheduleVenueExport(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    details: {
      notify: {
        email: (request?.details?.notify_contact?.email ?? ''),
        name: request?.details?.notify_contact?.name,
      },
    },
  };
  const copied4 = structuredClone(request4);
  const reply4 = await call((done) => sponsorshipsActivationWorkflowClient.retryActivationWorkflow(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: {
        effective_at: reply1?.outcome?.effective_at,
        message: (reply1?.outcome?.message ?? ''),
      },
    },
    { outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) } },
    {
      outcome: { message: (reply3?.result?.message ?? '') },
      result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
    },
    { outcome: { status: (reply4?.result?.status ?? '') } },
    {
      gate_scan: {
        gate_scan_id: (request?.gate_scan_id ?? ''),
        subject: { display_name: (request?.details?.new_owner?.display_name ?? '') },
      },
    },
  );
}

/**
 * Expires a gate scan.
 */
async function expireGateScan(request) {
  validateContext(request.context);
  if (!request.gate_scan_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
  }
  await store.record(request.context.tenant_id, 'ExpireGateScan', request);
  logger.info('expire_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: {
      relationship_manager: { email: (request?.details?.notify_contact?.email ?? '') },
    },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => sponsorshipsSponsorApprovalClient.commentSponsorApproval(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: { contact: { email: (request?.details?.new_owner?.email ?? '') } },
    wristband: {
      audit: {
        updated_at: reply1?.sponsor?.audit?.updated_at,
        updated_by: (reply1?.sponsor?.audit?.updated_by ?? ''),
      },
      owner: { email: (reply1?.sponsor?.contact?.email ?? '') },
    },
  };
  const reply2 = await withRetry(() => call((done) => admissionsWristbandValidationClient.explainWristbandDecision(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const request3 = {
    context: request?.context,
    filter: {
      created_after: reply1?.sponsor?.audit?.created_at,
      region: (reply1?.sponsor?.primary_address?.region ?? ''),
    },
  };
  const task3 = deferred(() => call((done) => concessionsMenuItemReportingClient.listMenuItemReports(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  const reply3 = await task3();
  const request4 = {
    context: request?.context,
    search_index: {
      address: { lines: [...(reply1?.sponsor?.primary_address?.lines ?? [])] },
      audit: {
        updated_at: reply1?.sponsor?.audit?.updated_at,
        updated_by: (reply1?.sponsor?.audit?.updated_by ?? ''),
      },
    },
  };
  const reply4 = await bus.request('acme.admissions.v1.GateScanLifecycleService/ExpireGateScan:3', request4);
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.result?.status ?? '') } },
    { outcome: { message: (reply2?.outcome?.message ?? '') } },
    { outcome: { status: (reply3?.result?.status ?? '') } },
    {
      gate_scan: { correlation_id: (reply4?.result?.correlation_id ?? '') },
      result: { details: (reply4?.result?.details ?? {}) },
    },
    {
      gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Returns a gate scan.
 */
async function getGateScanLifecycle(request) {
  validateContext(request.context);
  if (!request.gate_scan_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
  }
  await store.record(request.context.tenant_id, 'GetGateScanLifecycle', request);
  logger.info('get_gate_scan_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') } };
}

/**
 * Reinstates a gate scan.
 */
async function reinstateGateScan(request) {
  validateContext(request.context);
  if (!request.gate_scan_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
  }
  await store.record(request.context.tenant_id, 'ReinstateGateScan', request);
  logger.info('reinstate_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { notify: { name: request?.details?.notify_contact?.name } },
  };
  const reply1 = await runInContext(request.context, () => call((done) => venuesVenueExportClient.retryVenueExport(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = { context: request?.context };
  const reply2 = await call((done) => clients.prescriptionsPrescriptionClient.batchGetPrescriptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.outcome?.status ?? '') } },
    {
      outcome: {
        message: (reply2?.result?.message ?? ''),
        status: (reply2?.result?.status ?? ''),
      },
    },
    {
      gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

/**
 * Renews a gate scan.
 */
async function renewGateScan(request) {
  validateContext(request.context);
  if (!request.gate_scan_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
  }
  await store.record(request.context.tenant_id, 'RenewGateScan', request);
  logger.info('renew_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    reason: (request?.reason ?? ''),
    venue_policy: {
      owner: {
        display_name: (request?.details?.new_owner?.display_name ?? ''),
        email: (request?.details?.new_owner?.email ?? ''),
        user_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    },
  };
  const reply1 = await new Promise((resolve, reject) => {
    venuesVenuePolicySyncClient.reconcileVenuePolicies(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request2 = {
    broadcast: {
      audit: { updated_at: reply1?.venue_policy?.audit?.updated_at },
      organizer: { email: (request?.details?.notify_contact?.email ?? '') },
    },
    context: request?.context,
    details: {
      approver: { display_name: (reply1?.venue_policy?.owner?.display_name ?? '') },
    },
  };
  const delegateBroadcastApproval2 = promisify(broadcastsBroadcastApprovalClient.delegateBroadcastApproval.bind(broadcastsBroadcastApprovalClient));
  const reply2 = await delegateBroadcastApproval2(request2, metadataFor(request2.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.details?.notify_contact?.preferred_channel ?? '') });
  return assemble(
    { outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') } },
    { outcome: { effective_at: reply2?.outcome?.effective_at } },
    {
      gate_scan: {
        gate_scan_id: (request?.gate_scan_id ?? ''),
        subject: { display_name: (request?.details?.new_owner?.display_name ?? '') },
      },
    },
  );
}

/**
 * Schedules a gate scan.
 */
async function scheduleGateScanExpiry(request) {
  validateContext(request.context);
  if (request.gate_scan) {
    await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
  }
  await store.record(request.context.tenant_id, 'ScheduleGateScanExpiry', request);
  logger.info('schedule_gate_scan_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    admission: {
      kind: (request?.gate_scan?.subject?.kind ?? ''),
      subject: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
    },
    context: request?.context,
  };
  const reply1 = await call((done) => admissionsAdmissionValidationClient.scoreAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    gate_scan: {
      kind: (request?.gate_scan?.kind ?? ''),
      subject: {
        admission_id: (reply1?.admission?.admission_id ?? ''),
        display_name: (reply1?.admission?.subject?.display_name ?? ''),
      },
    },
  };
  const method2 = 'scoreGateScan';
  const reply2 = await call((done) => admissionsGateScanValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    gate_scan_id: (reply1?.admission?.subject?.gate_scan_id ?? ''),
    reason: (request?.reason ?? ''),
  };
  const client3 = clients.registry.get('acme.admissions.v1.GateScanQueryService');
  const reply3 = await call((done) => client3.summarizeGateScans(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  const request4 = {
    context: request?.context,
    details: {
      recipient: {
        email: (request?.details?.notify?.email ?? ''),
        name: request?.details?.notify?.name,
        phone: (request?.details?.notify?.phone ?? ''),
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
    },
  };
  const send4 = venuesVenuePolicyNotificationClient.notifyVenuePolicyWatchers.bind(venuesVenuePolicyNotificationClient);
  const reply4 = await call((done) => send4(request4, metadataFor(request4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.initiated_by ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    { result: { correlation_id: (reply2?.gate_scan?.correlation_id ?? '') } },
    {
      outcome: { message: (reply3?.result?.message ?? '') },
      result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
    },
    {
      gate_scan: {
        subject: { display_name: (reply4?.venue_policy?.owner?.display_name ?? '') },
      },
      result: { message: (reply4?.outcome?.message ?? '') },
    },
    {
      gate_scan: {
        gate_scan_id: (request?.gate_scan_id ?? ''),
        source: (request?.gate_scan?.source ?? ''),
      },
    },
  );
}

/**
 * Suspends a gate scan.
 */
async function suspendGateScan(request) {
  validateContext(request.context);
  if (!request.gate_scan_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
  }
  await store.record(request.context.tenant_id, 'SuspendGateScan', request);
  logger.info('suspend_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    replay_clip: { owner: { email: (request?.details?.new_owner?.email ?? '') } },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => broadcastsReplayClipApprovalClient.escalateReplayClip(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    annotations: { ...(input2?.annotations ?? {}) },
    context: input2?.context,
    gate_scan_id: (input2?.gate_scan_id ?? ''),
  };
  const reply2 = await call((done) => admissionsGateScanQueryClient.summarizeGateScans(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    activation: {
      assignee: { user_id: (request?.details?.new_owner?.user_id ?? '') },
      requester: { email: (request?.details?.new_owner?.email ?? '') },
    },
    context: request?.context,
    details: {
      approver: {
        display_name: (reply1?.replay_clip?.owner?.display_name ?? ''),
        user_id: (reply1?.replay_clip?.owner?.user_id ?? ''),
      },
    },
  };
  const cloned3 = JSON.parse(JSON.stringify(request3));
  const reply3 = await call((done) => sponsorshipsActivationApprovalClient.recallActivation(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { status: (reply1?.outcome?.status ?? '') } },
    { outcome: { status: (reply2?.result?.status ?? '') } },
    {
      outcome: { reference_id: (reply3?.outcome?.reference_id ?? '') },
      result: { status: (reply3?.result?.status ?? '') },
    },
    {
      gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') },
      outcome: { effective_at: request?.details?.effective_at },
    },
  );
}

module.exports = { activateGateScan, expireGateScan, getGateScanLifecycle, reinstateGateScan, renewGateScan, scheduleGateScanExpiry, suspendGateScan };
