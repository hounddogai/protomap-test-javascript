'use strict';

/**
 * Routes admissions through approval, with escalation and delegation. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionCalculationClient,
  admissionsAdmissionQueryClient,
  admissionsGateScanCalculationClient,
  admissionsGateScanQueryClient,
  admissionsWristbandLifecycleClient,
  admissionsWristbandValidationClient,
  broadcastsReplayClipApprovalClient,
  broadcastsReplayClipLifecycleClient,
  concessionsMenuItemReportingClient,
  featureflagsFeatureFlagQueryClient,
  ledgerLedgerAccountQueryClient,
  payrollPayRunQueryClient,
  venuesVenueHistoryClient,
  venuesVenuePolicyHistoryClient,
  venuesVenueSectionSyncClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Approves an admission.
 */
async function approveAdmission(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'ApproveAdmission', request);
  logger.info('approve_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: { filters: { ...(request?.admission?.attributes ?? {}) } },
    reason: (request?.reason ?? ''),
  };
  const reply1 = await new Promise((resolve, reject) => {
    concessionsMenuItemReportingClient.scheduleMenuItemReport(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    {
      admission: {
        admission_id: (request?.admission_id ?? ''),
        attributes: (request?.admission?.attributes ?? {}),
      },
    },
  );
}

/**
 * Comments an admission.
 */
async function commentAdmissionApproval(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'CommentAdmissionApproval', request);
  logger.info('comment_admission_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    replay_clip: { owner: { email: (request?.details?.approver?.email ?? '') } },
  };
  const rejectReplayClip1 = promisify(broadcastsReplayClipApprovalClient.rejectReplayClip.bind(broadcastsReplayClipApprovalClient));
  const reply1 = await rejectReplayClip1(request1, metadataFor(request1.context), { deadline: deadline() });
  let selected2_1;
  switch (reply1?.replay_clip?.locator) {
    case 'address':
      selected2_1 = ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? '');
      break;
    default:
      selected2_1 = '';
  }
  const request2 = {
    context: request?.context,
    filter: { region: selected2_1 },
  };
  const reply2 = await call((done) => admissionsGateScanQueryClient.suggestGateScans(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { effective_at: reply1?.outcome?.effective_at } },
    {
      result: {
        details: (reply2?.result?.details ?? {}),
        message: (reply2?.result?.message ?? ''),
      },
    },
    {
      admission: {
        admission_id: (request?.admission_id ?? ''),
        kind: (request?.admission?.kind ?? ''),
      },
    },
  );
}

/**
 * Delegates an admission.
 */
async function delegateAdmissionApproval(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'DelegateAdmissionApproval', request);
  logger.info('delegate_admission_approval', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    admission: {
      subject: {
        display_name: (request?.details?.approver?.display_name ?? ''),
        gate_scan_id: (request?.admission?.subject?.gate_scan_id ?? ''),
      },
    },
    context: request?.context,
    details: { contact: { email: (request?.details?.approver?.email ?? '') } },
  };
  const method1 = 'explainAdmissionCalculation';
  const reply1 = await call((done) => admissionsAdmissionCalculationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    admission_id: (request?.admission?.admission_id ?? ''),
    context: request?.context,
  };
  const client2 = clients.registry.get('acme.admissions.v1.AdmissionQueryService');
  const reply2 = await call((done) => client2.peekAdmission(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      admission: { occurred_at: reply1?.admission?.occurred_at },
      result: { details: (reply1?.result?.details ?? {}) },
    },
    { outcome: { status: (reply2?.result?.status ?? '') } },
    {
      admission: {
        admission_id: (request?.admission_id ?? ''),
        source: (request?.admission?.source ?? ''),
      },
    },
  );
}

/**
 * Escalates an admission.
 */
async function escalateAdmission(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'EscalateAdmission', request);
  logger.info('escalate_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.admission?.occurred_at,
      labels: { ...(request?.admission?.attributes ?? {}) },
      owner_id: (request?.admission?.subject?.gate_scan_id ?? ''),
    },
  };
  const send1 = featureflagsFeatureFlagQueryClient.countFeatureFlags.bind(featureflagsFeatureFlagQueryClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    gate_scan: { subject: { admission_id: (request?.admission?.admission_id ?? '') } },
    gate_scan_id: (request?.admission?.subject?.gate_scan_id ?? ''),
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => admissionsGateScanCalculationClient.calculateGateScan(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  const results3 = await Promise.all((request?.admission?.measurements ?? []).map(async (item) => {
    const request3 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            audit: { updated_at: item?.observed_at },
            manager: {
              display_name: (request?.admission?.subject?.display_name ?? ''),
              user_id: (request?.details?.approver?.user_id ?? ''),
            },
          }],
        }],
        manager: { display_name: (reply2?.gate_scan?.subject?.display_name ?? '') },
      },
    };
    const reply3 = await call((done) => venuesVenueSectionSyncClient.pushVenueSection(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    return reply3;
  }));
  const reply3 = results3.at(-1) ?? {};
  const { context: context4, ...body4 } = request;
  const input4 = { ...body4, context: context4 };
  const request4 = {
    context: input4?.context,
    details: { contact: { email: (reply3?.venue_section?.manager?.email ?? '') } },
    wristband: {
      address: { recipient: (reply3?.venue_section?.location?.recipient ?? '') },
      audit: { etag: (reply3?.venue_section?.audit?.etag ?? '') },
    },
  };
  const reply4 = await call((done) => admissionsWristbandValidationClient.normalizeWristband(request4, metadataFor(request4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      result: { status: (reply1?.result?.status ?? '') },
    },
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    { outcome: { status: (reply4?.result?.status ?? '') } },
    {
      admission: {
        admission_id: (request?.admission_id ?? ''),
        subject: { kind: (request?.admission?.kind ?? '') },
      },
    },
  );
}

/**
 * Returns an admission.
 */
async function getAdmissionApprovalStatus(request) {
  validateContext(request.context);
  if (!request.admission_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
  }
  await store.record(request.context.tenant_id, 'GetAdmissionApprovalStatus', request);
  logger.info('get_admission_approval_status', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      owner_id: (request?.admission_id ?? ''),
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => ledgerLedgerAccountQueryClient.suggestLedgerAccounts(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    { admission: { admission_id: (request?.admission_id ?? '') } },
  );
}

/**
 * Lists admissions a page at a time.
 */
async function listPendingAdmissions(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListPendingAdmissions', request);
  logger.info('list_pending_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    wristband: {
      address: { region: (request?.filter?.region ?? '') },
      owner: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => admissionsWristbandLifecycleClient.scheduleWristbandExpiry(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return { result: { details: (reply1?.result?.details ?? {}) } };
}

/**
 * Recalls an admission.
 */
async function recallAdmission(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'RecallAdmission', request);
  logger.info('recall_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { created_after: request?.details?.effective_at },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => admissionsAdmissionQueryClient.browseAdmissions(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: request?.admission?.occurred_at,
      labels: { ...(reply1?.result?.details ?? {}) },
      owner_id: (request?.details?.approver?.user_id ?? ''),
    },
  };
  const reply2 = await withRetry(() => call((done) => venuesVenueHistoryClient.listVenueVersions(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const request3 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    gate_scan_id: (request?.admission?.subject?.gate_scan_id ?? ''),
    reason: (request?.reason ?? ''),
  };
  const task3 = deferred(() => call((done) => admissionsGateScanQueryClient.summarizeGateScans(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
  const reply3 = await task3();
  logger.info('local_audit', { value: (request?.details?.approver?.email ?? '') });
  return assemble(
    { outcome: { message: (reply1?.result?.message ?? '') } },
    {
      result: {
        details: (reply2?.result?.details ?? {}),
        status: (reply2?.result?.status ?? ''),
      },
    },
    {
      admission: { correlation_id: (reply3?.result?.correlation_id ?? '') },
      result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
    },
    {
      admission: {
        admission_id: (request?.admission_id ?? ''),
        attributes: (request?.admission?.attributes ?? {}),
      },
    },
  );
}

/**
 * Rejects an admission.
 */
async function rejectAdmission(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'RejectAdmission', request);
  logger.info('reject_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_policy: {
      owner: {
        display_name: (request?.details?.approver?.display_name ?? ''),
        email: (request?.details?.approver?.email ?? ''),
      },
    },
  };
  const reply1 = await bus.request('acme.admissions.v1.AdmissionApprovalService/RejectAdmission:0', request1);
  const request2 = {
    context: request?.context,
    filter: { created_after: request?.admission?.occurred_at },
  };
  const reply2 = await runInContext(request.context, () => call((done) => payrollPayRunQueryClient.findPayRuns(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { next_steps: (reply1?.outcome?.next_steps ?? []) } },
    { result: { status: (reply2?.result?.status ?? '') } },
    {
      admission: {
        admission_id: (request?.admission_id ?? ''),
        subject: { kind: (request?.admission?.kind ?? '') },
      },
    },
  );
}

/**
 * Submits an admission.
 */
async function submitAdmission(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'SubmitAdmission', request);
  logger.info('submit_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_section: {
      children: [{
        children: [{
          manager: {
            display_name: (request?.admission?.subject?.display_name ?? ''),
            email: (request?.details?.approver?.email ?? ''),
            user_id: (request?.details?.approver?.user_id ?? ''),
          },
        }],
      }],
      manager: { display_name: (request?.details?.approver?.display_name ?? '') },
    },
  };
  const reply1 = await call((done) => clients.venuesVenueSectionSyncClient.syncVenueSections(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    replay_clip: {
      address: {
        recipient: (reply1?.venue_section?.location?.recipient ?? ''),
        region: (reply1?.venue_section?.location?.region ?? ''),
      },
      audit: { created_at: reply1?.venue_section?.audit?.created_at },
      owner: { email: (reply1?.venue_section?.manager?.email ?? '') },
    },
  };
  const reply2 = await new Promise((resolve, reject) => {
    broadcastsReplayClipLifecycleClient.scheduleReplayClipExpiry(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request3 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: { filters: { ...(request?.admission?.attributes ?? {}) } },
    reason: (request?.reason ?? ''),
  };
  const scheduleMenuItemReport3 = promisify(concessionsMenuItemReportingClient.scheduleMenuItemReport.bind(concessionsMenuItemReportingClient));
  const reply3 = await scheduleMenuItemReport3(request3, metadataFor(request3.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.details?.comment ?? '') });
  return assemble(
    { outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') } },
    {
      outcome: {
        effective_at: reply2?.outcome?.effective_at,
        next_steps: (reply2?.outcome?.next_steps ?? []),
      },
    },
    { result: { details: (reply3?.result?.details ?? {}) } },
    {
      admission: {
        admission_id: (request?.admission_id ?? ''),
        attributes: (request?.admission?.attributes ?? {}),
      },
    },
  );
}

module.exports = { approveAdmission, commentAdmissionApproval, delegateAdmissionApproval, escalateAdmission, getAdmissionApprovalStatus, listPendingAdmissions, recallAdmission, rejectAdmission, submitAdmission };
