'use strict';

/**
 * Runs the long-lived workflow that takes an admission from start to finish. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionLifecycleClient,
  admissionsGateScanApprovalClient,
  admissionsGateScanCalculationClient,
  admissionsGateScanValidationClient,
  admissionsWristbandReportingClient,
  concessionsConcessionOrderWorkflowClient,
  concessionsConcessionStandWorkflowClient,
  documentsDocumentValidationClient,
  maintenanceWorkOrderCalculationClient,
  sponsorshipsSponsorApprovalClient,
  sponsorshipsSponsorshipDealWorkflowClient,
  venuesVenuePolicyNotificationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Advances an admission.
 */
async function advanceAdmissionWorkflow(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'AdvanceAdmissionWorkflow', request);
  logger.info('advance_admission_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      approver: {
        display_name: (request?.admission?.subject?.display_name ?? ''),
        email: (request?.details?.notify?.email ?? ''),
      },
    },
    sponsor: { contact: { phone: (request?.details?.notify?.phone ?? '') } },
  };
  const method1 = 'escalateSponsor';
  const reply1 = await call((done) => sponsorshipsSponsorApprovalClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: (reply1?.sponsor?.identity_document === 'passport' ? reply1?.sponsor.passport : undefined)?.expires_at,
      region: (reply1?.sponsor?.primary_address?.region ?? ''),
    },
  };
  const client2 = clients.registry.get('acme.admissions.v1.WristbandReportingService');
  const reply2 = await call((done) => client2.listWristbandReports(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const results3 = [];
  for (const item of (request?.admission?.measurements ?? [])) {
    const request3 = {
      concession_order: {
        audit: {
          updated_at: item?.observed_at,
          updated_by: (reply1?.sponsor?.audit?.updated_by ?? ''),
        },
      },
      context: request?.context,
      details: {
        notify: {
          email: (request?.details?.notify?.email ?? ''),
          preferred_channel: (reply1?.sponsor?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply3 = await call((done) => concessionsConcessionOrderWorkflowClient.advanceConcessionOrderWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    results3.push(reply3);
  }
  const reply3 = results3.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') } },
    {
      outcome: { status: (reply2?.result?.status ?? '') },
      result: { status: (reply2?.result?.status ?? '') },
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
 * Cancels an admission.
 */
async function cancelAdmissionWorkflow(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'CancelAdmissionWorkflow', request);
  logger.info('cancel_admission_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    admission_id: (request?.admission_id ?? ''),
    context: request?.context,
  };
  const send1 = admissionsAdmissionLifecycleClient.getAdmissionLifecycle.bind(admissionsAdmissionLifecycleClient);
  const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    work_order: {
      assignee: { display_name: (request?.admission?.subject?.display_name ?? '') },
      requester: { phone: (request?.details?.notify?.phone ?? '') },
    },
  };
  const spread2 = {
    ...request2,
    context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
  };
  const reply2 = await call((done) => maintenanceWorkOrderCalculationClient.compareWorkOrderOptions(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
    {
      admission: {
        subject: {
          display_name: ((reply2?.work_order?.origin === 'customer' ? reply2?.work_order.customer : undefined)?.display_name ?? ''),
        },
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
 * Returns an admission.
 */
async function inspectAdmissionWorkflow(request) {
  validateContext(request.context);
  if (!request.admission_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
  }
  await store.record(request.context.tenant_id, 'InspectAdmissionWorkflow', request);
  logger.info('inspect_admission_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { admission: { admission_id: (request?.admission_id ?? '') } };
}

/**
 * Lists admissions a page at a time.
 */
async function listAdmissionWorkflows(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListAdmissionWorkflows', request);
  logger.info('list_admission_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    details: { contact: { email: (input1?.context?.actor?.email ?? '') } },
  };
  const reply1 = await call((done) => admissionsGateScanValidationClient.checkGateScanEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {
    admissions: [{
      subject: { display_name: (reply1?.gate_scan?.subject?.display_name ?? '') },
    }],
    result: { message: (reply1?.outcome?.message ?? '') },
  };
}

/**
 * Pauses an admission.
 */
async function pauseAdmissionWorkflow(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'PauseAdmissionWorkflow', request);
  logger.info('pause_admission_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: {
      contact: {
        preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
      },
      relationship_manager: { display_name: (request?.admission?.subject?.display_name ?? '') },
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => sponsorshipsSponsorApprovalClient.rejectSponsor(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  const request2 = {
    concession_stand: {
      address: {
        lines: [...(reply1?.sponsor?.primary_address?.lines ?? [])],
        region: (reply1?.sponsor?.primary_address?.region ?? ''),
      },
    },
    context: request?.context,
  };
  const copied2 = structuredClone(request2);
  const reply2 = await call((done) => concessionsConcessionStandWorkflowClient.advanceConcessionStandWorkflow(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    document: {
      lines: [{
        description: (reply2?.concession_stand?.components?.primary?.description ?? ''),
      }],
    },
  };
  const merged3 = Object.assign({}, request3, { context: request3.context });
  const reply3 = await call((done) => documentsDocumentValidationClient.checkDocumentEligibility(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { effective_at: reply1?.outcome?.effective_at } },
    {
      admission: {
        measurements: [{ unit: (reply2?.concession_stand?.dimensions?.unit ?? '') }],
      },
      result: { details: (reply2?.result?.details ?? {}) },
    },
    {
      admission: { correlation_id: (reply3?.result?.correlation_id ?? '') },
      result: { message: (reply3?.result?.message ?? '') },
    },
    {
      admission: {
        admission_id: (request?.admission_id ?? ''),
        subject: request?.admission?.subject,
      },
    },
  );
}

/**
 * Resumes an admission.
 */
async function resumeAdmissionWorkflow(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'ResumeAdmissionWorkflow', request);
  logger.info('resume_admission_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      notify: {
        email: (request?.details?.notify?.email ?? ''),
        name: request?.details?.notify?.name,
      },
    },
  };
  const reply1 = await withRetry(() => call((done) => sponsorshipsSponsorshipDealWorkflowClient.advanceSponsorshipDealWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const request2 = {
    context: request?.context,
    details: {
      destination: { region: (reply1?.sponsorship_deal?.billing_address?.region ?? '') },
    },
    gate_scan: {
      subject: {
        display_name: (reply1?.sponsorship_deal?.counterparty?.display_name ?? ''),
      },
    },
  };
  const task2 = deferred(() => call((done) => admissionsGateScanCalculationClient.compareGateScanOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  const reply2 = await task2();
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: {
        reference_id: (reply1?.outcome?.reference_id ?? ''),
        status: (reply1?.result?.status ?? ''),
      },
    },
    {
      admission: { correlation_id: (reply2?.gate_scan?.correlation_id ?? '') },
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
 * Retrys an admission.
 */
async function retryAdmissionWorkflow(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'RetryAdmissionWorkflow', request);
  logger.info('retry_admission_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    details: { filters: { ...(request?.admission?.attributes ?? {}) } },
    reason: (request?.reason ?? ''),
  };
  const reply1 = await bus.request('acme.admissions.v1.AdmissionWorkflowService/RetryAdmissionWorkflow:0', request1);
  logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
  return assemble(
    {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        status: (reply1?.result?.status ?? ''),
      },
    },
    {
      admission: {
        admission_id: (request?.admission_id ?? ''),
        occurred_at: request?.admission?.occurred_at,
      },
    },
  );
}

/**
 * Signals an admission.
 */
async function signalAdmissionWorkflow(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'SignalAdmissionWorkflow', request);
  logger.info('signal_admission_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: { approver: { email: (request?.details?.notify?.email ?? '') } },
    sponsor: {
      attributes: { ...(request?.admission?.attributes ?? {}) },
      contact: {
        name: request?.details?.notify?.name,
        phone: (request?.details?.notify?.phone ?? ''),
      },
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => sponsorshipsSponsorApprovalClient.submitSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { outcome: { next_steps: (reply1?.outcome?.next_steps ?? []) } },
    {
      admission: {
        admission_id: (request?.admission_id ?? ''),
        correlation_id: (request?.admission?.correlation_id ?? ''),
      },
    },
  );
}

/**
 * Starts an admission.
 */
async function startAdmissionWorkflow(request) {
  validateContext(request.context);
  if (request.admission) {
    await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
  }
  await store.record(request.context.tenant_id, 'StartAdmissionWorkflow', request);
  logger.info('start_admission_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    reason: (request?.reason ?? ''),
  };
  const reply1 = await call((done) => clients.admissionsGateScanQueryClient.summarizeGateScans(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      recipient: {
        name: request?.details?.notify?.name,
        phone: (request?.details?.notify?.phone ?? ''),
      },
    },
  };
  const reply2 = await new Promise((resolve, reject) => {
    venuesVenuePolicyNotificationClient.sendVenuePolicyTestNotice(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  const request3 = {
    context: request?.context,
    details: {
      approver: {
        email: (request?.details?.notify?.email ?? ''),
        user_id: (reply2?.venue_policy?.owner?.user_id ?? ''),
      },
    },
    gate_scan: {
      subject: { display_name: (reply2?.venue_policy?.owner?.display_name ?? '') },
    },
  };
  const recallGateScan3 = promisify(admissionsGateScanApprovalClient.recallGateScan.bind(admissionsGateScanApprovalClient));
  const reply3 = await recallGateScan3(request3, metadataFor(request3.context), { deadline: deadline() });
  const request4 = {
    context: request?.context,
    details: { contact: { email: (request?.details?.notify?.email ?? '') } },
  };
  const reply4 = await call((done) => documentsDocumentValidationClient.previewDocument(request4, metadataFor(request4.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    {
      admission: { correlation_id: (reply2?.result?.correlation_id ?? '') },
      result: { details: (reply2?.result?.details ?? {}) },
    },
    {
      admission: { admission_id: (reply3?.gate_scan?.subject?.admission_id ?? '') },
      result: { details: (reply3?.result?.details ?? {}) },
    },
    { result: { status: (reply4?.result?.status ?? '') } },
    {
      admission: {
        sequence: (request?.admission?.sequence ?? '0'),
        source: (request?.admission?.source ?? ''),
      },
    },
  );
}

module.exports = { advanceAdmissionWorkflow, cancelAdmissionWorkflow, inspectAdmissionWorkflow, listAdmissionWorkflows, pauseAdmissionWorkflow, resumeAdmissionWorkflow, retryAdmissionWorkflow, signalAdmissionWorkflow, startAdmissionWorkflow };
