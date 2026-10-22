'use strict';

/**
 * Moves a wristband between the states of its lifecycle. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  admissionsAdmissionClient,
  admissionsAdmissionQueryClient,
  admissionsGateScanCalculationClient,
  admissionsWristbandReportingClient,
  admissionsWristbandValidationClient,
  concessionsConcessionOrderApprovalClient,
  concessionsConcessionOrderReportingClient,
  documentsDocumentValidationClient,
  shippingShipmentCalculationClient,
  sponsorshipsActivationWorkflowClient,
  venuesVenuePolicyHistoryClient,
  venuesVenuePolicyValidationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Activates a wristband.
 */
async function activateWristband(request) {
  validateContext(request.context);
  if (!request.wristband_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
  }
  await store.record(request.context.tenant_id, 'ActivateWristband', request);
  logger.info('activate_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
    },
  };
  const copied1 = structuredClone(request1);
  const reply1 = await call((done) => admissionsAdmissionQueryClient.countAdmissions(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { details: (reply1?.result?.details ?? {}) } },
    {
      outcome: { effective_at: request?.details?.effective_at },
      wristband: { wristband_id: (request?.wristband_id ?? '') },
    },
  );
}

/**
 * Expires a wristband.
 */
async function expireWristband(request) {
  validateContext(request.context);
  if (!request.wristband_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
  }
  await store.record(request.context.tenant_id, 'ExpireWristband', request);
  logger.info('expire_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: { owner_id: (request?.details?.new_owner?.user_id ?? '') },
  };
  const merged1 = Object.assign({}, request1, { context: request1.context });
  const reply1 = await call((done) => sponsorshipsActivationWorkflowClient.listActivationWorkflows(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
  const request2 = { context: request?.context };
  const reply2 = await withRetry(() => call((done) => admissionsAdmissionClient.batchGetAdmissions(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
    },
    {
      result: {
        correlation_id: (reply2?.result?.correlation_id ?? ''),
        details: (reply2?.result?.details ?? {}),
      },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      wristband: { wristband_id: (request?.wristband_id ?? '') },
    },
  );
}

/**
 * Returns a wristband.
 */
async function getWristbandLifecycle(request) {
  validateContext(request.context);
  if (!request.wristband_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
  }
  await store.record(request.context.tenant_id, 'GetWristbandLifecycle', request);
  logger.info('get_wristband_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      approver: {
        email: (request?.context?.actor?.email ?? ''),
        user_id: (request?.context?.actor?.user_id ?? ''),
      },
    },
  };
  const task1 = deferred(() => call((done) => concessionsConcessionOrderApprovalClient.recallConcessionOrder(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  const reply1 = await task1();
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    { wristband: { wristband_id: (request?.wristband_id ?? '') } },
  );
}

/**
 * Reinstates a wristband.
 */
async function reinstateWristband(request) {
  validateContext(request.context);
  if (!request.wristband_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
  }
  await store.record(request.context.tenant_id, 'ReinstateWristband', request);
  logger.info('reinstate_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    annotations: { ...(request?.annotations ?? {}) },
    context: request?.context,
    reason: (request?.reason ?? ''),
    shipment: {
      counterparty: { display_name: (request?.details?.new_owner?.display_name ?? '') },
    },
  };
  const reply1 = await bus.request('acme.admissions.v1.WristbandLifecycleService/ReinstateWristband:0', request1);
  const request2 = {
    context: request?.context,
    wristband_id: (request?.wristband_id ?? ''),
  };
  const reply2 = await runInContext(request.context, () => call((done) => admissionsWristbandReportingClient.fetchWristbandReport(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.details?.notify_contact?.preferred_channel ?? '') });
  return assemble(
    {
      result: { status: (reply1?.result?.status ?? '') },
      wristband: { audit: { created_by: (reply1?.shipment?.audit?.created_by ?? '') } },
    },
    { wristband: { capacity: (reply2?.wristband?.capacity ?? '0') } },
    {
      outcome: { effective_at: request?.details?.effective_at },
      wristband: { wristband_id: (request?.wristband_id ?? '') },
    },
  );
}

/**
 * Renews a wristband.
 */
async function renewWristband(request) {
  validateContext(request.context);
  if (!request.wristband_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
  }
  await store.record(request.context.tenant_id, 'RenewWristband', request);
  logger.info('renew_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      contact: {
        email: (request?.details?.new_owner?.email ?? ''),
        phone: (request?.details?.notify_contact?.phone ?? ''),
      },
    },
  };
  const reply1 = await call((done) => clients.admissionsAdmissionValidationClient.lintAdmissions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    filter: {
      created_after: reply1?.outcome?.effective_at,
      labels: { ...(reply1?.admission?.attributes ?? {}) },
      owner_id: (reply1?.admission?.subject?.gate_scan_id ?? ''),
    },
  };
  const reply2 = await new Promise((resolve, reject) => {
    admissionsAdmissionQueryClient.countAdmissions(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') },
      result: { message: (reply1?.outcome?.message ?? '') },
    },
    {
      result: {
        correlation_id: (reply2?.result?.correlation_id ?? ''),
        status: (reply2?.result?.status ?? ''),
      },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      wristband: { wristband_id: (request?.wristband_id ?? '') },
    },
  );
}

/**
 * Schedules a wristband.
 */
async function scheduleWristbandExpiry(request) {
  validateContext(request.context);
  if (request.wristband) {
    await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
  }
  await store.record(request.context.tenant_id, 'ScheduleWristbandExpiry', request);
  logger.info('schedule_wristband_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    document: {
      billing_address: {
        postal_code: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.postal_code ?? ''),
      },
    },
  };
  const lintDocuments1 = promisify(documentsDocumentValidationClient.lintDocuments.bind(documentsDocumentValidationClient));
  const reply1 = await lintDocuments1(request1, metadataFor(request1.context), { deadline: deadline() });
  const request2 = {
    context: request?.context,
    details: {
      destination: {
        postal_code: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.postal_code ?? ''),
      },
    },
  };
  const reply2 = await call((done) => admissionsWristbandValidationClient.scoreWristband(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      wristband: { audit: { created_by: (reply1?.document?.audit?.created_by ?? '') } },
    },
    { wristband: { code: (reply2?.wristband?.code ?? '') } },
    {
      wristband: {
        capacity: (request?.wristband?.capacity ?? '0'),
        wristband_id: (request?.wristband_id ?? ''),
      },
    },
  );
}

/**
 * Suspends a wristband.
 */
async function suspendWristband(request) {
  validateContext(request.context);
  if (!request.wristband_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
  }
  await store.record(request.context.tenant_id, 'SuspendWristband', request);
  logger.info('suspend_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      owner_id: (request?.details?.new_owner?.user_id ?? ''),
    },
  };
  const method1 = 'listConcessionOrderReports';
  const reply1 = await call((done) => concessionsConcessionOrderReportingClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    gate_scan: {
      subject: { display_name: (request?.details?.new_owner?.display_name ?? '') },
    },
  };
  const client2 = clients.registry.get('acme.admissions.v1.GateScanCalculationService');
  const reply2 = await call((done) => client2.estimateGateScan(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    details: { contact: { email: (request?.details?.new_owner?.email ?? '') } },
    venue_policy: { block: { reason: (request?.reason ?? '') } },
  };
  const send3 = venuesVenuePolicyValidationClient.previewVenuePolicy.bind(venuesVenuePolicyValidationClient);
  const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.notify_contact?.preferred_channel ?? '') });
  return assemble(
    { outcome: { status: (reply1?.result?.status ?? '') } },
    { outcome: { message: (reply2?.result?.message ?? '') } },
    {
      outcome: { next_steps: (reply3?.outcome?.next_steps ?? []) },
      result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      wristband: { wristband_id: (request?.wristband_id ?? '') },
    },
  );
}

/**
 * Terminates a wristband.
 */
async function terminateWristband(request) {
  validateContext(request.context);
  if (!request.wristband_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
  }
  await store.record(request.context.tenant_id, 'TerminateWristband', request);
  logger.info('terminate_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.details?.effective_at,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.wristband_id ?? ''),
    },
  };
  const spread1 = {
    ...request1,
    context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
  };
  const reply1 = await call((done) => admissionsWristbandReportingClient.countWristbandTotals(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
  const { context: context2, ...body2 } = request;
  const input2 = { ...body2, context: context2 };
  const request2 = {
    context: input2?.context,
    venue_policy: {
      owner: {
        display_name: (input2?.details?.new_owner?.display_name ?? ''),
        email: (input2?.details?.notify_contact?.email ?? ''),
        user_id: (input2?.details?.new_owner?.user_id ?? ''),
      },
    },
  };
  const reply2 = await call((done) => venuesVenuePolicyHistoryClient.compareVenuePolicyVersions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    {
      wristband: { audit: { updated_at: reply2?.venue_policy?.audit?.updated_at } },
    },
    {
      outcome: { effective_at: request?.details?.effective_at },
      wristband: { wristband_id: (request?.wristband_id ?? '') },
    },
  );
}

module.exports = { activateWristband, expireWristband, getWristbandLifecycle, reinstateWristband, renewWristband, scheduleWristbandExpiry, suspendWristband, terminateWristband };
