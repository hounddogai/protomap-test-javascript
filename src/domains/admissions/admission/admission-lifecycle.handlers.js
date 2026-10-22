'use strict';

/**
 * Moves an admission between the states of its lifecycle.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of AdmissionLifecycleService, over the dependencies the admissions team is given.
 */
function createAdmissionLifecycleHandlers({ store, clients, bus, logger }) {
  /**
   * Activates an admission.
   */
  async function activateAdmission(request) {
    validateContext(request.context);
    if (!request.admission_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
    }
    await store.record(request.context.tenant_id, 'ActivateAdmission', request);
    logger.info('activate_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await call((done) => clients.admissionsGateScanValidationClient.explainGateScanDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      admission_id: (request?.admission_id ?? ''),
      context: request?.context,
    };
    const reply2 = await new Promise((resolve, reject) => {
      clients.admissionsAdmissionValidationClient.getAdmissionRules(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.outcome?.message ?? '') } },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      {
        admission: { admission_id: (request?.admission_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Expires an admission.
   */
  async function expireAdmission(request) {
    validateContext(request.context);
    if (!request.admission_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
    }
    await store.record(request.context.tenant_id, 'ExpireAdmission', request);
    logger.info('expire_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        subject: { display_name: (request?.details?.new_owner?.display_name ?? '') },
      },
      context: request?.context,
    };
    const createAdmission1 = promisify(clients.admissionsAdmissionClient.createAdmission.bind(clients.admissionsAdmissionClient));
    const reply1 = await createAdmission1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      admission: {
        kind: (reply1?.admission?.subject?.kind ?? ''),
        subject: { gate_scan_id: (reply1?.admission?.subject?.gate_scan_id ?? '') },
      },
      context: request?.context,
      details: { contact: { email: (request?.details?.notify_contact?.email ?? '') } },
    };
    const reply2 = await call((done) => clients.admissionsAdmissionValidationClient.checkAdmissionEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { admission: { occurred_at: reply1?.admission?.occurred_at } },
      { admission: { source: (reply2?.admission?.source ?? '') } },
      {
        admission: { admission_id: (request?.admission_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Returns an admission.
   */
  async function getAdmissionLifecycle(request) {
    validateContext(request.context);
    if (!request.admission_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
    }
    await store.record(request.context.tenant_id, 'GetAdmissionLifecycle', request);
    logger.info('get_admission_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      metric_series: { owner: { user_id: (request?.context?.actor?.user_id ?? '') } },
    };
    const method1 = 'lintMetricSerieses';
    const reply1 = await call((done) => clients.metricsMetricSeriesValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      admission_id: (request?.admission_id ?? ''),
      context: request?.context,
    };
    const client2 = clients.registry.get('acme.admissions.v1.AdmissionCalculationService');
    const reply2 = await call((done) => client2.getAdmissionRates(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      { admission: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        admission: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          kind: (reply2?.admission?.subject?.kind ?? ''),
        },
      },
      { admission: { admission_id: (request?.admission_id ?? '') } },
    );
  }

  /**
   * Reinstates an admission.
   */
  async function reinstateAdmission(request) {
    validateContext(request.context);
    if (!request.admission_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
    }
    await store.record(request.context.tenant_id, 'ReinstateAdmission', request);
    logger.info('reinstate_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission_id: (request?.admission_id ?? ''),
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const send1 = clients.admissionsAdmissionClient.deleteAdmission.bind(clients.admissionsAdmissionClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      subscriber: { email: (request?.details?.new_owner?.email ?? '') },
    };
    const messages2 = await collect(clients.metricsMetricSeriesClient.watchMetricSerieses(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      details: { contact: { name: request?.details?.notify_contact?.name } },
    };
    const spread3 = {
      ...request3,
      context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
    };
    const reply3 = await call((done) => clients.admissionsAdmissionCalculationClient.explainAdmissionCalculation(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
    const { context: context4, ...body4 } = request;
    const input4 = { ...body4, context: context4 };
    const request4 = {
      admission: {
        correlation_id: (reply3?.admission?.correlation_id ?? ''),
        kind: (reply3?.admission?.subject?.kind ?? ''),
        subject: {
          display_name: (reply3?.admission?.subject?.display_name ?? ''),
          gate_scan_id: (reply3?.admission?.subject?.gate_scan_id ?? ''),
        },
      },
      context: input4?.context,
    };
    const reply4 = await call((done) => clients.admissionsAdmissionCalculationClient.simulateAdmission(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.notify_contact?.preferred_channel ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        admission: {
          subject: {
            gate_scan_id: (reply3?.admission?.subject?.gate_scan_id ?? ''),
            kind: (reply3?.admission?.kind ?? ''),
          },
        },
      },
      {
        outcome: { message: (reply4?.result?.message ?? '') },
        result: { correlation_id: (reply4?.admission?.correlation_id ?? '') },
      },
      {
        admission: { admission_id: (request?.admission_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Schedules an admission.
   */
  async function scheduleAdmissionExpiry(request) {
    validateContext(request.context);
    if (request.admission) {
      await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await store.record(request.context.tenant_id, 'ScheduleAdmissionExpiry', request);
    logger.info('schedule_admission_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          email: (request?.details?.notify?.email ?? ''),
          name: request?.details?.notify?.name,
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
      stream_key: {
        holder: { display_name: (request?.admission?.subject?.display_name ?? '') },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.broadcastsStreamKeyWorkflowClient.retryStreamKeyWorkflow(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.authorizationAccessPolicyClient.batchGetAccessPolicies(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.outcome?.message ?? '') },
        result: { status: (reply1?.outcome?.status ?? '') },
      },
      {
        admission: { correlation_id: (reply2?.result?.correlation_id ?? '') },
        result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
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
   * Suspends an admission.
   */
  async function suspendAdmission(request) {
    validateContext(request.context);
    if (!request.admission_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
    }
    await store.record(request.context.tenant_id, 'SuspendAdmission', request);
    logger.info('suspend_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission_id: (request?.admission_id ?? ''),
      context: request?.context,
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.admissionsAdmissionValidationClient.getAdmissionRules(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      gate_scan_id: (reply1?.admission?.subject?.gate_scan_id ?? ''),
    };
    const reply2 = await withRetry(() => call((done) => clients.admissionsGateScanQueryClient.summarizeGateScans(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        admission: { admission_id: (request?.admission_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Terminates an admission.
   */
  async function terminateAdmission(request) {
    validateContext(request.context);
    if (!request.admission_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
    }
    await store.record(request.context.tenant_id, 'TerminateAdmission', request);
    logger.info('terminate_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        subject: { display_name: (request?.details?.new_owner?.display_name ?? '') },
      },
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
    };
    const task1 = deferred(() => call((done) => clients.admissionsAdmissionCalculationClient.compareAdmissionOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        labels: { ...(reply1?.admission?.attributes ?? {}) },
        owner_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    };
    const reply2 = await bus.request('acme.admissions.v1.AdmissionLifecycleService/TerminateAdmission:1', request2);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        admission: { subject: { kind: (reply1?.admission?.subject?.kind ?? '') } },
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          message: (reply2?.result?.message ?? ''),
        },
      },
      {
        admission: { admission_id: (request?.admission_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  return {
    activateAdmission: unary(activateAdmission),
    expireAdmission: unary(expireAdmission),
    getAdmissionLifecycle: unary(getAdmissionLifecycle),
    reinstateAdmission: unary(reinstateAdmission),
    scheduleAdmissionExpiry: unary(scheduleAdmissionExpiry),
    suspendAdmission: unary(suspendAdmission),
    terminateAdmission: unary(terminateAdmission),
  };
}

module.exports = { createAdmissionLifecycleHandlers };
