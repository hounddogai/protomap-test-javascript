'use strict';

/**
 * Checks admissions against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of AdmissionValidationService, over the dependencies the admissions team is given.
 */
function createAdmissionValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks an admission.
   */
  async function checkAdmissionEligibility(request) {
    validateContext(request.context);
    if (request.admission) {
      await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await store.record(request.context.tenant_id, 'CheckAdmissionEligibility', request);
    logger.info('check_admission_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        subject: {
          gate_scan_id: (request?.admission?.subject?.gate_scan_id ?? ''),
          kind: (request?.admission?.subject?.kind ?? ''),
        },
      },
      context: request?.context,
    };
    const client1 = clients.registry.get('acme.admissions.v1.AdmissionCalculationService');
    const reply1 = await call((done) => client1.compareAdmissionOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      admission: {
        subject: { display_name: (request?.admission?.subject?.display_name ?? '') },
      },
      context: request?.context,
      details: { contact: { phone: (request?.details?.contact?.phone ?? '') } },
    };
    const send2 = clients.admissionsAdmissionCalculationClient.explainAdmissionCalculation.bind(clients.admissionsAdmissionCalculationClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      admission_id: (request?.admission?.admission_id ?? ''),
      context: request?.context,
    };
    const spread3 = {
      ...request3,
      context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
    };
    const reply3 = await call((done) => clients.admissionsAdmissionCalculationClient.getAdmissionRates(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
    const { context: context4, ...body4 } = request;
    const input4 = { ...body4, context: context4 };
    const request4 = {
      annotations: { ...(input4?.annotations ?? {}) },
      context: input4?.context,
      details: { currency_code: (reply1?.breakdown?.currency_code ?? '') },
      menu_item: {
        owner: {
          display_name: (reply2?.admission?.subject?.display_name ?? ''),
          email: (input4?.details?.contact?.email ?? ''),
        },
      },
    };
    const reply4 = await call((done) => clients.concessionsMenuItemCalculationClient.recalculateMenuItems(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        admission: {
          kind: (reply2?.admission?.kind ?? ''),
          sequence: (reply2?.admission?.sequence ?? '0'),
        },
      },
      {
        admission: {
          attributes: (reply3?.admission?.attributes ?? {}),
          subject: reply3?.admission?.subject,
        },
      },
      { outcome: { status: (reply4?.result?.status ?? '') } },
      {
        admission: { admission_id: (request?.admission_id ?? '') },
        result: { correlation_id: (request?.admission?.correlation_id ?? '') },
      },
    );
  }

  /**
   * Explains an admission.
   */
  async function explainAdmissionDecision(request) {
    validateContext(request.context);
    if (request.admission) {
      await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await store.record(request.context.tenant_id, 'ExplainAdmissionDecision', request);
    logger.info('explain_admission_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        kind: (request?.admission?.subject?.kind ?? ''),
        subject: { gate_scan_id: (request?.admission?.subject?.gate_scan_id ?? '') },
      },
      context: request?.context,
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.admissionsAdmissionCalculationClient.calculateAdmission(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        admission: {
          admission_id: (reply1?.admission?.admission_id ?? ''),
          subject: reply1?.admission?.subject,
        },
      },
      {
        admission: {
          attributes: (request?.admission?.attributes ?? {}),
          source: (request?.admission?.source ?? ''),
        },
      },
    );
  }

  /**
   * Returns an admission.
   */
  async function getAdmissionRules(request) {
    validateContext(request.context);
    if (!request.admission_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
    }
    await store.record(request.context.tenant_id, 'GetAdmissionRules', request);
    logger.info('get_admission_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.admission_id ?? '') },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.venuesVenuePolicySyncClient.listVenuePolicyConflicts(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    return assemble(
      {
        admission: { correlation_id: (reply1?.result?.correlation_id ?? '') },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      { admission: { admission_id: (request?.admission_id ?? '') } },
    );
  }

  /**
   * Lints an admission.
   */
  async function lintAdmissions(request) {
    validateContext(request.context);
    if (request.admission) {
      await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await store.record(request.context.tenant_id, 'LintAdmissions', request);
    logger.info('lint_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        contact: {
          email: (request?.details?.contact?.email ?? ''),
          name: request?.details?.contact?.name,
          phone: (request?.details?.contact?.phone ?? ''),
        },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.sponsorshipsSponsorCalculationClient.estimateSponsor(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        admission: {
          admission_id: (request?.admission_id ?? ''),
          subject: { kind: (request?.admission?.kind ?? '') },
        },
      },
    );
  }

  /**
   * Normalizes an admission.
   */
  async function normalizeAdmission(request) {
    validateContext(request.context);
    if (request.admission) {
      await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await store.record(request.context.tenant_id, 'NormalizeAdmission', request);
    logger.info('normalize_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        correlation_id: (request?.admission?.correlation_id ?? ''),
        source: (request?.admission?.source ?? ''),
        subject: { display_name: (request?.admission?.subject?.display_name ?? '') },
      },
      context: request?.context,
    };
    const reply1 = await withRetry(() => call((done) => clients.admissionsAdmissionCalculationClient.simulateAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      admission: {
        subject: { gate_scan_id: (request?.admission?.subject?.gate_scan_id ?? '') },
      },
      context: request?.context,
    };
    const task2 = deferred(() => call((done) => clients.admissionsAdmissionCalculationClient.calculateAdmission(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        admission: {
          kind: (reply1?.admission?.subject?.kind ?? ''),
          subject: { kind: (reply1?.admission?.kind ?? '') },
        },
      },
      { outcome: { message: (reply2?.result?.message ?? '') } },
      {
        admission: {
          admission_id: (request?.admission_id ?? ''),
          sequence: (request?.admission?.sequence ?? '0'),
        },
      },
    );
  }

  /**
   * Previews an admission.
   */
  async function previewAdmission(request) {
    validateContext(request.context);
    if (request.admission) {
      await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await store.record(request.context.tenant_id, 'PreviewAdmission', request);
    logger.info('preview_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        kind: (request?.admission?.kind ?? ''),
        sequence: (request?.admission?.sequence ?? '0'),
        subject: {
          display_name: (request?.admission?.subject?.display_name ?? ''),
          kind: (request?.admission?.subject?.kind ?? ''),
        },
      },
      context: request?.context,
    };
    const reply1 = await bus.request('acme.admissions.v1.AdmissionValidationService/PreviewAdmission:0', request1);
    const request2 = {
      context: request?.context,
      gate_scan: { subject: { kind: (request?.admission?.subject?.kind ?? '') } },
    };
    const reply2 = await runInContext(request.context, () => call((done) => clients.admissionsGateScanCalculationClient.recalculateGateScans(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      admission_id: (request?.admission?.admission_id ?? ''),
      context: request?.context,
    };
    const reply3 = await call((done) => clients.admissionsAdmissionCalculationClient.getAdmissionRates(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        admission: { occurred_at: reply1?.admission?.occurred_at },
        result: { status: (reply1?.result?.status ?? '') },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        admission: { occurred_at: reply3?.admission?.occurred_at },
        result: { status: (reply3?.result?.status ?? '') },
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
   * Scores an admission.
   */
  async function scoreAdmission(request) {
    validateContext(request.context);
    if (request.admission) {
      await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await store.record(request.context.tenant_id, 'ScoreAdmission', request);
    logger.info('score_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.admission?.measurements ?? []).map(async (item) => {
      const request1 = {
        admission: {
          measurements: [{
            metric: (item?.metric ?? ''),
            unit: (item?.unit ?? ''),
            value: (item?.value ?? 0),
          }],
        },
        context: request?.context,
      };
      const reply1 = await call((done) => clients.admissionsAdmissionCalculationClient.recalculateAdmissions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      admission: {
        kind: (reply1?.admission?.subject?.kind ?? ''),
        subject: {
          display_name: (request?.admission?.subject?.display_name ?? ''),
          kind: (request?.admission?.subject?.kind ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: { country_code: (request?.details?.destination?.country_code ?? '') },
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      clients.admissionsAdmissionCalculationClient.compareAdmissionOptions(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request3 = {
      admission_id: (request?.admission?.admission_id ?? ''),
      context: request?.context,
    };
    const getAdmissionRates3 = promisify(clients.admissionsAdmissionCalculationClient.getAdmissionRates.bind(clients.admissionsAdmissionCalculationClient));
    const reply3 = await getAdmissionRates3(request3, metadataFor(request3.context), { deadline: deadline() });
    const settled4 = await Promise.allSettled((request?.admission?.measurements ?? []).map(async (item) => {
      const request4 = {
        admission: {
          measurements: [{
            metric: (item?.metric ?? ''),
            observed_at: item?.observed_at,
            unit: (item?.unit ?? ''),
            value: (item?.value ?? 0),
          }],
        },
        context: request?.context,
      };
      const reply4 = await call((done) => clients.admissionsAdmissionCalculationClient.explainAdmissionCalculation(request4, metadataFor(request4.context), { deadline: deadline() }, done));
      return reply4;
    }));
    const results4 = settled4.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply4 = results4.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { breakdown: { total: reply2?.breakdown?.total } },
      {
        admission: { correlation_id: (reply3?.result?.correlation_id ?? '') },
        result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
      },
      {
        admission: {
          admission_id: (request?.admission_id ?? ''),
          source: (request?.admission?.source ?? ''),
        },
      },
    );
  }

  /**
   * Validates an admission.
   */
  async function validateAdmission(request) {
    validateContext(request.context);
    if (request.admission) {
      await store.save(store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await store.record(request.context.tenant_id, 'ValidateAdmission', request);
    logger.info('validate_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.admission?.occurred_at,
        owner_id: (request?.admission?.subject?.gate_scan_id ?? ''),
      },
    };
    const reply1 = await call((done) => clients.concessionsConcessionOrderQueryClient.countConcessionOrders(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        admission: {
          admission_id: (request?.admission_id ?? ''),
          source: (request?.admission?.source ?? ''),
        },
      },
    );
  }

  return {
    checkAdmissionEligibility: unary(checkAdmissionEligibility),
    explainAdmissionDecision: unary(explainAdmissionDecision),
    getAdmissionRules: unary(getAdmissionRules),
    lintAdmissions: unary(lintAdmissions),
    normalizeAdmission: unary(normalizeAdmission),
    previewAdmission: unary(previewAdmission),
    scoreAdmission: unary(scoreAdmission),
    validateAdmission: unary(validateAdmission),
  };
}

module.exports = { createAdmissionValidationHandlers };
