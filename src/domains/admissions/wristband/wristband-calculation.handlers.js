'use strict';

/**
 * Computes the figures of a wristband, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred } = require('../../../platform/rpc');

/**
 * The handlers of WristbandCalculationService, over the dependencies the admissions team is given.
 */
function createWristbandCalculationHandlers({ store, clients, bus, logger }) {
  /**
   * Calculates a wristband.
   */
  async function calculateWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'CalculateWristband', request);
    logger.info('calculate_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        audit: { updated_by: (request?.wristband?.audit?.updated_by ?? '') },
        primary_address: {
          lines: [...((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.lines ?? [])],
        },
      },
    };
    const task1 = deferred(() => call((done) => clients.sponsorshipsSponsorCalculationClient.simulateSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.wristband?.audit?.updated_at,
        region: (reply1?.sponsor?.primary_address?.region ?? ''),
      },
    };
    const reply2 = await bus.request('acme.admissions.v1.WristbandCalculationService/CalculateWristband:1', request2);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        wristband: { audit: { created_by: (reply1?.sponsor?.audit?.created_by ?? '') } },
      },
      { result: { details: (reply2?.result?.details ?? {}) } },
      {
        wristband: {
          status: (request?.wristband?.status ?? ''),
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  /**
   * Compares a wristband.
   */
  async function compareWristbandOptions(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'CompareWristbandOptions', request);
    logger.info('compare_wristband_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        subject: { display_name: (request?.wristband?.owner?.display_name ?? '') },
      },
      context: request?.context,
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.admissionsAdmissionValidationClient.explainAdmissionDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = { context: request?.context };
    const reply2 = await call((done) => clients.venuesVenueSectionQueryClient.resolveVenueSections(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.outcome?.effective_at,
        owner_id: (reply1?.result?.correlation_id ?? ''),
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const reply3 = await new Promise((resolve, reject) => {
      clients.concessionsConcessionOrderQueryClient.countConcessionOrders(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.admission?.correlation_id ?? ''),
          details: (reply1?.result?.details ?? {}),
        },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        result: {
          correlation_id: (reply3?.result?.correlation_id ?? ''),
          message: (reply3?.result?.message ?? ''),
        },
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
   * Estimates a wristband.
   */
  async function estimateWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'EstimateWristband', request);
    logger.info('estimate_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        measurements: [{ unit: (request?.wristband?.dimensions?.unit ?? '') }],
        subject: { display_name: (request?.wristband?.owner?.display_name ?? '') },
      },
      context: request?.context,
      details: { contact: { email: (request?.wristband?.owner?.email ?? '') } },
    };
    const explainAdmissionCalculation1 = promisify(clients.admissionsAdmissionCalculationClient.explainAdmissionCalculation.bind(clients.admissionsAdmissionCalculationClient));
    const reply1 = await explainAdmissionCalculation1(request1, metadataFor(request1.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        wristband: {
          name: (request?.wristband?.name ?? ''),
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a wristband.
   */
  async function getWristbandRates(request) {
    validateContext(request.context);
    if (!request.wristband_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
    }
    await store.record(request.context.tenant_id, 'GetWristbandRates', request);
    logger.info('get_wristband_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        attributes: { ...(request?.context?.baggage ?? {}) },
        correlation_id: (request?.context?.trace_id ?? ''),
        occurred_at: request?.context?.deadline,
        source: (request?.wristband_id ?? ''),
      },
      context: request?.context,
    };
    const reply1 = await call((done) => clients.admissionsAdmissionValidationClient.scoreAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: {
          correlation_id: (reply1?.admission?.correlation_id ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      { wristband: { wristband_id: (request?.wristband_id ?? '') } },
    );
  }

  /**
   * Quotes a wristband.
   */
  async function quoteWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'QuoteWristband', request);
    logger.info('quote_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.wristband?.audit?.created_at,
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const method1 = 'listVenuePolicyExports';
    const reply1 = await call((done) => clients.venuesVenuePolicyExportClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      admission: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        subject: { display_name: (request?.wristband?.owner?.display_name ?? '') },
      },
      context: request?.context,
      details: { contact: { email: (request?.wristband?.owner?.email ?? '') } },
    };
    const client2 = clients.registry.get('acme.admissions.v1.AdmissionValidationService');
    const reply2 = await call((done) => client2.previewAdmission(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          reference_ids: (reply1?.result?.reference_ids ?? []),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { result: { message: (reply2?.outcome?.message ?? '') } },
      {
        wristband: {
          owner: request?.wristband?.owner,
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  /**
   * Recalculates a wristband.
   */
  async function recalculateWristbands(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'RecalculateWristbands', request);
    logger.info('recalculate_wristbands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.wristband?.owner?.email ?? '') } },
    };
    const send1 = clients.auditAuditEventValidationClient.previewAuditEvent.bind(clients.auditAuditEventValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      admission: { subject: { kind: (reply1?.audit_event?.kind ?? '') } },
      context: request?.context,
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.admissionsAdmissionValidationClient.normalizeAdmission(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      admission: {
        admission_id: (reply2?.admission?.admission_id ?? ''),
        subject: {
          display_name: (reply2?.admission?.subject?.display_name ?? ''),
          gate_scan_id: (reply2?.admission?.subject?.gate_scan_id ?? ''),
          kind: (reply2?.admission?.subject?.kind ?? ''),
        },
      },
      context: input3?.context,
    };
    const reply3 = await call((done) => clients.admissionsAdmissionValidationClient.validateAdmission(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      admission: {
        measurements: [{ unit: (request?.wristband?.dimensions?.unit ?? '') }],
        occurred_at: reply1?.audit_event?.occurred_at,
        subject: { gate_scan_id: (reply2?.admission?.subject?.gate_scan_id ?? '') },
      },
      context: request?.context,
    };
    const cloned4 = JSON.parse(JSON.stringify(request4));
    const reply4 = await call((done) => clients.admissionsAdmissionValidationClient.checkAdmissionEligibility(cloned4, metadataFor(cloned4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        result: {
          correlation_id: (reply2?.admission?.correlation_id ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      { result: { correlation_id: (reply3?.admission?.correlation_id ?? '') } },
      {
        result: { message: (reply4?.result?.message ?? '') },
        wristband: {
          owner: { display_name: (reply4?.admission?.subject?.display_name ?? '') },
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

  return {
    calculateWristband: unary(calculateWristband),
    compareWristbandOptions: unary(compareWristbandOptions),
    estimateWristband: unary(estimateWristband),
    getWristbandRates: unary(getWristbandRates),
    quoteWristband: unary(quoteWristband),
    recalculateWristbands: unary(recalculateWristbands),
  };
}

module.exports = { createWristbandCalculationHandlers };
