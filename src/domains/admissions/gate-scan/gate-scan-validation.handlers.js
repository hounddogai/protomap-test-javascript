'use strict';

/**
 * Checks gate scans against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of GateScanValidationService, over the dependencies the admissions team is given.
 */
function createGateScanValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks a gate scan.
   */
  async function checkGateScanEligibility(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'CheckGateScanEligibility', request);
    logger.info('check_gate_scan_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: { subject: { kind: (request?.gate_scan?.subject?.kind ?? '') } },
      context: request?.context,
      details: {
        contact: {
          name: request?.details?.contact?.name,
          phone: (request?.details?.contact?.phone ?? ''),
        },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.admissionsAdmissionValidationClient.validateAdmission(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      admission_id: (request?.gate_scan?.subject?.admission_id ?? ''),
      context: request?.context,
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.admissionsAdmissionValidationClient.getAdmissionRules(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    const request3 = {
      admission: {
        subject: { gate_scan_id: (reply1?.admission?.subject?.gate_scan_id ?? '') },
      },
      context: request?.context,
      details: {
        contact: {
          name: request?.details?.contact?.name,
          phone: (request?.details?.contact?.phone ?? ''),
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply3 = await withRetry(() => call((done) => clients.admissionsAdmissionValidationClient.explainAdmissionDecision(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const request4 = {
      context: request?.context,
      wristband: {
        owner: { display_name: (reply1?.admission?.subject?.display_name ?? '') },
      },
    };
    const task4 = deferred(() => call((done) => clients.admissionsWristbandCalculationClient.recalculateWristbands(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    const reply4 = await task4();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        gate_scan: { kind: (reply1?.admission?.kind ?? '') },
        outcome: { next_steps: (reply1?.outcome?.next_steps ?? []) },
      },
      {
        gate_scan: { subject: { admission_id: (reply2?.admission?.admission_id ?? '') } },
        outcome: { message: (reply2?.result?.message ?? '') },
      },
      {
        gate_scan: { sequence: (reply3?.admission?.sequence ?? '0') },
        result: { status: (reply3?.result?.status ?? '') },
      },
      {
        gate_scan: { measurements: [{ unit: (reply4?.wristband?.dimensions?.unit ?? '') }] },
        result: { message: (reply4?.result?.message ?? '') },
      },
      {
        gate_scan: {
          gate_scan_id: (request?.gate_scan_id ?? ''),
          occurred_at: request?.gate_scan?.occurred_at,
        },
      },
    );
  }

  /**
   * Explains a gate scan.
   */
  async function explainGateScanDecision(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'ExplainGateScanDecision', request);
    logger.info('explain_gate_scan_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          email: (request?.details?.contact?.email ?? ''),
          name: request?.details?.contact?.name,
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
      sponsorship_deal: {
        lines: [{ attributes: { ...(request?.gate_scan?.attributes ?? {}) } }],
      },
    };
    const reply1 = await bus.request('acme.admissions.v1.GateScanValidationService/ExplainGateScanDecision:0', request1);
    const settled2 = await Promise.allSettled((request?.gate_scan?.measurements ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        replay_clip: {
          dimensions: { unit: (item?.unit ?? '') },
          owner: {
            display_name: (reply1?.sponsorship_deal?.counterparty?.display_name ?? ''),
          },
        },
      };
      const reply2 = await call((done) => clients.broadcastsReplayClipValidationClient.checkReplayClipEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { next_steps: (reply1?.outcome?.next_steps ?? []) },
        result: { status: (reply1?.outcome?.status ?? '') },
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
   * Returns a gate scan.
   */
  async function getGateScanRules(request) {
    validateContext(request.context);
    if (!request.gate_scan_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
    }
    await store.record(request.context.tenant_id, 'GetGateScanRules', request);
    logger.info('get_gate_scan_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') } };
  }

  /**
   * Lints a gate scan.
   */
  async function lintGateScans(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'LintGateScans', request);
    logger.info('lint_gate_scans', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.gate_scan?.measurements ?? [])) {
      const request1 = {
        context: request?.context,
        venue: { dimensions: { unit: (item?.unit ?? '') } },
      };
      const reply1 = await call((done) => clients.venuesVenueValidationClient.scoreVenue(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      details: {
        destination: {
          country_code: ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.country_code ?? ''),
          locality: ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.locality ?? ''),
        },
      },
      sponsorship_deal: {
        billing_address: {
          lines: [...((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.lines ?? [])],
        },
      },
    };
    const reply2 = await runInContext(request.context, () => call((done) => clients.sponsorshipsSponsorshipDealValidationClient.scoreSponsorshipDeal(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { gate_scan: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        gate_scan: {
          gate_scan_id: (request?.gate_scan_id ?? ''),
          kind: (request?.gate_scan?.kind ?? ''),
        },
      },
    );
  }

  /**
   * Normalizes a gate scan.
   */
  async function normalizeGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'NormalizeGateScan', request);
    logger.info('normalize_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
      search_index: {
        owner: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
      },
    };
    const reply1 = await call((done) => clients.searchSearchIndexValidationClient.previewSearchIndex(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          status: (reply1?.result?.status ?? ''),
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

  /**
   * Previews a gate scan.
   */
  async function previewGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'PreviewGateScan', request);
    logger.info('preview_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.gate_scan?.measurements ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        wristband: {
          audit: { created_at: item?.observed_at },
          dimensions: { unit: (item?.unit ?? '') },
        },
      };
      const reply1 = await call((done) => clients.admissionsWristbandCalculationClient.recalculateWristbands(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      admission_id: (request?.gate_scan?.subject?.admission_id ?? ''),
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
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        gate_scan: {
          gate_scan_id: (request?.gate_scan_id ?? ''),
          kind: (request?.gate_scan?.kind ?? ''),
        },
      },
    );
  }

  /**
   * Scores a gate scan.
   */
  async function scoreGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'ScoreGateScan', request);
    logger.info('score_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.gate_scan?.measurements ?? []).map(async (item) => {
      const request1 = {
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
      const reply1 = await call((done) => clients.admissionsAdmissionValidationClient.validateAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      gate_scan: {
        attributes: (request?.gate_scan?.attributes ?? {}),
        gate_scan_id: (request?.gate_scan_id ?? ''),
      },
    };
  }

  /**
   * Validates a gate scan.
   */
  async function validateGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'ValidateGateScan', request);
    logger.info('validate_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      gate_scan: {
        sequence: (request?.gate_scan?.sequence ?? '0'),
        source: (request?.gate_scan?.source ?? ''),
        subject: {
          display_name: (request?.gate_scan?.subject?.display_name ?? ''),
          kind: (request?.gate_scan?.kind ?? ''),
        },
      },
    };
    const estimateGateScan1 = promisify(clients.admissionsGateScanCalculationClient.estimateGateScan.bind(clients.admissionsGateScanCalculationClient));
    const reply1 = await estimateGateScan1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.gate_scan?.occurred_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (reply1?.gate_scan?.subject?.admission_id ?? ''),
      },
    };
    const reply2 = await call((done) => clients.sponsorshipsSponsorshipDealQueryClient.browseSponsorshipDeals(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { gate_scan: { subject: { kind: (reply1?.gate_scan?.kind ?? '') } } },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        gate_scan: {
          correlation_id: (request?.gate_scan?.correlation_id ?? ''),
          gate_scan_id: (request?.gate_scan_id ?? ''),
        },
      },
    );
  }

  return {
    checkGateScanEligibility: unary(checkGateScanEligibility),
    explainGateScanDecision: unary(explainGateScanDecision),
    getGateScanRules: unary(getGateScanRules),
    lintGateScans: unary(lintGateScans),
    normalizeGateScan: unary(normalizeGateScan),
    previewGateScan: unary(previewGateScan),
    scoreGateScan: unary(scoreGateScan),
    validateGateScan: unary(validateGateScan),
  };
}

module.exports = { createGateScanValidationHandlers };
