'use strict';

/**
 * Checks wristbands against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred } = require('../../../platform/rpc');

/**
 * The handlers of WristbandValidationService, over the dependencies the admissions team is given.
 */
function createWristbandValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks a wristband.
   */
  async function checkWristbandEligibility(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'CheckWristbandEligibility', request);
    logger.info('check_wristband_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { phone: (request?.details?.contact?.phone ?? '') } },
      ledger_account: {
        audit: { updated_at: request?.wristband?.audit?.updated_at },
        contact: { email: (request?.wristband?.owner?.email ?? '') },
        holder: { display_name: (request?.wristband?.owner?.display_name ?? '') },
      },
    };
    const client1 = clients.registry.get('acme.ledger.v1.LedgerAccountValidationService');
    const reply1 = await call((done) => client1.validateLedgerAccount(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const send2 = clients.authorizationAccessPolicyQueryClient.resolveAccessPolicies.bind(clients.authorizationAccessPolicyQueryClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        wristband: { audit: { created_at: reply1?.ledger_account?.audit?.created_at } },
      },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { details: (reply2?.result?.details ?? {}) },
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
   * Explains a wristband.
   */
  async function explainWristbandDecision(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'ExplainWristbandDecision', request);
    logger.info('explain_wristband_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          name: request?.details?.contact?.name,
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
      gate_scan: {
        subject: { display_name: (request?.wristband?.owner?.display_name ?? '') },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.admissionsGateScanValidationClient.explainGateScanDecision(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      menu_item: {
        address: {
          recipient: ((input2?.wristband?.locator === 'address' ? input2?.wristband.address : undefined)?.recipient ?? ''),
        },
        audit: { etag: (input2?.wristband?.audit?.etag ?? '') },
        components: { primary: { quantity: (input2?.wristband?.components?.primary?.quantity ?? '0') } },
        owner: { display_name: (reply1?.gate_scan?.subject?.display_name ?? '') },
      },
    };
    const reply2 = await call((done) => clients.concessionsMenuItemCalculationClient.calculateMenuItem(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply1?.outcome?.status ?? '') } },
      { wristband: { owner: reply2?.menu_item?.owner } },
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
  async function getWristbandRules(request) {
    validateContext(request.context);
    if (!request.wristband_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'wristband_id is required');
    }
    await store.record(request.context.tenant_id, 'GetWristbandRules', request);
    logger.info('get_wristband_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { wristband: { wristband_id: (request?.wristband_id ?? '') } };
  }

  /**
   * Lints a wristband.
   */
  async function lintWristbands(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'LintWristbands', request);
    logger.info('lint_wristbands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.wristband?.locator) {
      case 'address':
        selected1_1 = ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.recipient ?? '');
        break;
      default:
        selected1_1 = '';
    }
    let selected1_2;
    switch (request?.wristband?.locator) {
      case 'address':
        selected1_2 = ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.lines ?? []);
        break;
      default:
        selected1_2 = [];
    }
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          lines: selected1_2,
          recipient: selected1_1,
        },
      },
    };
    const reply1 = await call((done) => clients.admissionsAdmissionCalculationClient.recalculateAdmissions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      admission: {
        measurements: [{ unit: (request?.wristband?.dimensions?.unit ?? '') }],
        subject: { kind: (reply1?.admission?.subject?.kind ?? '') },
      },
      context: request?.context,
      details: {
        contact: {
          email: (request?.details?.contact?.email ?? ''),
          phone: (request?.details?.contact?.phone ?? ''),
        },
      },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.admissionsAdmissionCalculationClient.explainAdmissionCalculation(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.wristband?.audit?.updated_at,
        owner_id: (reply2?.result?.correlation_id ?? ''),
        region: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.region ?? ''),
      },
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => clients.sponsorshipsSponsorQueryClient.countSponsors(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      { result: { status: (reply3?.result?.status ?? '') } },
      {
        wristband: {
          name: (request?.wristband?.name ?? ''),
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  /**
   * Normalizes a wristband.
   */
  async function normalizeWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'NormalizeWristband', request);
    logger.info('normalize_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.wristband?.locator) {
      case 'address':
        selected1_1 = ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.country_code ?? '');
        break;
      default:
        selected1_1 = '';
    }
    let selected1_2;
    switch (request?.wristband?.locator) {
      case 'address':
        selected1_2 = ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.lines ?? []);
        break;
      default:
        selected1_2 = [];
    }
    let selected1_3;
    switch (request?.wristband?.locator) {
      case 'address':
        selected1_3 = ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.region ?? '');
        break;
      default:
        selected1_3 = '';
    }
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          country_code: selected1_1,
          lines: selected1_2,
          region: selected1_3,
        },
      },
    };
    const reply1 = await call((done) => clients.admissionsAdmissionCalculationClient.estimateAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        region: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.region ?? ''),
      },
    };
    const task2 = deferred(() => call((done) => clients.sponsorshipsSponsorQueryClient.suggestSponsors(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        outcome: { status: (reply2?.result?.status ?? '') },
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
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
   * Previews a wristband.
   */
  async function previewWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'PreviewWristband', request);
    logger.info('preview_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        subject: { display_name: (request?.wristband?.owner?.display_name ?? '') },
      },
      context: request?.context,
      details: {
        contact: {
          name: request?.details?.contact?.name,
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await bus.request('acme.admissions.v1.WristbandValidationService/PreviewWristband:0', request1);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.outcome?.status ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
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
   * Scores a wristband.
   */
  async function scoreWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'ScoreWristband', request);
    logger.info('score_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.details?.destination?.region ?? '') },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.venuesVenueSectionNotificationClient.listVenueSectionSubscriptions(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: { contact: { email: (request?.wristband?.owner?.email ?? '') } },
      gate_scan: {
        subject: { display_name: (request?.wristband?.owner?.display_name ?? '') },
      },
    };
    const reply2 = await call((done) => clients.admissionsGateScanValidationClient.explainGateScanDecision(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: {
        destination: { locality: (request?.details?.destination?.locality ?? '') },
      },
      wristband: {
        address: {
          country_code: ((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.country_code ?? ''),
          lines: [...((request?.wristband?.locator === 'address' ? request?.wristband.address : undefined)?.lines ?? [])],
        },
      },
    };
    const reply3 = await new Promise((resolve, reject) => {
      clients.admissionsWristbandCalculationClient.calculateWristband(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request4 = {
      context: request?.context,
      wristband_id: (request?.wristband?.wristband_id ?? ''),
    };
    const getWristbandRates4 = promisify(clients.admissionsWristbandCalculationClient.getWristbandRates.bind(clients.admissionsWristbandCalculationClient));
    const reply4 = await getWristbandRates4(request4, metadataFor(request4.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        result: { message: (reply3?.result?.message ?? '') },
        wristband: { capacity: (reply3?.wristband?.capacity ?? '0') },
      },
      { result: { details: (reply4?.result?.details ?? {}) } },
      {
        wristband: {
          name: (request?.wristband?.name ?? ''),
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  /**
   * Validates a wristband.
   */
  async function validateWristband(request) {
    validateContext(request.context);
    if (request.wristband) {
      await store.save(store.toWristbandRecord(request.context.tenant_id, request.wristband));
    }
    await store.record(request.context.tenant_id, 'ValidateWristband', request);
    logger.info('validate_wristband', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      wristband: { dimensions: { length: (request?.wristband?.dimensions?.length ?? 0) } },
    };
    const reply1 = await call((done) => clients.admissionsWristbandCalculationClient.quoteWristband(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
        wristband: { dimensions: { length: (reply1?.wristband?.dimensions?.length ?? 0) } },
      },
      {
        wristband: {
          status: (request?.wristband?.status ?? ''),
          wristband_id: (request?.wristband_id ?? ''),
        },
      },
    );
  }

  return {
    checkWristbandEligibility: unary(checkWristbandEligibility),
    explainWristbandDecision: unary(explainWristbandDecision),
    getWristbandRules: unary(getWristbandRules),
    lintWristbands: unary(lintWristbands),
    normalizeWristband: unary(normalizeWristband),
    previewWristband: unary(previewWristband),
    scoreWristband: unary(scoreWristband),
    validateWristband: unary(validateWristband),
  };
}

module.exports = { createWristbandValidationHandlers };
