'use strict';

/**
 * Computes the figures of a gate scan, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of GateScanCalculationService, over the dependencies the admissions team is given.
 */
function createGateScanCalculationHandlers({ store, clients, bus, logger }) {
  /**
   * Calculates a gate scan.
   */
  async function calculateGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'CalculateGateScan', request);
    logger.info('calculate_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        subject: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
      },
      context: request?.context,
      details: {
        destination: { recipient: (request?.details?.destination?.recipient ?? '') },
      },
    };
    const send1 = clients.admissionsAdmissionCalculationClient.calculateAdmission.bind(clients.admissionsAdmissionCalculationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      access_policy: {
        block: { reason: (request?.reason ?? '') },
        owner: {
          display_name: (reply1?.admission?.subject?.display_name ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
      context: request?.context,
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.authorizationAccessPolicyValidationClient.previewAccessPolicy(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      context: input3?.context,
      venue: {
        address: { lines: [...(input3?.details?.destination?.lines ?? [])] },
        audit: {
          etag: (reply2?.access_policy?.audit?.etag ?? ''),
          updated_by: (reply2?.access_policy?.audit?.updated_by ?? ''),
        },
        owner: { display_name: (input3?.gate_scan?.subject?.display_name ?? '') },
      },
    };
    const reply3 = await call((done) => clients.venuesVenueClient.cloneVenue(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.destination?.country_code ?? '') });
    return assemble(
      {
        gate_scan: { subject: { kind: (reply1?.admission?.kind ?? '') } },
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      },
      {
        breakdown: {
          lines: [{
            amount: (reply2?.access_policy?.action === 'discount' ? reply2?.access_policy.discount : undefined)?.amount,
          }],
        },
      },
      { result: { details: (reply3?.result?.details ?? {}) } },
      {
        gate_scan: {
          attributes: (request?.gate_scan?.attributes ?? {}),
          gate_scan_id: (request?.gate_scan_id ?? ''),
        },
      },
    );
  }

  /**
   * Compares a gate scan.
   */
  async function compareGateScanOptions(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'CompareGateScanOptions', request);
    logger.info('compare_gate_scan_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.gate_scan?.occurred_at,
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.sponsorshipsSponsorQueryClient.browseSponsors(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      admission: {
        subject: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
      },
      context: request?.context,
      details: {
        destination: {
          lines: [...(request?.details?.destination?.lines ?? [])],
          locality: (request?.details?.destination?.locality ?? ''),
          region: (request?.details?.destination?.region ?? ''),
        },
      },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.admissionsAdmissionCalculationClient.simulateAdmission(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply2?.admission?.occurred_at,
        owner_id: (request?.gate_scan?.subject?.admission_id ?? ''),
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => clients.broadcastsBroadcastClient.listBroadcasts(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        gate_scan: { correlation_id: (reply1?.result?.correlation_id ?? '') },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        breakdown: { total: reply2?.breakdown?.total },
        result: { message: (reply2?.result?.message ?? '') },
      },
      {
        result: {
          details: (reply3?.result?.details ?? {}),
          status: (reply3?.result?.status ?? ''),
        },
      },
      {
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
        gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') },
      },
    );
  }

  /**
   * Estimates a gate scan.
   */
  async function estimateGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'EstimateGateScan', request);
    logger.info('estimate_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        currency_code: (request?.details?.currency_code ?? ''),
        holder: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.broadcastsStreamKeyValidationClient.lintStreamKeys(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: {
        destination: { postal_code: (request?.details?.destination?.postal_code ?? '') },
      },
      sponsor: {
        contact: {
          preferred_channel: (reply1?.stream_key?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const task2 = deferred(() => call((done) => clients.sponsorshipsSponsorCalculationClient.estimateSponsor(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    const results3 = await Promise.all((request?.gate_scan?.measurements ?? []).map(async (item) => {
      const request3 = {
        admission: {
          measurements: [{
            metric: (item?.metric ?? ''),
            value: (item?.value ?? 0),
          }],
        },
        context: request?.context,
      };
      const reply3 = await call((done) => clients.admissionsAdmissionCalculationClient.simulateAdmission(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        breakdown: { currency_code: (reply1?.stream_key?.currency_code ?? '') },
      },
      {
        gate_scan: {
          subject: {
            display_name: (reply2?.sponsor?.relationship_manager?.display_name ?? ''),
          },
        },
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
  async function explainGateScanCalculation(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'ExplainGateScanCalculation', request);
    logger.info('explain_gate_scan_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        source: (request?.gate_scan?.source ?? ''),
        subject: {
          display_name: (request?.gate_scan?.subject?.display_name ?? ''),
          gate_scan_id: (request?.gate_scan?.gate_scan_id ?? ''),
          kind: (request?.gate_scan?.subject?.kind ?? ''),
        },
      },
      context: request?.context,
    };
    const reply1 = await bus.request('acme.admissions.v1.GateScanCalculationService/ExplainGateScanCalculation:0', request1);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { gate_scan: { kind: (reply1?.admission?.kind ?? '') } },
      {
        gate_scan: {
          correlation_id: (request?.gate_scan?.correlation_id ?? ''),
          gate_scan_id: (request?.gate_scan_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a gate scan.
   */
  async function getGateScanRates(request) {
    validateContext(request.context);
    if (!request.gate_scan_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
    }
    await store.record(request.context.tenant_id, 'GetGateScanRates', request);
    logger.info('get_gate_scan_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
      document: { audit: { updated_at: request?.context?.deadline } },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.documentsDocumentValidationClient.previewDocument(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: {
        destination: {
          country_code: (reply1?.document?.billing_address?.country_code ?? ''),
          lines: [...(reply1?.document?.billing_address?.lines ?? [])],
          locality: (reply1?.document?.billing_address?.locality ?? ''),
          region: (reply1?.document?.billing_address?.region ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.admissionsAdmissionCalculationClient.estimateAdmission(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.gate_scan_id ?? '') });
    return assemble(
      {
        gate_scan: {
          subject: { display_name: (reply1?.document?.counterparty?.display_name ?? '') },
        },
        result: { message: (reply1?.outcome?.message ?? '') },
      },
      {
        gate_scan: {
          gate_scan_id: (reply2?.admission?.subject?.gate_scan_id ?? ''),
          occurred_at: reply2?.admission?.occurred_at,
        },
      },
    );
  }

  /**
   * Quotes a gate scan.
   */
  async function quoteGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'QuoteGateScan', request);
    logger.info('quote_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: {
        primary_address: {
          lines: [...(request?.details?.destination?.lines ?? [])],
          locality: (request?.details?.destination?.locality ?? ''),
          region: (request?.details?.destination?.region ?? ''),
        },
        relationship_manager: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.sponsorshipsSponsorValidationClient.explainSponsorDecision(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      details: {
        recipient: {
          email: (reply1?.sponsor?.relationship_manager?.email ?? ''),
          name: reply1?.sponsor?.contact?.name,
          phone: (reply1?.sponsor?.contact?.phone ?? ''),
          preferred_channel: (reply1?.sponsor?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const unsubscribeVenueUpdates2 = promisify(clients.venuesVenueNotificationClient.unsubscribeVenueUpdates.bind(clients.venuesVenueNotificationClient));
    const reply2 = await unsubscribeVenueUpdates2(request2, metadataFor(request2.context), { deadline: deadline() });
    const request3 = {
      context: request?.context,
      sponsor: {
        primary_address: {
          locality: (request?.details?.destination?.locality ?? ''),
          recipient: (reply1?.sponsor?.primary_address?.recipient ?? ''),
        },
      },
    };
    const reply3 = await call((done) => clients.sponsorshipsSponsorClient.updateSponsor(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          status: (reply2?.outcome?.status ?? ''),
        },
      },
      {
        gate_scan: { correlation_id: (reply3?.result?.correlation_id ?? '') },
        result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
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
   * Recalculates a gate scan.
   */
  async function recalculateGateScans(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'RecalculateGateScans', request);
    logger.info('recalculate_gate_scans', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.gate_scan?.occurred_at,
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const method1 = 'findSearchIndexes';
    const reply1 = await call((done) => clients.searchSearchIndexQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.gate_scan?.occurred_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (request?.gate_scan?.correlation_id ?? ''),
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const client2 = clients.registry.get('acme.venues.v1.VenueService');
    const reply2 = await call((done) => client2.searchVenues(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        gate_scan: {
          gate_scan_id: (request?.gate_scan_id ?? ''),
          occurred_at: request?.gate_scan?.occurred_at,
        },
      },
    );
  }

  /**
   * Simulates a gate scan.
   */
  async function simulateGateScan(request) {
    validateContext(request.context);
    if (request.gate_scan) {
      await store.save(store.toGateScanRecord(request.context.tenant_id, request.gate_scan));
    }
    await store.record(request.context.tenant_id, 'SimulateGateScan', request);
    logger.info('simulate_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        subject: { display_name: (request?.gate_scan?.subject?.display_name ?? '') },
      },
      context: request?.context,
      details: {
        destination: { region: (request?.details?.destination?.region ?? '') },
      },
    };
    const send1 = clients.admissionsAdmissionCalculationClient.compareAdmissionOptions.bind(clients.admissionsAdmissionCalculationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { gate_scan: { source: (reply1?.admission?.source ?? '') } },
      {
        gate_scan: {
          gate_scan_id: (request?.gate_scan_id ?? ''),
          kind: (request?.gate_scan?.kind ?? ''),
        },
      },
    );
  }

  return {
    calculateGateScan: unary(calculateGateScan),
    compareGateScanOptions: unary(compareGateScanOptions),
    estimateGateScan: unary(estimateGateScan),
    explainGateScanCalculation: unary(explainGateScanCalculation),
    getGateScanRates: unary(getGateScanRates),
    quoteGateScan: unary(quoteGateScan),
    recalculateGateScans: unary(recalculateGateScans),
    simulateGateScan: unary(simulateGateScan),
  };
}

module.exports = { createGateScanCalculationHandlers };
