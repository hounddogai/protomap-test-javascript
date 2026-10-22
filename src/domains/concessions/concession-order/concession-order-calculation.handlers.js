'use strict';

/**
 * Computes the figures of a concession order, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of ConcessionOrderCalculationService, over the dependencies the concessions team is given.
 */
function createConcessionOrderCalculationHandlers({ store, clients, bus, logger }) {
  /**
   * Calculates a concession order.
   */
  async function calculateConcessionOrder(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await store.record(request.context.tenant_id, 'CalculateConcessionOrder', request);
    logger.info('calculate_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: { audit: { created_at: request?.concession_order?.audit?.created_at } },
      context: request?.context,
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.concessionsConcessionStandCalculationClient.calculateConcessionStand(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      details: {
        destination: { postal_code: (input2?.details?.destination?.postal_code ?? '') },
      },
    };
    const reply2 = await call((done) => clients.concessionsConcessionStandCalculationClient.estimateConcessionStand(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      concession_stand: {
        audit: { created_by: (reply2?.concession_stand?.audit?.created_by ?? '') },
        owner: {
          display_name: (request?.concession_order?.counterparty?.display_name ?? ''),
        },
      },
      context: request?.context,
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => clients.concessionsConcessionStandCalculationClient.compareConcessionStandOptions(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_order: {
          billing_address: {
            postal_code: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.postal_code ?? ''),
          },
        },
      },
      {
        concession_order: {
          counterparty: { display_name: (reply2?.concession_stand?.owner?.display_name ?? '') },
        },
      },
      {
        breakdown: { currency_code: (reply3?.breakdown?.currency_code ?? '') },
      },
      {
        breakdown: { total: request?.concession_order?.total },
        concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      },
    );
  }

  /**
   * Compares a concession order.
   */
  async function compareConcessionOrderOptions(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await store.record(request.context.tenant_id, 'CompareConcessionOrderOptions', request);
    logger.info('compare_concession_order_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        address: {
          lines: [...(request?.details?.destination?.lines ?? [])],
          recipient: (request?.concession_order?.billing_address?.recipient ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: {
          locality: (request?.concession_order?.billing_address?.locality ?? ''),
        },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.concessionsConcessionStandCalculationClient.estimateConcessionStand(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      replay_clip: {
        audit: { created_by: (request?.concession_order?.audit?.created_by ?? '') },
        dimensions: { width: (reply1?.concession_stand?.dimensions?.width ?? 0) },
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.broadcastsReplayClipCalculationClient.explainReplayClipCalculation(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        breakdown: { total: reply1?.breakdown?.total },
        concession_order: {
          billing_address: {
            recipient: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.recipient ?? ''),
          },
        },
      },
      {
        concession_order: { audit: { created_by: (reply2?.replay_clip?.audit?.created_by ?? '') } },
        result: { status: (reply2?.result?.status ?? '') },
      },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          total: request?.concession_order?.total,
        },
      },
    );
  }

  /**
   * Estimates a concession order.
   */
  async function estimateConcessionOrder(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await store.record(request.context.tenant_id, 'EstimateConcessionOrder', request);
    logger.info('estimate_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        owner: {
          display_name: (request?.concession_order?.counterparty?.display_name ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.concession_order?.billing_address?.country_code ?? ''),
          recipient: (request?.details?.destination?.recipient ?? ''),
        },
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.concessionsConcessionStandCalculationClient.simulateConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      stream_key: {
        audit: { updated_by: (request?.concession_order?.audit?.updated_by ?? '') },
      },
    };
    const task2 = deferred(() => call((done) => clients.broadcastsStreamKeyCalculationClient.explainStreamKeyCalculation(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_order: {
          lines: [{
            description: (reply1?.concession_stand?.components?.primary?.description ?? ''),
          }],
        },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        concession_order: {
          audit: {
            etag: (reply2?.stream_key?.audit?.etag ?? ''),
            updated_by: (reply2?.stream_key?.audit?.updated_by ?? ''),
          },
        },
      },
      {
        breakdown: { total: request?.concession_order?.total },
        concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      },
    );
  }

  /**
   * Returns a concession order.
   */
  async function getConcessionOrderRates(request) {
    validateContext(request.context);
    if (!request.concession_order_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_order_id is required');
    }
    await store.record(request.context.tenant_id, 'GetConcessionOrderRates', request);
    logger.info('get_concession_order_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: { owner: { email: (request?.context?.actor?.email ?? '') } },
    };
    const reply1 = await bus.request('acme.concessions.v1.ConcessionOrderCalculationService/GetConcessionOrderRates:0', request1);
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
    const reply2 = await call((done) => clients.sponsorshipsSponsorClient.listSponsors(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      {
        concession_order: {
          billing_address: {
            postal_code: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.postal_code ?? ''),
          },
        },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        result: {
          message: (reply2?.result?.message ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      {
        concession_order: { concession_order_id: (request?.concession_order_id ?? '') },
      },
    );
  }

  /**
   * Quotes a concession order.
   */
  async function quoteConcessionOrder(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await store.record(request.context.tenant_id, 'QuoteConcessionOrder', request);
    logger.info('quote_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      search_index: {
        address: { lines: [...(request?.concession_order?.billing_address?.lines ?? [])] },
      },
    };
    const reply1 = await call((done) => clients.searchSearchIndexValidationClient.normalizeSearchIndex(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_order: {
          audit: { etag: (reply1?.search_index?.audit?.etag ?? '') },
          lines: [{
            description: (reply1?.search_index?.components?.primary?.description ?? ''),
          }],
        },
      },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          due_at: request?.concession_order?.due_at,
        },
      },
    );
  }

  /**
   * Recalculates a concession order.
   */
  async function recalculateConcessionOrders(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await store.record(request.context.tenant_id, 'RecalculateConcessionOrders', request);
    logger.info('recalculate_concession_orders', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        address: {
          lines: [...(request?.details?.destination?.lines ?? [])],
          locality: (request?.concession_order?.billing_address?.locality ?? ''),
          postal_code: (request?.details?.destination?.postal_code ?? ''),
          recipient: (request?.concession_order?.billing_address?.recipient ?? ''),
        },
      },
      context: request?.context,
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.concessionsConcessionStandCalculationClient.explainConcessionStandCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const results2 = [];
    for (const item of (request?.concession_order?.attachments ?? [])) {
      const request2 = {
        context: request?.context,
        document: {
          attachments: [{
            content_type: (item?.content_type ?? ''),
            size_bytes: (item?.size_bytes ?? '0'),
            uri: (item?.uri ?? ''),
          }],
        },
      };
      const reply2 = await call((done) => clients.documentsDocumentValidationClient.normalizeDocument(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      results2.push(reply2);
    }
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_order: {
          billing_address: {
            lines: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.lines ?? []),
          },
        },
        result: { message: (reply1?.outcome?.message ?? '') },
      },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          notes: (request?.concession_order?.notes ?? ''),
        },
      },
    );
  }

  /**
   * Simulates a concession order.
   */
  async function simulateConcessionOrder(request) {
    validateContext(request.context);
    if (request.concession_order) {
      await store.save(store.toConcessionOrderRecord(request.context.tenant_id, request.concession_order));
    }
    await store.record(request.context.tenant_id, 'SimulateConcessionOrder', request);
    logger.info('simulate_concession_order', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.details?.destination?.country_code ?? ''),
          postal_code: (request?.concession_order?.billing_address?.postal_code ?? ''),
          recipient: (request?.details?.destination?.recipient ?? ''),
        },
      },
      stream_key: {
        holder: {
          display_name: (request?.concession_order?.counterparty?.display_name ?? ''),
        },
      },
    };
    const compareStreamKeyOptions1 = promisify(clients.broadcastsStreamKeyCalculationClient.compareStreamKeyOptions.bind(clients.broadcastsStreamKeyCalculationClient));
    const reply1 = await compareStreamKeyOptions1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      concession_stand: {
        address: {
          locality: (request?.details?.destination?.locality ?? ''),
          postal_code: (request?.concession_order?.billing_address?.postal_code ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: {
          recipient: (request?.concession_order?.billing_address?.recipient ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.concessionsConcessionStandCalculationClient.calculateConcessionStand(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      sponsor: {
        primary_address: {
          country_code: ((reply2?.concession_stand?.locator === 'address' ? reply2?.concession_stand.address : undefined)?.country_code ?? ''),
          lines: [...(request?.details?.destination?.lines ?? [])],
        },
      },
    };
    const method3 = 'quoteSponsor';
    const reply3 = await call((done) => clients.sponsorshipsSponsorCalculationClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        concession_order: {
          counterparty: { display_name: (reply2?.concession_stand?.owner?.display_name ?? '') },
        },
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
      },
      {
        concession_order: {
          counterparty: {
            display_name: (reply3?.sponsor?.relationship_manager?.display_name ?? ''),
          },
        },
      },
      {
        concession_order: {
          concession_order_id: (request?.concession_order_id ?? ''),
          status: (request?.concession_order?.status ?? ''),
        },
      },
    );
  }

  return {
    calculateConcessionOrder: unary(calculateConcessionOrder),
    compareConcessionOrderOptions: unary(compareConcessionOrderOptions),
    estimateConcessionOrder: unary(estimateConcessionOrder),
    getConcessionOrderRates: unary(getConcessionOrderRates),
    quoteConcessionOrder: unary(quoteConcessionOrder),
    recalculateConcessionOrders: unary(recalculateConcessionOrders),
    simulateConcessionOrder: unary(simulateConcessionOrder),
  };
}

module.exports = { createConcessionOrderCalculationHandlers };
