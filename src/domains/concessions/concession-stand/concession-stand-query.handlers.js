'use strict';

/**
 * Answers read-only questions about concession stands, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');
const { assemble, call } = require('../../../platform/rpc');

/**
 * The handlers of ConcessionStandQueryService, over the dependencies the concessions team is given.
 */
function createConcessionStandQueryHandlers({ store, clients, bus, logger }) {
  /**
   * Lists concession stands a page at a time.
   */
  async function browseConcessionStands(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'BrowseConcessionStands', request);
    logger.info('browse_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: {
        audit: { updated_at: request?.filter?.created_after },
        billing_address: { region: (request?.filter?.region ?? '') },
      },
      context: request?.context,
    };
    const reply1 = await bus.request('acme.concessions.v1.ConcessionStandQueryService/BrowseConcessionStands:0', request1);
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      concession_stands: [{ code: (reply1?.concession_order?.counterparty?.code ?? '') }],
    };
  }

  /**
   * Counts concession stands by status.
   */
  async function countConcessionStands(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'CountConcessionStands', request);
    logger.info('count_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { labels: { ...(request?.filter?.labels ?? {}) } },
      page: { page_token: (request?.page?.page_token ?? '') },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.sponsorshipsSponsorClient.listSponsors(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    };
  }

  /**
   * Streams every matching concession stand.
   */
  async function exportConcessionStands(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'ExportConcessionStands', request);
    logger.info('export_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: {
        audit: { created_at: request?.filter?.created_after },
        lines: [{ attributes: { ...(request?.filter?.labels ?? {}) } }],
      },
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
    };
    const reply1 = await call((done) => clients.concessionsConcessionOrderCalculationClient.compareConcessionOrderOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        destination: {
          recipient: (reply1?.concession_order?.billing_address?.recipient ?? ''),
        },
      },
      menu_item: {
        address: {
          lines: [...(reply1?.concession_order?.billing_address?.lines ?? [])],
          locality: (reply1?.concession_order?.billing_address?.locality ?? ''),
          postal_code: (reply1?.concession_order?.billing_address?.postal_code ?? ''),
        },
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      clients.concessionsMenuItemValidationClient.scoreMenuItem(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      {
        concession_stand: {
          code: (reply1?.concession_order?.counterparty?.code ?? ''),
          owner: {
            display_name: (reply1?.concession_order?.counterparty?.display_name ?? ''),
          },
        },
      },
      {
        concession_stand: { audit: { etag: (reply2?.menu_item?.audit?.etag ?? '') } },
        result: { status: (reply2?.result?.status ?? '') },
      },
    );
  }

  /**
   * Lists concession stands a page at a time.
   */
  async function findConcessionStands(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'FindConcessionStands', request);
    logger.info('find_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        address: { region: (request?.filter?.region ?? '') },
        audit: { updated_at: request?.filter?.created_after },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
      context: request?.context,
    };
    const calculateConcessionStand1 = promisify(clients.concessionsConcessionStandCalculationClient.calculateConcessionStand.bind(clients.concessionsConcessionStandCalculationClient));
    const reply1 = await calculateConcessionStand1(request1, metadataFor(request1.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      concession_stands: [{ code: (reply1?.concession_stand?.code ?? '') }],
      result: { message: (reply1?.result?.message ?? '') },
    };
  }

  /**
   * Returns a concession stand.
   */
  async function lookupConcessionStand(request) {
    validateContext(request.context);
    if (!request.concession_stand_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
    }
    await store.record(request.context.tenant_id, 'LookupConcessionStand', request);
    logger.info('lookup_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: {
        audit: { created_at: request?.context?.deadline },
        lines: [{ attributes: { ...(request?.context?.baggage ?? {}) } }],
      },
      context: request?.context,
    };
    const reply1 = await call((done) => clients.concessionsConcessionOrderCalculationClient.calculateConcessionOrder(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_order: {
        bank: {
          bic: ((reply1?.concession_order?.settlement === 'bank' ? reply1?.concession_order.bank : undefined)?.bic ?? ''),
        },
      },
      context: request?.context,
    };
    const method2 = 'quoteConcessionOrder';
    const reply2 = await call((done) => clients.concessionsConcessionOrderCalculationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        concession_stand: { code: (reply2?.concession_order?.counterparty?.code ?? '') },
      },
      {
        concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
      },
    );
  }

  /**
   * Returns a concession stand.
   */
  async function peekConcessionStand(request) {
    validateContext(request.context);
    if (!request.concession_stand_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
    }
    await store.record(request.context.tenant_id, 'PeekConcessionStand', request);
    logger.info('peek_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return {
      concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
    };
  }

  /**
   * Returns several concession stands at once.
   */
  async function resolveConcessionStands(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'ResolveConcessionStands', request);
    logger.info('resolve_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsorship_deal: { counterparty: { email: (request?.context?.actor?.email ?? '') } },
    };
    const client1 = clients.registry.get('acme.sponsorships.v1.SponsorshipDealCalculationService');
    const reply1 = await call((done) => client1.compareSponsorshipDealOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return {
      concession_stands: [{ audit: { created_at: reply1?.sponsorship_deal?.audit?.created_at } }],
    };
  }

  /**
   * Lists concession stands a page at a time.
   */
  async function suggestConcessionStands(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'SuggestConcessionStands', request);
    logger.info('suggest_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
    };
    const send1 = clients.concessionsConcessionOrderCalculationClient.recalculateConcessionOrders.bind(clients.concessionsConcessionOrderCalculationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      concession_stands: [{
        owner: {
          display_name: (reply1?.concession_order?.counterparty?.display_name ?? ''),
        },
      }],
      result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
    };
  }

  /**
   * Summarizes a concession stand.
   */
  async function summarizeConcessionStands(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'SummarizeConcessionStands', request);
    logger.info('summarize_concession_stands', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      concession_order: { lines: [{ attributes: { ...(request?.details?.filters ?? {}) } }] },
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.concessionsConcessionOrderCalculationClient.recalculateConcessionOrders(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      concession_order: {
        billing_address: {
          country_code: (reply1?.concession_order?.billing_address?.country_code ?? ''),
          region: (reply1?.concession_order?.billing_address?.region ?? ''),
        },
        card: {
          holder_name: ((reply1?.concession_order?.settlement === 'card' ? reply1?.concession_order.card : undefined)?.holder_name ?? ''),
        },
      },
      context: input2?.context,
    };
    const reply2 = await call((done) => clients.concessionsConcessionOrderCalculationClient.calculateConcessionOrder(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      venue_policy: {
        audit: {
          created_at: reply2?.concession_order?.audit?.created_at,
          updated_at: reply1?.concession_order?.audit?.updated_at,
          updated_by: (reply2?.concession_order?.audit?.updated_by ?? ''),
        },
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => clients.venuesVenuePolicyHistoryClient.recordVenuePolicyVersion(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      menu_item: {
        address: {
          country_code: (reply2?.concession_order?.billing_address?.country_code ?? ''),
        },
        audit: {
          created_at: reply1?.concession_order?.audit?.created_at,
          etag: (reply2?.concession_order?.audit?.etag ?? ''),
          updated_by: (reply1?.concession_order?.audit?.updated_by ?? ''),
        },
      },
    };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => clients.concessionsMenuItemValidationClient.validateMenuItem(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      { result: { message: (reply3?.outcome?.message ?? '') } },
      { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
      { period: request?.details?.period },
    );
  }

  return {
    browseConcessionStands: unary(browseConcessionStands),
    countConcessionStands: unary(countConcessionStands),
    exportConcessionStands: serverStream(exportConcessionStands),
    findConcessionStands: unary(findConcessionStands),
    lookupConcessionStand: unary(lookupConcessionStand),
    peekConcessionStand: unary(peekConcessionStand),
    resolveConcessionStands: unary(resolveConcessionStands),
    suggestConcessionStands: unary(suggestConcessionStands),
    summarizeConcessionStands: unary(summarizeConcessionStands),
  };
}

module.exports = { createConcessionStandQueryHandlers };
