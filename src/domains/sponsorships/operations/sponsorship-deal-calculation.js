'use strict';

/**
 * Computes the figures of a sponsorship deal, such as totals, taxes, and estimates. Each operation validates,
 * records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const clients = require('../../../platform/clients');
const {
  sponsorshipsSponsorCalculationClient,
  venuesVenueQueryClient,
  venuesVenueSectionHistoryClient,
  venuesVenueSectionSyncClient,
} = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Calculates a sponsorship deal.
 */
async function calculateSponsorshipDeal(request) {
  validateContext(request.context);
  if (request.sponsorship_deal) {
    await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
  }
  await store.record(request.context.tenant_id, 'CalculateSponsorshipDeal', request);
  logger.info('calculate_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const results1 = [];
  for (const item of (request?.sponsorship_deal?.lines ?? [])) {
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.sponsorship_deal?.billing_address?.country_code ?? ''),
        },
      },
      sponsor: { attributes: { ...(item?.attributes ?? {}) } },
    };
    const reply1 = await call((done) => sponsorshipsSponsorCalculationClient.compareSponsorOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    results1.push(reply1);
  }
  const reply1 = results1.at(-1) ?? {};
  const request2 = {
    context: request?.context,
    sponsor: {
      relationship_manager: {
        display_name: (request?.sponsorship_deal?.counterparty?.display_name ?? ''),
        user_id: (reply1?.sponsor?.relationship_manager?.user_id ?? ''),
      },
    },
  };
  const reply2 = await call((done) => clients.sponsorshipsSponsorCalculationClient.quoteSponsor(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    sponsor: {
      national_id: {
        number: ((reply2?.sponsor?.identity_document === 'passport' ? reply2?.sponsor.passport : undefined)?.number ?? ''),
      },
      primary_address: {
        recipient: (request?.sponsorship_deal?.billing_address?.recipient ?? ''),
        region: (request?.details?.destination?.region ?? ''),
      },
    },
  };
  const reply3 = await new Promise((resolve, reject) => {
    sponsorshipsSponsorCalculationClient.explainSponsorCalculation(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    {
      sponsorship_deal: {
        billing_address: { lines: (reply3?.sponsor?.primary_address?.lines ?? []) },
      },
    },
    {
      sponsorship_deal: {
        metadata: (request?.sponsorship_deal?.metadata ?? {}),
        sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
      },
    },
  );
}

/**
 * Compares a sponsorship deal.
 */
async function compareSponsorshipDealOptions(request) {
  validateContext(request.context);
  if (request.sponsorship_deal) {
    await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
  }
  await store.record(request.context.tenant_id, 'CompareSponsorshipDealOptions', request);
  logger.info('compare_sponsorship_deal_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_section: {
      children: [{
        children: [{
          location: {
            country_code: (request?.details?.destination?.country_code ?? ''),
            lines: [...(request?.details?.destination?.lines ?? [])],
            region: (request?.details?.destination?.region ?? ''),
          },
        }],
      }],
    },
  };
  const pushVenueSection1 = promisify(venuesVenueSectionSyncClient.pushVenueSection.bind(venuesVenueSectionSyncClient));
  const reply1 = await pushVenueSection1(request1, metadataFor(request1.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { status: (reply1?.result?.status ?? '') } },
    {
      sponsorship_deal: {
        due_at: request?.sponsorship_deal?.due_at,
        sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
      },
    },
  );
}

/**
 * Estimates a sponsorship deal.
 */
async function estimateSponsorshipDeal(request) {
  validateContext(request.context);
  if (request.sponsorship_deal) {
    await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
  }
  await store.record(request.context.tenant_id, 'EstimateSponsorshipDeal', request);
  logger.info('estimate_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    sponsor: {
      primary_address: {
        country_code: (request?.details?.destination?.country_code ?? ''),
        lines: [...(request?.details?.destination?.lines ?? [])],
        postal_code: (request?.sponsorship_deal?.billing_address?.postal_code ?? ''),
        region: (request?.details?.destination?.region ?? ''),
      },
    },
  };
  const reply1 = await call((done) => sponsorshipsSponsorCalculationClient.explainSponsorCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    details: {
      destination: {
        locality: (request?.sponsorship_deal?.billing_address?.locality ?? ''),
      },
    },
    sponsor: {
      audit: { created_at: reply1?.sponsor?.audit?.created_at },
      passport: {
        number: ((reply1?.sponsor?.identity_document === 'passport' ? reply1?.sponsor.passport : undefined)?.number ?? ''),
      },
      primary_address: { lines: [...(request?.details?.destination?.lines ?? [])] },
    },
  };
  const method2 = 'estimateSponsor';
  const reply2 = await call((done) => sponsorshipsSponsorCalculationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      sponsorship_deal: {
        audit: {
          created_at: reply1?.sponsor?.audit?.created_at,
          etag: (reply1?.sponsor?.audit?.etag ?? ''),
        },
      },
    },
    {
      sponsorship_deal: {
        audit: { created_by: (reply2?.sponsor?.audit?.created_by ?? '') },
        billing_address: { recipient: (reply2?.sponsor?.primary_address?.recipient ?? '') },
      },
    },
    {
      sponsorship_deal: {
        notes: (request?.sponsorship_deal?.notes ?? ''),
        sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
      },
    },
  );
}

/**
 * Returns a sponsorship deal.
 */
async function getSponsorshipDealRates(request) {
  validateContext(request.context);
  if (!request.sponsorship_deal_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsorship_deal_id is required');
  }
  await store.record(request.context.tenant_id, 'GetSponsorshipDealRates', request);
  logger.info('get_sponsorship_deal_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return {
    sponsorship_deal: { sponsorship_deal_id: (request?.sponsorship_deal_id ?? '') },
  };
}

/**
 * Quotes a sponsorship deal.
 */
async function quoteSponsorshipDeal(request) {
  validateContext(request.context);
  if (request.sponsorship_deal) {
    await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
  }
  await store.record(request.context.tenant_id, 'QuoteSponsorshipDeal', request);
  logger.info('quote_sponsorship_deal', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_section: {
      children: [{
        children: [{
          location: {
            locality: (request?.sponsorship_deal?.billing_address?.locality ?? ''),
            region: (request?.sponsorship_deal?.billing_address?.region ?? ''),
          },
        }],
      }],
    },
  };
  const client1 = clients.registry.get('acme.venues.v1.VenueSectionHistoryService');
  const reply1 = await call((done) => client1.revertVenueSection(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    sponsor_id: (request?.sponsorship_deal?.counterparty?.sponsor_id ?? ''),
  };
  const send2 = sponsorshipsSponsorCalculationClient.getSponsorRates.bind(sponsorshipsSponsorCalculationClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    filter: { region: (reply1?.venue_section?.location?.region ?? '') },
  };
  const spread3 = {
    ...request3,
    context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
  };
  const reply3 = await call((done) => venuesVenueQueryClient.findVenues(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { message: (reply1?.outcome?.message ?? '') },
      sponsorship_deal: {
        audit: { updated_by: (reply1?.venue_section?.audit?.updated_by ?? '') },
      },
    },
    {
      sponsorship_deal: { audit: { created_at: reply2?.sponsor?.audit?.created_at } },
    },
    { result: { details: (reply3?.result?.details ?? {}) } },
    {
      sponsorship_deal: {
        billing_address: request?.sponsorship_deal?.billing_address,
        sponsorship_deal_id: (request?.sponsorship_deal_id ?? ''),
      },
    },
  );
}

/**
 * Recalculates a sponsorship deal.
 */
async function recalculateSponsorshipDeals(request) {
  validateContext(request.context);
  if (request.sponsorship_deal) {
    await store.save(store.toSponsorshipDealRecord(request.context.tenant_id, request.sponsorship_deal));
  }
  await store.record(request.context.tenant_id, 'RecalculateSponsorshipDeals', request);
  logger.info('recalculate_sponsorship_deals', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    sponsor: {
      primary_address: {
        lines: [...(input1?.details?.destination?.lines ?? [])],
        postal_code: (input1?.details?.destination?.postal_code ?? ''),
      },
      relationship_manager: {
        display_name: (input1?.sponsorship_deal?.counterparty?.display_name ?? ''),
      },
    },
  };
  const reply1 = await call((done) => sponsorshipsSponsorCalculationClient.explainSponsorCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    venue_section: {
      children: [{
        children: [{
          audit: { updated_by: (request?.sponsorship_deal?.audit?.updated_by ?? '') },
        }],
      }],
    },
  };
  const cloned2 = JSON.parse(JSON.stringify(request2));
  const reply2 = await call((done) => venuesVenueSectionHistoryClient.annotateVenueSectionVersion(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      sponsorship_deal: { lines: [{ attributes: (reply1?.sponsor?.attributes ?? {}) }] },
    },
    {
      sponsorship_deal: {
        billing_address: { postal_code: (reply2?.venue_section?.location?.postal_code ?? '') },
      },
    },
    {
      breakdown: { total: request?.sponsorship_deal?.total },
      sponsorship_deal: { sponsorship_deal_id: (request?.sponsorship_deal_id ?? '') },
    },
  );
}

module.exports = { calculateSponsorshipDeal, compareSponsorshipDealOptions, estimateSponsorshipDeal, getSponsorshipDealRates, quoteSponsorshipDeal, recalculateSponsorshipDeals };
