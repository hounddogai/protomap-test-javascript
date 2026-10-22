'use strict';

/**
 * Answers read-only questions about venues, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble } = require('../../../platform/rpc');

/**
 * The handlers of VenueQueryService, over the dependencies the venues team is given.
 */
function createVenueQueryHandlers({ store, clients, bus, logger }) {
  /**
   * Lists venues a page at a time.
   */
  async function browseVenues(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'BrowseVenues', request);
    logger.info('browse_venues', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Counts venues by status.
   */
  async function countVenues(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'CountVenues', request);
    logger.info('count_venues', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Lists venues a page at a time.
   */
  async function findVenues(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'FindVenues', request);
    logger.info('find_venues', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Returns a venue.
   */
  async function lookupVenue(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await store.record(request.context.tenant_id, 'LookupVenue', request);
    logger.info('lookup_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { venue: { venue_id: (request?.venue_id ?? '') } };
  }

  /**
   * Returns a venue.
   */
  async function peekVenue(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await store.record(request.context.tenant_id, 'PeekVenue', request);
    logger.info('peek_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: { owner: { email: (request?.context?.actor?.email ?? '') } },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.venuesVenuePolicyHistoryClient.recordVenuePolicyVersion(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    return assemble(
      {
        venue: {
          audit: { created_by: (reply1?.venue_policy?.audit?.created_by ?? '') },
        },
      },
      { venue: { venue_id: (request?.venue_id ?? '') } },
    );
  }

  /**
   * Lists venues a page at a time.
   */
  async function suggestVenues(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'SuggestVenues', request);
    logger.info('suggest_venues', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Summarizes a venue.
   */
  async function summarizeVenues(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'SummarizeVenues', request);
    logger.info('summarize_venues', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      venue_policy: {
        block: {
          notify_email: (request?.context?.actor?.email ?? ''),
          reason: (request?.reason ?? ''),
        },
        owner: { user_id: (request?.context?.actor?.user_id ?? '') },
      },
    };
    const syncVenuePolicies1 = promisify(clients.venuesVenuePolicySyncClient.syncVenuePolicies.bind(clients.venuesVenuePolicySyncClient));
    const reply1 = await syncVenuePolicies1(request1, metadataFor(request1.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      { result: { message: (reply1?.outcome?.message ?? '') } },
      { period: request?.details?.period },
    );
  }

  return {
    browseVenues: unary(browseVenues),
    countVenues: unary(countVenues),
    findVenues: unary(findVenues),
    lookupVenue: unary(lookupVenue),
    peekVenue: unary(peekVenue),
    suggestVenues: unary(suggestVenues),
    summarizeVenues: unary(summarizeVenues),
  };
}

module.exports = { createVenueQueryHandlers };
