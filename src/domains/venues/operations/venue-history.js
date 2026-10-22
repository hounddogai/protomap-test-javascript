'use strict';

/**
 * Keeps every version of a venue and restores earlier ones. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../../platform/context');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const store = require('../store');

/**
 * Annotates a venue.
 */
async function annotateVenueVersion(request) {
  validateContext(request.context);
  if (request.venue) {
    await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
  }
  await store.record(request.context.tenant_id, 'AnnotateVenueVersion', request);
  logger.info('annotate_venue_version', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue: {
      capacity: (request?.venue?.capacity ?? '0'),
      venue_id: (request?.venue_id ?? ''),
    },
  };
}

/**
 * Compares a venue.
 */
async function compareVenueVersions(request) {
  validateContext(request.context);
  if (request.venue) {
    await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
  }
  await store.record(request.context.tenant_id, 'CompareVenueVersions', request);
  logger.info('compare_venue_versions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue: {
      owner: request?.venue?.owner,
      venue_id: (request?.venue_id ?? ''),
    },
  };
}

/**
 * Returns a venue.
 */
async function getVenueVersion(request) {
  validateContext(request.context);
  if (!request.venue_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
  }
  await store.record(request.context.tenant_id, 'GetVenueVersion', request);
  logger.info('get_venue_version', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  return { venue: { venue_id: (request?.venue_id ?? '') } };
}

/**
 * Lists venues a page at a time.
 */
async function listVenueVersions(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListVenueVersions', request);
  logger.info('list_venue_versions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Records a venue.
 */
async function recordVenueVersion(request) {
  validateContext(request.context);
  if (request.venue) {
    await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
  }
  await store.record(request.context.tenant_id, 'RecordVenueVersion', request);
  logger.info('record_venue_version', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue: {
      name: (request?.venue?.name ?? ''),
      venue_id: (request?.venue_id ?? ''),
    },
  };
}

/**
 * Reverts a venue.
 */
async function revertVenue(request) {
  validateContext(request.context);
  if (request.venue) {
    await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
  }
  await store.record(request.context.tenant_id, 'RevertVenue', request);
  logger.info('revert_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue: {
      name: (request?.venue?.name ?? ''),
      venue_id: (request?.venue_id ?? ''),
    },
  };
}

module.exports = { annotateVenueVersion, compareVenueVersions, getVenueVersion, listVenueVersions, recordVenueVersion, revertVenue };
