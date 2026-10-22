'use strict';

/**
 * Operator actions on venue sections that support staff run by hand. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { venuesVenueExportClient } = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { call } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Archives a venue section.
 */
async function archiveVenueSection(request) {
  validateContext(request.context);
  if (!request.venue_section_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
  }
  await store.record(request.context.tenant_id, 'ArchiveVenueSection', request);
  logger.info('archive_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    outcome: { effective_at: request?.details?.effective_at },
    venue_section: { venue_section_id: (request?.venue_section_id ?? '') },
  };
}

/**
 * Locks a venue section.
 */
async function lockVenueSection(request) {
  validateContext(request.context);
  if (!request.venue_section_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
  }
  await store.record(request.context.tenant_id, 'LockVenueSection', request);
  logger.info('lock_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    outcome: { effective_at: request?.details?.effective_at },
    venue_section: { venue_section_id: (request?.venue_section_id ?? '') },
  };
}

/**
 * Deletes a venue section.
 */
async function purgeVenueSection(request) {
  validateContext(request.context);
  if (!request.venue_section_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
  }
  await store.record(request.context.tenant_id, 'PurgeVenueSection', request);
  logger.info('purge_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      labels: { ...(request?.context?.baggage ?? {}) },
      owner_id: (request?.context?.actor?.user_id ?? ''),
    },
  };
  const reply1 = await call((done) => venuesVenueExportClient.listVenueExports(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.etag ?? '') });
  return {
    result: {
      details: (reply1?.result?.details ?? {}),
      status: (reply1?.result?.status ?? ''),
    },
  };
}

/**
 * Reassigns a venue section.
 */
async function reassignVenueSection(request) {
  validateContext(request.context);
  if (!request.venue_section_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
  }
  await store.record(request.context.tenant_id, 'ReassignVenueSection', request);
  logger.info('reassign_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      labels: { ...(request?.annotations ?? {}) },
      owner_id: (request?.details?.assignee?.user_id ?? ''),
    },
  };
  const messages1 = await collect(venuesVenueExportClient.streamVenueExport(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_section: {
      manager: { user_id: (request?.details?.assignee?.user_id ?? '') },
      venue_section_id: (request?.venue_section_id ?? ''),
    },
  };
}

/**
 * Restores a venue section.
 */
async function restoreVenueSection(request) {
  validateContext(request.context);
  if (!request.venue_section_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
  }
  await store.record(request.context.tenant_id, 'RestoreVenueSection', request);
  logger.info('restore_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    outcome: { effective_at: request?.details?.effective_at },
    venue_section: { venue_section_id: (request?.venue_section_id ?? '') },
  };
}

/**
 * Transfers a venue section.
 */
async function transferVenueSection(request) {
  validateContext(request.context);
  if (!request.venue_section_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
  }
  await store.record(request.context.tenant_id, 'TransferVenueSection', request);
  logger.info('transfer_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    outcome: { effective_at: request?.details?.effective_at },
    venue_section: { venue_section_id: (request?.venue_section_id ?? '') },
  };
}

module.exports = { archiveVenueSection, lockVenueSection, purgeVenueSection, reassignVenueSection, restoreVenueSection, transferVenueSection };
