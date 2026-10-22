'use strict';

/**
 * Exports venue policies to files and other systems. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { venuesVenueExportClient, venuesVenuePolicySyncClient } = require('../../../platform/clients');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');
const store = require('../store');

/**
 * Cancels a venue policy.
 */
async function cancelVenuePolicyExport(request) {
  validateContext(request.context);
  if (!request.venue_policy_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
  }
  await store.record(request.context.tenant_id, 'CancelVenuePolicyExport', request);
  logger.info('cancel_venue_policy_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    outcome: { effective_at: request?.details?.effective_at },
    venue_policy: { venue_policy_id: (request?.venue_policy_id ?? '') },
  };
}

/**
 * Returns a venue policy.
 */
async function getVenuePolicyExport(request) {
  validateContext(request.context);
  if (!request.venue_policy_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
  }
  await store.record(request.context.tenant_id, 'GetVenuePolicyExport', request);
  logger.info('get_venue_policy_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.context?.deadline,
      owner_id: (request?.venue_policy_id ?? ''),
    },
  };
  const messages1 = await collect(venuesVenueExportClient.streamVenueExport(request1, metadataFor(request1.context), { deadline: deadline() }));
  const reply1 = messages1.at(-1) ?? {};
  return { venue_policy: { venue_policy_id: (request?.venue_policy_id ?? '') } };
}

/**
 * Lists venue policies a page at a time.
 */
async function listVenuePolicyExports(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListVenuePolicyExports', request);
  logger.info('list_venue_policy_exports', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return {};
}

/**
 * Retrys a venue policy.
 */
async function retryVenuePolicyExport(request) {
  validateContext(request.context);
  if (request.venue_policy) {
    await store.save(store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
  }
  await store.record(request.context.tenant_id, 'RetryVenuePolicyExport', request);
  logger.info('retry_venue_policy_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_policy: {
      name: (request?.venue_policy?.name ?? ''),
      venue_policy_id: (request?.venue_policy_id ?? ''),
    },
  };
}

/**
 * Schedules a venue policy.
 */
async function scheduleVenuePolicyExport(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ScheduleVenuePolicyExport', request);
  logger.info('schedule_venue_policy_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    venue_policy: { block: { reason: (request?.reason ?? '') } },
  };
  const reply1 = await withRetry(() => call((done) => venuesVenuePolicySyncClient.pushVenuePolicy(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.details?.cron ?? '') });
  return assemble(
    { result: { status: (reply1?.outcome?.status ?? '') } },
    { period: request?.details?.period },
  );
}

/**
 * Shares a venue policy.
 */
async function shareVenuePolicyExport(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'ShareVenuePolicyExport', request);
  logger.info('share_venue_policy_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_policy: {
      owner: { email: (request?.details?.recipient?.email ?? '') },
      venue_policy_id: (request?.venue_policy_id ?? ''),
    },
  };
}

/**
 * Streams every matching venue policy.
 */
async function streamVenuePolicyExport(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'StreamVenuePolicyExport', request);
  logger.info('stream_venue_policy_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.destination_uri ?? '') });
  return {};
}

module.exports = { cancelVenuePolicyExport, getVenuePolicyExport, listVenuePolicyExports, retryVenuePolicyExport, scheduleVenuePolicyExport, shareVenuePolicyExport, streamVenuePolicyExport };
