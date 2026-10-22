'use strict';

/**
 * Keeps venue policies in step with the systems of record in other regions. Each operation validates, records, and
 * calls.
 */

const grpc = require('@grpc/grpc-js');
const clients = require('../../../platform/clients');
const { venuesVenueSectionSyncClient } = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Lists venue policies a page at a time.
 */
async function listVenuePolicyConflicts(request) {
  validateContext(request.context);
  const pageSize = request.page?.page_size ?? 0;
  if (pageSize < 0 || pageSize > 500) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
  }
  await store.record(request.context.tenant_id, 'ListVenuePolicyConflicts', request);
  logger.info('list_venue_policy_conflicts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.filter?.created_after,
      labels: { ...(request?.filter?.labels ?? {}) },
    },
    page: {
      page_size: (request?.page?.page_size ?? 0),
      page_token: (request?.page?.page_token ?? ''),
    },
  };
  const reply1 = await runInContext(request.context, () => call((done) => venuesVenueSectionSyncClient.listVenueSectionConflicts(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.order_by ?? '') });
  return { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } };
}

/**
 * Pushs a venue policy.
 */
async function pushVenuePolicy(request) {
  validateContext(request.context);
  if (request.venue_policy) {
    await store.save(store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
  }
  await store.record(request.context.tenant_id, 'PushVenuePolicy', request);
  logger.info('push_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.venue_policy?.audit?.updated_at,
      labels: { ...(request?.venue_policy?.parameters ?? {}) },
      owner_id: (request?.venue_policy?.venue_policy_id ?? ''),
    },
  };
  const reply1 = await call((done) => clients.venuesVenueSectionSyncClient.listVenueSectionConflicts(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    },
    {
      venue_policy: {
        venue_policy_id: (request?.venue_policy_id ?? ''),
        version: (request?.venue_policy?.version ?? '0'),
      },
    },
  );
}

/**
 * Reconciles a venue policy.
 */
async function reconcileVenuePolicies(request) {
  validateContext(request.context);
  if (request.venue_policy) {
    await store.save(store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
  }
  await store.record(request.context.tenant_id, 'ReconcileVenuePolicies', request);
  logger.info('reconcile_venue_policies', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_policy: {
      name: (request?.venue_policy?.name ?? ''),
      venue_policy_id: (request?.venue_policy_id ?? ''),
    },
  };
}

/**
 * Resolves a venue policy.
 */
async function resolveVenuePolicyConflict(request) {
  validateContext(request.context);
  if (request.venue_policy) {
    await store.save(store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
  }
  await store.record(request.context.tenant_id, 'ResolveVenuePolicyConflict', request);
  logger.info('resolve_venue_policy_conflict', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_policy: {
      effective: request?.venue_policy?.effective,
      venue_policy_id: (request?.venue_policy_id ?? ''),
    },
  };
}

/**
 * Exchanges changes of venue policies with a peer.
 */
async function streamVenuePolicyChanges(request) {
  validateContext(request.context);
  await store.record(request.context.tenant_id, 'StreamVenuePolicyChanges', request);
  logger.info('stream_venue_policy_changes', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.cursor ?? '') });
  return {
    result: {
      status: ((request?.frame === 'ack' ? request.ack : undefined)?.status ?? ''),
    },
    venue_policy: {
      venue_policy_id: ((request?.frame === 'change' ? request.change : undefined)?.venue_policy_id ?? ''),
    },
  };
}

/**
 * Syncs a venue policy.
 */
async function syncVenuePolicies(request) {
  validateContext(request.context);
  if (request.venue_policy) {
    await store.save(store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
  }
  await store.record(request.context.tenant_id, 'SyncVenuePolicies', request);
  logger.info('sync_venue_policies', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return {
    venue_policy: {
      venue_policy_id: (request?.venue_policy_id ?? ''),
      version: (request?.venue_policy?.version ?? '0'),
    },
  };
}

module.exports = { listVenuePolicyConflicts, pushVenuePolicy, reconcileVenuePolicies, resolveVenuePolicyConflict, streamVenuePolicyChanges, syncVenuePolicies };
