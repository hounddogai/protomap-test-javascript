'use strict';

/**
 * Operator actions on venue policies that support staff run by hand.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of VenuePolicyAdminService, over the dependencies the venues team is given.
 */
function createVenuePolicyAdminHandlers({ store, clients, bus, logger }) {
  /**
   * Archives a venue policy.
   */
  async function archiveVenuePolicy(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await store.record(request.context.tenant_id, 'ArchiveVenuePolicy', request);
    logger.info('archive_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      outcome: { effective_at: request?.details?.effective_at },
      venue_policy: { venue_policy_id: (request?.venue_policy_id ?? '') },
    };
  }

  /**
   * Returns a venue policy.
   */
  async function inspectVenuePolicy(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await store.record(request.context.tenant_id, 'InspectVenuePolicy', request);
    logger.info('inspect_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { venue_policy: { venue_policy_id: (request?.venue_policy_id ?? '') } };
  }

  /**
   * Locks a venue policy.
   */
  async function lockVenuePolicy(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await store.record(request.context.tenant_id, 'LockVenuePolicy', request);
    logger.info('lock_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      outcome: { effective_at: request?.details?.effective_at },
      venue_policy: { venue_policy_id: (request?.venue_policy_id ?? '') },
    };
  }

  /**
   * Deletes a venue policy.
   */
  async function purgeVenuePolicy(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await store.record(request.context.tenant_id, 'PurgeVenuePolicy', request);
    logger.info('purge_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.etag ?? '') });
    return {};
  }

  /**
   * Reassigns a venue policy.
   */
  async function reassignVenuePolicy(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await store.record(request.context.tenant_id, 'ReassignVenuePolicy', request);
    logger.info('reassign_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        owner: { user_id: (request?.details?.assignee?.user_id ?? '') },
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }

  /**
   * Repairs a venue policy.
   */
  async function repairVenuePolicy(request) {
    validateContext(request.context);
    if (request.venue_policy) {
      await store.save(store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
    }
    await store.record(request.context.tenant_id, 'RepairVenuePolicy', request);
    logger.info('repair_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        description: (request?.venue_policy?.description ?? ''),
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }

  /**
   * Restores a venue policy.
   */
  async function restoreVenuePolicy(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await store.record(request.context.tenant_id, 'RestoreVenuePolicy', request);
    logger.info('restore_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      outcome: { effective_at: request?.details?.effective_at },
      venue_policy: { venue_policy_id: (request?.venue_policy_id ?? '') },
    };
  }

  /**
   * Transfers a venue policy.
   */
  async function transferVenuePolicy(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await store.record(request.context.tenant_id, 'TransferVenuePolicy', request);
    logger.info('transfer_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      outcome: { effective_at: request?.details?.effective_at },
      venue_policy: { venue_policy_id: (request?.venue_policy_id ?? '') },
    };
  }

  /**
   * Unlocks a venue policy.
   */
  async function unlockVenuePolicy(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await store.record(request.context.tenant_id, 'UnlockVenuePolicy', request);
    logger.info('unlock_venue_policy', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.details?.effective_at,
        owner_id: (request?.details?.new_owner?.user_id ?? ''),
      },
    };
    const messages1 = await collect(clients.venuesVenueExportClient.streamVenueExport(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      outcome: { effective_at: request?.details?.effective_at },
      venue_policy: { venue_policy_id: (request?.venue_policy_id ?? '') },
    };
  }

  return {
    archiveVenuePolicy: unary(archiveVenuePolicy),
    inspectVenuePolicy: unary(inspectVenuePolicy),
    lockVenuePolicy: unary(lockVenuePolicy),
    purgeVenuePolicy: unary(purgeVenuePolicy),
    reassignVenuePolicy: unary(reassignVenuePolicy),
    repairVenuePolicy: unary(repairVenuePolicy),
    restoreVenuePolicy: unary(restoreVenuePolicy),
    transferVenuePolicy: unary(transferVenuePolicy),
    unlockVenuePolicy: unary(unlockVenuePolicy),
  };
}

module.exports = { createVenuePolicyAdminHandlers };
