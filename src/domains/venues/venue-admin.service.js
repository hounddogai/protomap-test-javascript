'use strict';

/**
 * Operator actions on venues that support staff run by hand.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call } = require('../../platform/rpc');

/**
 * Serve VenueAdminService, recording every operation of the venues team.
 */
class VenueAdminService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'VenueAdminService' });
  }

  /**
   * Archives a venue.
   */
  archiveVenue(call, callback) {
    this.executeArchiveVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeArchiveVenue(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await this.store.record(request.context.tenant_id, 'ArchiveVenue', request);
    this.logger.info('archive_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      outcome: { effective_at: request?.details?.effective_at },
      venue: { venue_id: (request?.venue_id ?? '') },
    };
  }

  /**
   * Returns a venue.
   */
  inspectVenue(call, callback) {
    this.executeInspectVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeInspectVenue(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await this.store.record(request.context.tenant_id, 'InspectVenue', request);
    this.logger.info('inspect_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { venue: { venue_id: (request?.venue_id ?? '') } };
  }

  /**
   * Locks a venue.
   */
  lockVenue(call, callback) {
    this.executeLockVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLockVenue(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LockVenue', request);
    this.logger.info('lock_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      outcome: { effective_at: request?.details?.effective_at },
      venue: { venue_id: (request?.venue_id ?? '') },
    };
  }

  /**
   * Reassigns a venue.
   */
  reassignVenue(call, callback) {
    this.executeReassignVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeReassignVenue(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await this.store.record(request.context.tenant_id, 'ReassignVenue', request);
    this.logger.info('reassign_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue: {
        capacity: String((request?.details?.capacity ?? 0)),
        venue_id: (request?.venue_id ?? ''),
      },
    };
  }

  /**
   * Repairs a venue.
   */
  repairVenue(call, callback) {
    this.executeRepairVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRepairVenue(request) {
    validateContext(request.context);
    if (request.venue) {
      await this.store.save(this.store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await this.store.record(request.context.tenant_id, 'RepairVenue', request);
    this.logger.info('repair_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue: {
        name: (request?.venue?.name ?? ''),
        venue_id: (request?.venue_id ?? ''),
      },
    };
  }

  /**
   * Restores a venue.
   */
  restoreVenue(call, callback) {
    this.executeRestoreVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRestoreVenue(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await this.store.record(request.context.tenant_id, 'RestoreVenue', request);
    this.logger.info('restore_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      filter: {
        created_after: input1?.details?.effective_at,
        labels: { ...(input1?.annotations ?? {}) },
        owner_id: (input1?.details?.new_owner?.user_id ?? ''),
      },
    };
    const reply1 = await call((done) => this.clients.venuesVenuePolicySyncClient.listVenuePolicyConflicts(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        outcome: { effective_at: request?.details?.effective_at },
        venue: { venue_id: (request?.venue_id ?? '') },
      },
    );
  }

  /**
   * Transfers a venue.
   */
  transferVenue(call, callback) {
    this.executeTransferVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeTransferVenue(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await this.store.record(request.context.tenant_id, 'TransferVenue', request);
    this.logger.info('transfer_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      outcome: { effective_at: request?.details?.effective_at },
      venue: { venue_id: (request?.venue_id ?? '') },
    };
  }

  /**
   * Unlocks a venue.
   */
  unlockVenue(call, callback) {
    this.executeUnlockVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeUnlockVenue(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await this.store.record(request.context.tenant_id, 'UnlockVenue', request);
    this.logger.info('unlock_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      outcome: { effective_at: request?.details?.effective_at },
      venue: { venue_id: (request?.venue_id ?? '') },
    };
  }
}

module.exports = { VenueAdminService };
