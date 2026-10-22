'use strict';

/**
 * Keeps every version of a venue policy and restores earlier ones.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred } = require('../../platform/rpc');

/**
 * Serve VenuePolicyHistoryService, recording every operation of the venues team.
 */
class VenuePolicyHistoryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'VenuePolicyHistoryService' });
  }

  /**
   * Annotates a venue policy.
   */
  annotateVenuePolicyVersion(call, callback) {
    this.executeAnnotateVenuePolicyVersion(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeAnnotateVenuePolicyVersion(request) {
    validateContext(request.context);
    if (request.venue_policy) {
      await this.store.save(this.store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
    }
    await this.store.record(request.context.tenant_id, 'AnnotateVenuePolicyVersion', request);
    this.logger.info('annotate_venue_policy_version', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.venue_policy?.audit?.created_at,
        labels: { ...(request?.venue_policy?.parameters ?? {}) },
        owner_id: (request?.venue_policy?.owner?.user_id ?? ''),
      },
    };
    const task1 = deferred(() => call((done) => this.clients.venuesVenueHistoryClient.listVenueVersions(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        venue_policy: {
          parameters: (request?.venue_policy?.parameters ?? {}),
          venue_policy_id: (request?.venue_policy_id ?? ''),
        },
      },
    );
  }

  /**
   * Compares a venue policy.
   */
  compareVenuePolicyVersions(call, callback) {
    this.executeCompareVenuePolicyVersions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCompareVenuePolicyVersions(request) {
    validateContext(request.context);
    if (request.venue_policy) {
      await this.store.save(this.store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
    }
    await this.store.record(request.context.tenant_id, 'CompareVenuePolicyVersions', request);
    this.logger.info('compare_venue_policy_versions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        description: (request?.venue_policy?.description ?? ''),
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }

  /**
   * Returns a venue policy.
   */
  getVenuePolicyVersion(call, callback) {
    this.executeGetVenuePolicyVersion(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetVenuePolicyVersion(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetVenuePolicyVersion', request);
    this.logger.info('get_venue_policy_version', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { venue_policy: { venue_policy_id: (request?.venue_policy_id ?? '') } };
  }

  /**
   * Lists venue policies a page at a time.
   */
  listVenuePolicyVersions(call, callback) {
    this.executeListVenuePolicyVersions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListVenuePolicyVersions(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListVenuePolicyVersions', request);
    this.logger.info('list_venue_policy_versions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Deletes a venue policy.
   */
  pruneVenuePolicyVersions(call, callback) {
    this.executePruneVenuePolicyVersions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePruneVenuePolicyVersions(request) {
    validateContext(request.context);
    if (!request.venue_policy_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_policy_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PruneVenuePolicyVersions', request);
    this.logger.info('prune_venue_policy_versions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.etag ?? '') });
    return {};
  }

  /**
   * Records a venue policy.
   */
  recordVenuePolicyVersion(call, callback) {
    this.executeRecordVenuePolicyVersion(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecordVenuePolicyVersion(request) {
    validateContext(request.context);
    if (request.venue_policy) {
      await this.store.save(this.store.toVenuePolicyRecord(request.context.tenant_id, request.venue_policy));
    }
    await this.store.record(request.context.tenant_id, 'RecordVenuePolicyVersion', request);
    this.logger.info('record_venue_policy_version', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_policy: {
        owner: request?.venue_policy?.owner,
        venue_policy_id: (request?.venue_policy_id ?? ''),
      },
    };
  }
}

module.exports = { VenuePolicyHistoryService };
