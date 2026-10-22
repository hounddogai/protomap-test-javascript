'use strict';

/**
 * Keeps every version of a venue section and restores earlier ones.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { call } = require('../../../platform/rpc');

/**
 * The handlers of VenueSectionHistoryService, over the dependencies the venues team is given.
 */
function createVenueSectionHistoryHandlers({ store, clients, bus, logger }) {
  /**
   * Annotates a venue section.
   */
  async function annotateVenueSectionVersion(request) {
    validateContext(request.context);
    if (request.venue_section) {
      await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
    }
    await store.record(request.context.tenant_id, 'AnnotateVenueSectionVersion', request);
    logger.info('annotate_venue_section_version', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_section: {
        location: request?.venue_section?.location,
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    };
  }

  /**
   * Compares a venue section.
   */
  async function compareVenueSectionVersions(request) {
    validateContext(request.context);
    if (request.venue_section) {
      await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
    }
    await store.record(request.context.tenant_id, 'CompareVenueSectionVersions', request);
    logger.info('compare_venue_section_versions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_section: {
        parent_id: (request?.venue_section?.parent_id ?? ''),
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    };
  }

  /**
   * Lists venue sections a page at a time.
   */
  async function listVenueSectionVersions(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListVenueSectionVersions', request);
    logger.info('list_venue_section_versions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: { owner: { email: (request?.context?.actor?.email ?? '') } },
    };
    const send1 = clients.venuesVenuePolicySyncClient.pushVenuePolicy.bind(clients.venuesVenuePolicySyncClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      venue_sections: [{
        children: [{
          children: [{
            audit: { updated_by: (reply1?.venue_policy?.audit?.updated_by ?? '') },
          }],
        }],
        manager: { email: (reply1?.venue_policy?.owner?.email ?? '') },
      }],
    };
  }

  /**
   * Deletes a venue section.
   */
  async function pruneVenueSectionVersions(request) {
    validateContext(request.context);
    if (!request.venue_section_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
    }
    await store.record(request.context.tenant_id, 'PruneVenueSectionVersions', request);
    logger.info('prune_venue_section_versions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.etag ?? '') });
    return {};
  }

  /**
   * Records a venue section.
   */
  async function recordVenueSectionVersion(request) {
    validateContext(request.context);
    if (request.venue_section) {
      await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
    }
    await store.record(request.context.tenant_id, 'RecordVenueSectionVersion', request);
    logger.info('record_venue_section_version', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_section: {
        attributes: (request?.venue_section?.attributes ?? {}),
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    };
  }

  /**
   * Reverts a venue section.
   */
  async function revertVenueSection(request) {
    validateContext(request.context);
    if (request.venue_section) {
      await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
    }
    await store.record(request.context.tenant_id, 'RevertVenueSection', request);
    logger.info('revert_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_section: {
        parent_id: (request?.venue_section?.parent_id ?? ''),
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    };
  }

  return {
    annotateVenueSectionVersion: unary(annotateVenueSectionVersion),
    compareVenueSectionVersions: unary(compareVenueSectionVersions),
    listVenueSectionVersions: unary(listVenueSectionVersions),
    pruneVenueSectionVersions: unary(pruneVenueSectionVersions),
    recordVenueSectionVersion: unary(recordVenueSectionVersion),
    revertVenueSection: unary(revertVenueSection),
  };
}

module.exports = { createVenueSectionHistoryHandlers };
