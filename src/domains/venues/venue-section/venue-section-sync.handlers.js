'use strict';

/**
 * Keeps venue sections in step with the systems of record in other regions.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../../platform/context');
const { RpcError } = require('../../../platform/errors');
const { duplex, unary } = require('../../../platform/handlers');

/**
 * The handlers of VenueSectionSyncService, over the dependencies the venues team is given.
 */
function createVenueSectionSyncHandlers({ store, clients, bus, logger }) {
  /**
   * Returns a venue section.
   */
  async function getVenueSectionSyncState(request) {
    validateContext(request.context);
    if (!request.venue_section_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
    }
    await store.record(request.context.tenant_id, 'GetVenueSectionSyncState', request);
    logger.info('get_venue_section_sync_state', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return {
      venue_section: { venue_section_id: (request?.venue_section_id ?? '') },
    };
  }

  /**
   * Lists venue sections a page at a time.
   */
  async function listVenueSectionConflicts(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListVenueSectionConflicts', request);
    logger.info('list_venue_section_conflicts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Pushs a venue section.
   */
  async function pushVenueSection(request) {
    validateContext(request.context);
    if (request.venue_section) {
      await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
    }
    await store.record(request.context.tenant_id, 'PushVenueSection', request);
    logger.info('push_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_section: {
        parent_id: (request?.venue_section?.parent_id ?? ''),
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    };
  }

  /**
   * Reconciles a venue section.
   */
  async function reconcileVenueSections(request) {
    validateContext(request.context);
    if (request.venue_section) {
      await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
    }
    await store.record(request.context.tenant_id, 'ReconcileVenueSections', request);
    logger.info('reconcile_venue_sections', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_section: {
        manager: request?.venue_section?.manager,
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    };
  }

  /**
   * Resolves a venue section.
   */
  async function resolveVenueSectionConflict(request) {
    validateContext(request.context);
    if (request.venue_section) {
      await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
    }
    await store.record(request.context.tenant_id, 'ResolveVenueSectionConflict', request);
    logger.info('resolve_venue_section_conflict', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_section: {
        location: request?.venue_section?.location,
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    };
  }

  /**
   * Exchanges changes of venue sections with a peer.
   */
  async function streamVenueSectionChanges(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'StreamVenueSectionChanges', request);
    logger.info('stream_venue_section_changes', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.cursor ?? '') });
    return {
      result: {
        status: ((request?.frame === 'ack' ? request.ack : undefined)?.status ?? ''),
      },
      sequence: ((request?.frame === 'ack' ? request.ack : undefined)?.sequence ?? '0'),
    };
  }

  /**
   * Syncs a venue section.
   */
  async function syncVenueSections(request) {
    validateContext(request.context);
    if (request.venue_section) {
      await store.save(store.toVenueSectionRecord(request.context.tenant_id, request.venue_section));
    }
    await store.record(request.context.tenant_id, 'SyncVenueSections', request);
    logger.info('sync_venue_sections', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_section: {
        attributes: (request?.venue_section?.attributes ?? {}),
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    };
  }

  return {
    getVenueSectionSyncState: unary(getVenueSectionSyncState),
    listVenueSectionConflicts: unary(listVenueSectionConflicts),
    pushVenueSection: unary(pushVenueSection),
    reconcileVenueSections: unary(reconcileVenueSections),
    resolveVenueSectionConflict: unary(resolveVenueSectionConflict),
    streamVenueSectionChanges: duplex(streamVenueSectionChanges),
    syncVenueSections: unary(syncVenueSections),
  };
}

module.exports = { createVenueSectionSyncHandlers };
