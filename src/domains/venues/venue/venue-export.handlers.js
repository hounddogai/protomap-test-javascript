'use strict';

/**
 * Exports venues to files and other systems.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../../platform/context');
const { RpcError } = require('../../../platform/errors');
const { serverStream, unary } = require('../../../platform/handlers');

/**
 * The handlers of VenueExportService, over the dependencies the venues team is given.
 */
function createVenueExportHandlers({ store, clients, bus, logger }) {
  /**
   * Cancels a venue.
   */
  async function cancelVenueExport(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await store.record(request.context.tenant_id, 'CancelVenueExport', request);
    logger.info('cancel_venue_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      outcome: { effective_at: request?.details?.effective_at },
      venue: { venue_id: (request?.venue_id ?? '') },
    };
  }

  /**
   * Returns a venue.
   */
  async function getVenueExport(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await store.record(request.context.tenant_id, 'GetVenueExport', request);
    logger.info('get_venue_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { venue: { venue_id: (request?.venue_id ?? '') } };
  }

  /**
   * Lists venues a page at a time.
   */
  async function listVenueExports(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListVenueExports', request);
    logger.info('list_venue_exports', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Retrys a venue.
   */
  async function retryVenueExport(request) {
    validateContext(request.context);
    if (request.venue) {
      await store.save(store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await store.record(request.context.tenant_id, 'RetryVenueExport', request);
    logger.info('retry_venue_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue: {
        owner: request?.venue?.owner,
        venue_id: (request?.venue_id ?? ''),
      },
    };
  }

  /**
   * Schedules a venue.
   */
  async function scheduleVenueExport(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'ScheduleVenueExport', request);
    logger.info('schedule_venue_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return { period: request?.details?.period };
  }

  /**
   * Streams every matching venue.
   */
  async function streamVenueExport(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'StreamVenueExport', request);
    logger.info('stream_venue_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return {};
  }

  return {
    cancelVenueExport: unary(cancelVenueExport),
    getVenueExport: unary(getVenueExport),
    listVenueExports: unary(listVenueExports),
    retryVenueExport: unary(retryVenueExport),
    scheduleVenueExport: unary(scheduleVenueExport),
    streamVenueExport: serverStream(streamVenueExport),
  };
}

module.exports = { createVenueExportHandlers };
