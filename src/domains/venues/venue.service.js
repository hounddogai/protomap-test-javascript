'use strict';

/**
 * Manages venues: creates, reads, changes, and lists them.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { clientStream } = require('../../platform/handlers');
const { call, deferred } = require('../../platform/rpc');

/**
 * Serve VenueService, recording every operation of the venues team.
 */
class VenueService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'VenueService' });
  }

  /**
   * Takes a stream of venues and answers once at the end.
   */
  batchCreateVenues(call, callback) {
    clientStream((request) => this.executeBatchCreateVenues(request), () => ({}))(call, callback);
  }

  async executeBatchCreateVenues(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'BatchCreateVenues', request);
    this.logger.info('batch_create_venues', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? '') });
    return {};
  }

  /**
   * Creates a venue.
   */
  cloneVenue(call, callback) {
    this.executeCloneVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCloneVenue(request) {
    validateContext(request.context);
    if (request.venue) {
      await this.store.save(this.store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await this.store.record(request.context.tenant_id, 'CloneVenue', request);
    this.logger.info('clone_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.request_id ?? '') });
    return {
      venue: {
        capacity: (request?.venue?.capacity ?? '0'),
        status: (request?.venue?.status ?? ''),
      },
    };
  }

  /**
   * Creates a venue.
   */
  createVenue(call, callback) {
    this.executeCreateVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCreateVenue(request) {
    validateContext(request.context);
    if (request.venue) {
      await this.store.save(this.store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await this.store.record(request.context.tenant_id, 'CreateVenue', request);
    this.logger.info('create_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.request_id ?? '') });
    return {
      venue: {
        name: (request?.venue?.name ?? ''),
        status: (request?.venue?.status ?? ''),
      },
    };
  }

  /**
   * Deletes a venue.
   */
  deleteVenue(call, callback) {
    this.executeDeleteVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDeleteVenue(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await this.store.record(request.context.tenant_id, 'DeleteVenue', request);
    this.logger.info('delete_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { filters: { ...(request?.context?.baggage ?? {}) } },
      reason: (request?.reason ?? ''),
      venue_id: (request?.venue_id ?? ''),
    };
    const task1 = deferred(() => call((done) => this.clients.venuesVenueExportClient.scheduleVenueExport(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    this.logger.info('local_audit', { value: (request?.etag ?? '') });
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    };
  }

  /**
   * Returns a venue.
   */
  getVenue(call, callback) {
    this.executeGetVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetVenue(request) {
    validateContext(request.context);
    if (!request.venue_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetVenue', request);
    this.logger.info('get_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { venue: { venue_id: (request?.venue_id ?? '') } };
  }

  /**
   * Lists venues a page at a time.
   */
  searchVenues(call, callback) {
    this.executeSearchVenues(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSearchVenues(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SearchVenues', request);
    this.logger.info('search_venues', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      page: { page_size: (request?.page?.page_size ?? 0) },
    };
    const reply1 = await this.bus.request('acme.venues.v1.VenueService/SearchVenues:0', request1);
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      page_info: { next_page_token: (reply1?.page_info?.next_page_token ?? '') },
      result: { details: (reply1?.result?.details ?? {}) },
    };
  }

  /**
   * Changes a venue.
   */
  updateVenue(call, callback) {
    this.executeUpdateVenue(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeUpdateVenue(request) {
    validateContext(request.context);
    if (request.venue) {
      await this.store.save(this.store.toVenueRecord(request.context.tenant_id, request.venue));
    }
    await this.store.record(request.context.tenant_id, 'UpdateVenue', request);
    this.logger.info('update_venue', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.venue?.audit?.etag ?? '') });
    return {
      venue: {
        capacity: (request?.venue?.capacity ?? '0'),
        specs: (request?.venue?.specs ?? {}),
      },
    };
  }
}

module.exports = { VenueService };
