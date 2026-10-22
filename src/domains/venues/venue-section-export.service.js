'use strict';

/**
 * Exports venue sections to files and other systems.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call } = require('../../platform/rpc');

/**
 * Serve VenueSectionExportService, recording every operation of the venues team.
 */
class VenueSectionExportService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'VenueSectionExportService' });
  }

  /**
   * Cancels a venue section.
   */
  cancelVenueSectionExport(call, callback) {
    this.executeCancelVenueSectionExport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCancelVenueSectionExport(request) {
    validateContext(request.context);
    if (!request.venue_section_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
    }
    await this.store.record(request.context.tenant_id, 'CancelVenueSectionExport', request);
    this.logger.info('cancel_venue_section_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      outcome: { effective_at: request?.details?.effective_at },
      venue_section: { venue_section_id: (request?.venue_section_id ?? '') },
    };
  }

  /**
   * Returns a venue section.
   */
  getVenueSectionExport(call, callback) {
    this.executeGetVenueSectionExport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetVenueSectionExport(request) {
    validateContext(request.context);
    if (!request.venue_section_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetVenueSectionExport', request);
    this.logger.info('get_venue_section_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: { created_at: request?.context?.deadline },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const method1 = 'resolveVenuePolicyConflict';
    const reply1 = await call((done) => this.clients.venuesVenuePolicySyncClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      {
        venue_section: {
          audit: { created_by: (reply1?.venue_policy?.audit?.created_by ?? '') },
        },
      },
      {
        venue_section: { venue_section_id: (request?.venue_section_id ?? '') },
      },
    );
  }

  /**
   * Lists venue sections a page at a time.
   */
  listVenueSectionExports(call, callback) {
    this.executeListVenueSectionExports(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListVenueSectionExports(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListVenueSectionExports', request);
    this.logger.info('list_venue_section_exports', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_section: { location: { region: (request?.filter?.region ?? '') } },
    };
    const client1 = this.clients.registry.get('acme.venues.v1.VenueSectionSyncService');
    const reply1 = await call((done) => client1.resolveVenueSectionConflict(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: { details: (reply1?.result?.details ?? {}) },
      venue_sections: [{ children: [{ depth: (reply1?.venue_section?.depth ?? 0) }] }],
    };
  }

  /**
   * Schedules a venue section.
   */
  scheduleVenueSectionExport(call, callback) {
    this.executeScheduleVenueSectionExport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScheduleVenueSectionExport(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ScheduleVenueSectionExport', request);
    this.logger.info('schedule_venue_section_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return { period: request?.details?.period };
  }

  /**
   * Shares a venue section.
   */
  shareVenueSectionExport(call, callback) {
    this.executeShareVenueSectionExport(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeShareVenueSectionExport(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ShareVenueSectionExport', request);
    this.logger.info('share_venue_section_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      venue_section: {
        manager: { email: (request?.details?.recipient?.email ?? '') },
        venue_section_id: (request?.venue_section_id ?? ''),
      },
    };
  }

  /**
   * Streams every matching venue section.
   */
  streamVenueSectionExport(call) {
    serverStream((request) => this.executeStreamVenueSectionExport(request))(call);
  }

  async executeStreamVenueSectionExport(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'StreamVenueSectionExport', request);
    this.logger.info('stream_venue_section_export', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return {
      venue_section: { location: { region: (request?.filter?.region ?? '') } },
    };
  }
}

module.exports = { VenueSectionExportService };
