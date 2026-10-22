'use strict';

/**
 * Answers read-only questions about venue sections, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { call } = require('../../platform/rpc');

/**
 * Serve VenueSectionQueryService, recording every operation of the venues team.
 */
class VenueSectionQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'VenueSectionQueryService' });
  }

  /**
   * Lists venue sections a page at a time.
   */
  browseVenueSections(call, callback) {
    this.executeBrowseVenueSections(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseVenueSections(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseVenueSections', request);
    this.logger.info('browse_venue_sections', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: { created_at: request?.filter?.created_after },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.venuesVenuePolicyHistoryClient.annotateVenuePolicyVersion(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      venue_sections: [{
        audit: { created_at: reply1?.venue_policy?.audit?.created_at },
        children: [{ audit: { created_at: reply1?.venue_policy?.audit?.created_at } }],
      }],
    };
  }

  /**
   * Streams every matching venue section.
   */
  exportVenueSections(call) {
    serverStream((request) => this.executeExportVenueSections(request))(call);
  }

  async executeExportVenueSections(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportVenueSections', request);
    this.logger.info('export_venue_sections', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return {
      venue_section: { location: { region: (request?.filter?.region ?? '') } },
    };
  }

  /**
   * Lists venue sections a page at a time.
   */
  findVenueSections(call, callback) {
    this.executeFindVenueSections(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindVenueSections(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindVenueSections', request);
    this.logger.info('find_venue_sections', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Returns a venue section.
   */
  lookupVenueSection(call, callback) {
    this.executeLookupVenueSection(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupVenueSection(request) {
    validateContext(request.context);
    if (!request.venue_section_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupVenueSection', request);
    this.logger.info('lookup_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return {
      venue_section: { venue_section_id: (request?.venue_section_id ?? '') },
    };
  }

  /**
   * Returns a venue section.
   */
  peekVenueSection(call, callback) {
    this.executePeekVenueSection(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekVenueSection(request) {
    validateContext(request.context);
    if (!request.venue_section_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'venue_section_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekVenueSection', request);
    this.logger.info('peek_venue_section', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return {
      venue_section: { venue_section_id: (request?.venue_section_id ?? '') },
    };
  }

  /**
   * Returns several venue sections at once.
   */
  resolveVenueSections(call, callback) {
    this.executeResolveVenueSections(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveVenueSections(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveVenueSections', request);
    this.logger.info('resolve_venue_sections', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { filters: { ...(request?.context?.baggage ?? {}) } },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.venuesVenueQueryClient.summarizeVenues(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    return { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } };
  }

  /**
   * Summarizes a venue section.
   */
  summarizeVenueSections(call, callback) {
    this.executeSummarizeVenueSections(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeVenueSections(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeVenueSections', request);
    this.logger.info('summarize_venue_sections', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return { period: request?.details?.period };
  }
}

module.exports = { VenueSectionQueryService };
