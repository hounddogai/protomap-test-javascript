'use strict';

/**
 * Answers read-only questions about seat maps, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve SeatMapQueryService, recording every operation of the seating team.
 */
class SeatMapQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'SeatMapQueryService' });
  }

  /**
   * Lists seat maps a page at a time.
   */
  browseSeatMaps(call, callback) {
    this.executeBrowseSeatMaps(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseSeatMaps(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseSeatMaps', request);
    this.logger.info('browse_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            manager: {
              email: (request?.context?.actor?.email ?? ''),
              user_id: (request?.context?.actor?.user_id ?? ''),
            },
          }],
          location: { region: (request?.filter?.region ?? '') },
        }],
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.seatingSeatMapCalculationClient.recalculateSeatMaps(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { seat_maps: [{ attributes: (reply1?.seat_map?.attributes ?? {}) }] };
  }

  /**
   * Counts seat maps by status.
   */
  countSeatMaps(call, callback) {
    this.executeCountSeatMaps(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountSeatMaps(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountSeatMaps', request);
    this.logger.info('count_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
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
    const reply1 = await withRetry(() => call((done) => this.clients.searchSearchIndexQueryClient.suggestSearchIndexes(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        message: (reply1?.result?.message ?? ''),
        status: (reply1?.result?.status ?? ''),
      },
    };
  }

  /**
   * Streams every matching seat map.
   */
  exportSeatMaps(call) {
    serverStream((request) => this.executeExportSeatMaps(request))(call);
  }

  async executeExportSeatMaps(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportSeatMaps', request);
    this.logger.info('export_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_hold: {
        audit: { created_at: request?.filter?.created_after },
        location: { region: (request?.filter?.region ?? '') },
        organizer: { user_id: (request?.context?.actor?.user_id ?? '') },
        participants: [{ contact: { email: (request?.context?.actor?.email ?? '') } }],
      },
    };
    const task1 = deferred(() => call((done) => this.clients.seatingSeatHoldCalculationClient.estimateSeatHold(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      {
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
        seat_map: { location: { locality: (reply1?.seat_hold?.location?.locality ?? '') } },
      },
      { seat_map: { location: { region: (request?.filter?.region ?? '') } } },
    );
  }

  /**
   * Lists seat maps a page at a time.
   */
  findSeatMaps(call, callback) {
    this.executeFindSeatMaps(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindSeatMaps(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindSeatMaps', request);
    this.logger.info('find_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      wristband: {
        address: { region: (request?.filter?.region ?? '') },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const reply1 = await this.bus.request('acme.seating.v1.SeatMapQueryService/FindSeatMaps:0', request1);
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      seat_maps: [{
        children: [{
          children: [{ audit: { etag: (reply1?.wristband?.audit?.etag ?? '') } }],
        }],
      }],
    };
  }

  /**
   * Returns a seat map.
   */
  lookupSeatMap(call, callback) {
    this.executeLookupSeatMap(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupSeatMap(request) {
    validateContext(request.context);
    if (!request.seat_map_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupSeatMap', request);
    this.logger.info('lookup_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { locale: (request?.context?.locale ?? '') },
      seat_assignment: { customer: { seat_map_id: (request?.seat_map_id ?? '') } },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.seatingSeatAssignmentValidationClient.lintSeatAssignments(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.seat_assignment?.audit?.created_at,
        labels: { ...(reply1?.result?.details ?? {}) },
      },
    };
    const messages2 = await collect(this.clients.notificationsNotificationQueryClient.exportNotifications(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    return assemble(
      { result: { message: (reply1?.outcome?.message ?? '') } },
      { seat_map: { seat_map_id: (request?.seat_map_id ?? '') } },
    );
  }

  /**
   * Returns a seat map.
   */
  peekSeatMap(call, callback) {
    this.executePeekSeatMap(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekSeatMap(request) {
    validateContext(request.context);
    if (!request.seat_map_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekSeatMap', request);
    this.logger.info('peek_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
    };
    const reply1 = await call((done) => this.clients.broadcastsReplayClipValidationClient.previewReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.outcome?.effective_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (reply1?.outcome?.reference_id ?? ''),
        region: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? ''),
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      this.clients.broadcastsBroadcastQueryClient.suggestBroadcasts(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    return assemble(
      {
        seat_map: {
          children: [{
            location: {
              locality: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.locality ?? ''),
              postal_code: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.postal_code ?? ''),
            },
          }],
        },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          status: (reply2?.result?.status ?? ''),
        },
      },
      { seat_map: { seat_map_id: (request?.seat_map_id ?? '') } },
    );
  }

  /**
   * Returns several seat maps at once.
   */
  resolveSeatMaps(call, callback) {
    this.executeResolveSeatMaps(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveSeatMaps(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveSeatMaps', request);
    this.logger.info('resolve_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const resolveUsers1 = promisify(this.clients.identityUserQueryClient.resolveUsers.bind(this.clients.identityUserQueryClient));
    const reply1 = await resolveUsers1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      performer: {
        attributes: { ...(reply1?.result?.details ?? {}) },
        relationship_manager: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const reply2 = await call((done) => this.clients.performersPerformerClient.updatePerformer(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
        seat_maps: [{
          children: [{
            children: [{ audit: { created_by: (reply2?.performer?.audit?.created_by ?? '') } }],
          }],
        }],
      },
    );
  }

  /**
   * Lists seat maps a page at a time.
   */
  suggestSeatMaps(call, callback) {
    this.executeSuggestSeatMaps(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuggestSeatMaps(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SuggestSeatMaps', request);
    this.logger.info('suggest_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_hold: {
        location: { region: (request?.filter?.region ?? '') },
        organizer: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const method1 = 'validateSeatHold';
    const reply1 = await call((done) => this.clients.seatingSeatHoldValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      seat_maps: [{
        manager: { display_name: (reply1?.seat_hold?.organizer?.display_name ?? '') },
      }],
    };
  }

  /**
   * Summarizes a seat map.
   */
  summarizeSeatMaps(call, callback) {
    this.executeSummarizeSeatMaps(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeSeatMaps(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeSeatMaps', request);
    this.logger.info('summarize_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            children: [{ seat_map_id: (request?.seat_map_id ?? '') }],
            manager: { email: (request?.context?.actor?.email ?? '') },
          }],
        }],
      },
    };
    const client1 = this.clients.registry.get('acme.seating.v1.SeatMapCalculationService');
    const reply1 = await call((done) => client1.compareSeatMapOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      { period: request?.details?.period },
    );
  }
}

module.exports = { SeatMapQueryService };
