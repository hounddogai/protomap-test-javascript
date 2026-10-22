'use strict';

/**
 * Manages seat maps: creates, reads, changes, and lists them.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { clientStream, serverStream, unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of SeatMapService, over the dependencies the seating team is given.
 */
function createSeatMapHandlers({ store, clients, bus, logger }) {
  /**
   * Takes a stream of seat maps and answers once at the end.
   */
  async function batchCreateSeatMaps(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'BatchCreateSeatMaps', request);
    logger.info('batch_create_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: ((request?.chunk === 'header' ? request.header : undefined)?.submitted_by?.preferred_channel ?? ''),
        },
      },
      ledger_account: {
        audit: {
          created_by: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.created_by ?? ''),
          etag: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? ''),
        },
        holder: {
          display_name: ((request?.chunk === 'record' ? request.record : undefined)?.manager?.display_name ?? ''),
        },
      },
    };
    const send1 = clients.ledgerLedgerAccountValidationClient.checkLedgerAccountEligibility.bind(clients.ledgerLedgerAccountValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      broadcast: {
        audit: {
          created_by: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.created_by ?? ''),
          etag: (reply1?.ledger_account?.audit?.etag ?? ''),
        },
        organizer: {
          email: ((request?.chunk === 'record' ? request.record : undefined)?.manager?.email ?? ''),
        },
        participants: [{
          contact: {
            phone: ((request?.chunk === 'header' ? request.header : undefined)?.submitted_by?.phone ?? ''),
          },
        }],
      },
      context: request?.context,
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.broadcastsBroadcastValidationClient.validateBroadcast(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.updated_by ?? '') });
    return assemble(
      { result: { message: (reply1?.outcome?.message ?? '') } },
      { result: { details: (reply2?.result?.details ?? {}) } },
    );
  }

  /**
   * Returns several seat maps at once.
   */
  async function batchGetSeatMaps(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'BatchGetSeatMaps', request);
    logger.info('batch_get_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return {};
  }

  /**
   * Creates a seat map.
   */
  async function cloneSeatMap(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await store.record(request.context.tenant_id, 'CloneSeatMap', request);
    logger.info('clone_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      annotations: { ...(input1?.context?.baggage ?? {}) },
      context: input1?.context,
      details: {
        recipient: { email: (input1?.seat_map?.manager?.email ?? '') },
        variables: { ...(input1?.seat_map?.attributes ?? {}) },
      },
    };
    const reply1 = await call((done) => clients.venuesVenuePolicyNotificationClient.notifyVenuePolicyWatchers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      seat_assignment: {
        audit: { updated_by: (request?.seat_map?.audit?.updated_by ?? '') },
        requester: { email: (reply1?.venue_policy?.owner?.email ?? '') },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.seatingSeatAssignmentCalculationClient.quoteSeatAssignment(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      seat_hold: {
        participants: [{
          contact: {
            name: reply2?.seat_assignment?.requester?.name,
            preferred_channel: (reply2?.seat_assignment?.requester?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const copied3 = structuredClone(request3);
    const reply3 = await call((done) => clients.seatingSeatHoldValidationClient.previewSeatHold(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      {
        seat_map: {
          children: [{ audit: { updated_at: reply1?.venue_policy?.audit?.updated_at } }],
        },
      },
      {
        seat_map: {
          audit: { etag: (reply2?.seat_assignment?.audit?.etag ?? '') },
          manager: { display_name: (reply2?.seat_assignment?.assignee?.display_name ?? '') },
        },
      },
      {
        seat_map: {
          children: [{ children: [{ children: [{ location: reply3?.seat_hold?.location }] }] }],
          location: { postal_code: (reply3?.seat_hold?.location?.postal_code ?? '') },
        },
      },
      {
        seat_map: {
          attributes: (request?.seat_map?.attributes ?? {}),
          status: (request?.seat_map?.status ?? ''),
        },
      },
    );
  }

  /**
   * Creates a seat map.
   */
  async function createSeatMap(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await store.record(request.context.tenant_id, 'CreateSeatMap', request);
    logger.info('create_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{ location: { locality: (request?.seat_map?.location?.locality ?? '') } }],
        }],
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.seatingSeatMapCalculationClient.calculateSeatMap(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      {
        seat_map: {
          location: { postal_code: (reply1?.seat_map?.location?.postal_code ?? '') },
        },
      },
      {
        seat_map: {
          manager: request?.seat_map?.manager,
          name: (request?.seat_map?.name ?? ''),
        },
      },
    );
  }

  /**
   * Deletes a seat map.
   */
  async function deleteSeatMap(request) {
    validateContext(request.context);
    if (!request.seat_map_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
    }
    await store.record(request.context.tenant_id, 'DeleteSeatMap', request);
    logger.info('delete_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { owner_id: (request?.seat_map_id ?? '') },
    };
    const messages1 = await collect(clients.seatingSeatHoldQueryClient.exportSeatHolds(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.etag ?? '') });
    return {};
  }

  /**
   * Returns a seat map.
   */
  async function getSeatMap(request) {
    validateContext(request.context);
    if (!request.seat_map_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
    }
    await store.record(request.context.tenant_id, 'GetSeatMap', request);
    logger.info('get_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { seat_map: { seat_map_id: (request?.seat_map_id ?? '') } };
  }

  /**
   * Lists seat maps a page at a time.
   */
  async function listSeatMaps(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListSeatMaps', request);
    logger.info('list_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Streams the changes of seat maps as they happen.
   */
  async function watchSeatMaps(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'WatchSeatMaps', request);
    logger.info('watch_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          email: (request?.subscriber?.email ?? ''),
          name: request?.subscriber?.name,
          phone: (request?.subscriber?.phone ?? ''),
        },
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.admissionsWristbandValidationClient.lintWristbands(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      seat_hold: {
        audit: { etag: (reply1?.wristband?.audit?.etag ?? '') },
        location: {
          locality: ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.locality ?? ''),
          region: ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.region ?? ''),
        },
      },
    };
    const task2 = deferred(() => call((done) => clients.seatingSeatHoldValidationClient.validateSeatHold(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
    return assemble(
      { seat_map: { audit: { etag: (reply1?.wristband?.audit?.etag ?? '') } } },
      {
        seat_map: {
          children: [{ audit: { etag: (reply2?.seat_hold?.audit?.etag ?? '') } }],
        },
      },
      {
        seat_map: {
          location: { region: (request?.filter?.region ?? '') },
          manager: { email: (request?.subscriber?.email ?? '') },
        },
      },
    );
  }

  return {
    batchCreateSeatMaps: clientStream(batchCreateSeatMaps, () => ({})),
    batchGetSeatMaps: unary(batchGetSeatMaps),
    cloneSeatMap: unary(cloneSeatMap),
    createSeatMap: unary(createSeatMap),
    deleteSeatMap: unary(deleteSeatMap),
    getSeatMap: unary(getSeatMap),
    listSeatMaps: unary(listSeatMaps),
    watchSeatMaps: serverStream(watchSeatMaps),
  };
}

module.exports = { createSeatMapHandlers };
