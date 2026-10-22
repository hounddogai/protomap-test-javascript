'use strict';

/**
 * Computes the figures of a seat map, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of SeatMapCalculationService, over the dependencies the seating team is given.
 */
function createSeatMapCalculationHandlers({ store, clients, bus, logger }) {
  /**
   * Calculates a seat map.
   */
  async function calculateSeatMap(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await store.record(request.context.tenant_id, 'CalculateSeatMap', request);
    logger.info('calculate_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_policy: {
        audit: {
          etag: (request?.seat_map?.audit?.etag ?? ''),
          updated_at: request?.seat_map?.audit?.updated_at,
        },
        owner: { email: (request?.seat_map?.manager?.email ?? '') },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.venuesVenuePolicyValidationClient.lintVenuePolicies(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.venue_policy?.audit?.created_at,
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.boxofficeBoxOfficeSaleQueryClient.suggestBoxOfficeSales(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    const request3 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => clients.toursTourStopQueryClient.summarizeTourStops(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      filter: {
        created_after: request?.seat_map?.audit?.updated_at,
        owner_id: (reply1?.outcome?.reference_id ?? ''),
        region: (request?.seat_map?.location?.region ?? ''),
      },
    };
    const reply4 = await withRetry(() => call((done) => clients.ledgerLedgerAccountClient.listLedgerAccounts(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.details?.destination?.country_code ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      {
        result: {
          correlation_id: (reply3?.result?.correlation_id ?? ''),
          message: (reply3?.result?.message ?? ''),
        },
      },
      { result: { reference_ids: (reply4?.result?.reference_ids ?? []) } },
      {
        seat_map: {
          location: request?.seat_map?.location,
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Compares a seat map.
   */
  async function compareSeatMapOptions(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await store.record(request.context.tenant_id, 'CompareSeatMapOptions', request);
    logger.info('compare_seat_map_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      wristband: {
        address: {
          country_code: (request?.seat_map?.location?.country_code ?? ''),
          lines: [...(request?.details?.destination?.lines ?? [])],
        },
        audit: { etag: (request?.seat_map?.audit?.etag ?? '') },
        owner: { email: (request?.seat_map?.manager?.email ?? '') },
      },
    };
    const task1 = deferred(() => call((done) => clients.admissionsWristbandValidationClient.checkWristbandEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      sponsor: {
        primary_address: { postal_code: (request?.seat_map?.location?.postal_code ?? '') },
      },
    };
    const reply2 = await bus.request('acme.seating.v1.SeatMapCalculationService/CompareSeatMapOptions:1', request2);
    const request3 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: {
        filters: { ...(reply2?.sponsor?.attributes ?? {}) },
        period: reply2?.sponsor?.preferences?.quiet_hours,
      },
      reason: (request?.reason ?? ''),
    };
    const reply3 = await runInContext(request.context, () => call((done) => clients.venuesVenueExportClient.scheduleVenueExport(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.details?.destination?.country_code ?? '') });
    return assemble(
      {
        seat_map: {
          children: [{
            children: [{
              location: {
                lines: ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.lines ?? []),
              },
            }],
          }],
        },
      },
      {
        seat_map: {
          children: [{
            children: [{ audit: { updated_at: reply2?.sponsor?.audit?.updated_at } }],
          }],
        },
      },
      {
        result: {
          reference_ids: (reply3?.result?.reference_ids ?? []),
          status: (reply3?.result?.status ?? ''),
        },
      },
      {
        seat_map: {
          name: (request?.seat_map?.name ?? ''),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Estimates a seat map.
   */
  async function estimateSeatMap(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await store.record(request.context.tenant_id, 'EstimateSeatMap', request);
    logger.info('estimate_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      search_index: { address: { locality: (request?.seat_map?.location?.locality ?? '') } },
    };
    const reply1 = await call((done) => clients.searchSearchIndexValidationClient.explainSearchIndexDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    let selected2_1;
    switch (reply1?.search_index?.locator) {
      case 'address':
        selected2_1 = ((reply1?.search_index?.locator === 'address' ? reply1?.search_index.address : undefined)?.recipient ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      details: {
        destination: {
          recipient: selected2_1,
          region: (request?.seat_map?.location?.region ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.admissionsAdmissionCalculationClient.compareAdmissionOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_map: {
          children: [{
            children: [{
              location: {
                postal_code: ((reply1?.search_index?.locator === 'address' ? reply1?.search_index.address : undefined)?.postal_code ?? ''),
              },
            }],
            manager: { email: (reply1?.search_index?.owner?.email ?? '') },
          }],
        },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        seat_map: {
          name: (request?.seat_map?.name ?? ''),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Explains a seat map.
   */
  async function explainSeatMapCalculation(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await store.record(request.context.tenant_id, 'ExplainSeatMapCalculation', request);
    logger.info('explain_seat_map_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            audit: { updated_by: (request?.seat_map?.audit?.updated_by ?? '') },
            manager: { user_id: (request?.seat_map?.manager?.user_id ?? '') },
          }],
        }],
      },
    };
    const explainVenueSectionDecision1 = promisify(clients.venuesVenueSectionValidationClient.explainVenueSectionDecision.bind(clients.venuesVenueSectionValidationClient));
    const reply1 = await explainVenueSectionDecision1(request1, metadataFor(request1.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_map: {
          children: [{
            children: [{ children: [{ manager: reply1?.venue_section?.manager }] }],
            location: { postal_code: (reply1?.venue_section?.location?.postal_code ?? '') },
          }],
        },
      },
      {
        seat_map: {
          attributes: (request?.seat_map?.attributes ?? {}),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a seat map.
   */
  async function getSeatMapRates(request) {
    validateContext(request.context);
    if (!request.seat_map_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
    }
    await store.record(request.context.tenant_id, 'GetSeatMapRates', request);
    logger.info('get_seat_map_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.seat_map_id ?? ''),
      },
    };
    const reply1 = await call((done) => clients.ledgerLedgerAccountClient.searchLedgerAccounts(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { locale: (request?.context?.locale ?? '') },
    };
    const method2 = 'previewGuestList';
    const reply2 = await call((done) => clients.boxofficeGuestListValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        seat_map: {
          children: [{
            location: {
              country_code: (reply2?.guest_list?.billing_address?.country_code ?? ''),
            },
          }],
        },
      },
      { seat_map: { seat_map_id: (request?.seat_map_id ?? '') } },
    );
  }

  /**
   * Quotes a seat map.
   */
  async function quoteSeatMap(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await store.record(request.context.tenant_id, 'QuoteSeatMap', request);
    logger.info('quote_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      notification: {
        assignee: { display_name: (request?.seat_map?.manager?.display_name ?? '') },
        audit: {
          created_by: (request?.seat_map?.audit?.created_by ?? ''),
          etag: (request?.seat_map?.audit?.etag ?? ''),
          updated_at: request?.seat_map?.audit?.updated_at,
        },
      },
    };
    const client1 = clients.registry.get('acme.notifications.v1.NotificationValidationService');
    const reply1 = await call((done) => client1.lintNotifications(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      search_index: {
        address: { country_code: (request?.seat_map?.location?.country_code ?? '') },
      },
    };
    const send2 = clients.searchSearchIndexValidationClient.previewSearchIndex.bind(clients.searchSearchIndexValidationClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_map: {
          children: [{
            children: [{
              manager: { display_name: (reply1?.notification?.assignee?.display_name ?? '') },
            }],
          }],
        },
      },
      {
        seat_map: {
          children: [{
            children: [{ name: (reply2?.search_index?.name ?? '') }],
            location: {
              region: ((reply2?.search_index?.locator === 'address' ? reply2?.search_index.address : undefined)?.region ?? ''),
            },
          }],
        },
      },
      {
        seat_map: {
          parent_id: (request?.seat_map?.parent_id ?? ''),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Recalculates a seat map.
   */
  async function recalculateSeatMaps(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await store.record(request.context.tenant_id, 'RecalculateSeatMaps', request);
    logger.info('recalculate_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.seat_map?.location?.country_code ?? ''),
          locality: (request?.seat_map?.location?.locality ?? ''),
          postal_code: (request?.seat_map?.location?.postal_code ?? ''),
        },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.admissionsAdmissionCalculationClient.compareAdmissionOptions(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      notification: {
        assignee: { email: (input2?.seat_map?.manager?.email ?? '') },
        audit: {
          created_by: (input2?.seat_map?.audit?.created_by ?? ''),
          updated_at: input2?.seat_map?.audit?.updated_at,
        },
      },
    };
    const reply2 = await call((done) => clients.notificationsNotificationValidationClient.validateNotification(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        breakdown: { total: reply1?.breakdown?.total },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
        seat_map: {
          children: [{
            audit: { created_by: (reply2?.notification?.audit?.created_by ?? '') },
          }],
        },
      },
      {
        seat_map: {
          manager: request?.seat_map?.manager,
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  return {
    calculateSeatMap: unary(calculateSeatMap),
    compareSeatMapOptions: unary(compareSeatMapOptions),
    estimateSeatMap: unary(estimateSeatMap),
    explainSeatMapCalculation: unary(explainSeatMapCalculation),
    getSeatMapRates: unary(getSeatMapRates),
    quoteSeatMap: unary(quoteSeatMap),
    recalculateSeatMaps: unary(recalculateSeatMaps),
  };
}

module.exports = { createSeatMapCalculationHandlers };
