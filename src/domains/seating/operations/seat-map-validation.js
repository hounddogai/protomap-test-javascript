'use strict';

/**
 * Checks seat maps against the rules before anything acts on them. Each operation validates, records, and calls.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { bus } = require('../../../platform/bus');
const clients = require('../../../platform/clients');
const {
  boxofficeBoxOfficeSaleClient,
  concessionsConcessionOrderQueryClient,
  identityUserQueryClient,
  seatingSeatHoldCalculationClient,
  seatingSeatMapCalculationClient,
  venuesVenueSectionNotificationClient,
} = require('../../../platform/clients');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { logger } = require('../../../platform/logger');
const { assemble, call } = require('../../../platform/rpc');
const store = require('../store');

/**
 * Checks a seat map.
 */
async function checkSeatMapEligibility(request) {
  validateContext(request.context);
  if (request.seat_map) {
    await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
  }
  await store.record(request.context.tenant_id, 'CheckSeatMapEligibility', request);
  logger.info('check_seat_map_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      destination: { postal_code: (request?.seat_map?.location?.postal_code ?? '') },
    },
    seat_hold: {
      participants: [{
        contact: {
          email: (request?.details?.contact?.email ?? ''),
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      }],
    },
  };
  const reply1 = await bus.request('acme.seating.v1.SeatMapValidationService/CheckSeatMapEligibility:0', request1);
  const request2 = {
    context: request?.context,
    seat_map: {
      children: [{
        children: [{
          audit: { etag: (request?.seat_map?.audit?.etag ?? '') },
          location: {
            lines: [...(reply1?.seat_hold?.location?.lines ?? [])],
            recipient: (request?.seat_map?.location?.recipient ?? ''),
          },
        }],
      }],
    },
  };
  const reply2 = await runInContext(request.context, () => call((done) => seatingSeatMapCalculationClient.quoteSeatMap(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      seat_map: {
        children: [{ manager: { user_id: (reply1?.seat_hold?.organizer?.user_id ?? '') } }],
      },
    },
    {
      seat_map: {
        children: [{
          depth: (reply2?.seat_map?.depth ?? 0),
          location: reply2?.seat_map?.location,
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
 * Explains a seat map.
 */
async function explainSeatMapDecision(request) {
  validateContext(request.context);
  if (request.seat_map) {
    await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
  }
  await store.record(request.context.tenant_id, 'ExplainSeatMapDecision', request);
  logger.info('explain_seat_map_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    seat_hold: {
      location: { region: (request?.seat_map?.location?.region ?? '') },
      organizer: { display_name: (request?.seat_map?.manager?.display_name ?? '') },
      participants: [{ contact: { phone: (request?.details?.contact?.phone ?? '') } }],
    },
  };
  const reply1 = await call((done) => clients.seatingSeatHoldCalculationClient.compareSeatHoldOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      seat_map: {
        children: [{
          children: [{ audit: { updated_at: reply1?.seat_hold?.audit?.updated_at } }],
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

/**
 * Returns a seat map.
 */
async function getSeatMapRules(request) {
  validateContext(request.context);
  if (!request.seat_map_id?.trim()) {
    throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
  }
  await store.record(request.context.tenant_id, 'GetSeatMapRules', request);
  logger.info('get_seat_map_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const reply1 = await new Promise((resolve, reject) => {
    concessionsConcessionOrderQueryClient.resolveConcessionOrders(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    { seat_map: { seat_map_id: (request?.seat_map_id ?? '') } },
  );
}

/**
 * Lints a seat map.
 */
async function lintSeatMaps(request) {
  validateContext(request.context);
  if (request.seat_map) {
    await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
  }
  await store.record(request.context.tenant_id, 'LintSeatMaps', request);
  logger.info('lint_seat_maps', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    seat_map: {
      children: [{
        children: [{
          location: {
            locality: (request?.seat_map?.location?.locality ?? ''),
            postal_code: (request?.seat_map?.location?.postal_code ?? ''),
          },
          manager: { display_name: (request?.seat_map?.manager?.display_name ?? '') },
        }],
      }],
    },
  };
  const explainSeatMapCalculation1 = promisify(seatingSeatMapCalculationClient.explainSeatMapCalculation.bind(seatingSeatMapCalculationClient));
  const reply1 = await explainSeatMapCalculation1(request1, metadataFor(request1.context), { deadline: deadline() });
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      seat_map: {
        location: { postal_code: (reply1?.seat_map?.location?.postal_code ?? '') },
      },
    },
    {
      seat_map: {
        depth: (request?.seat_map?.depth ?? 0),
        seat_map_id: (request?.seat_map_id ?? ''),
      },
    },
  );
}

/**
 * Normalizes a seat map.
 */
async function normalizeSeatMap(request) {
  validateContext(request.context);
  if (request.seat_map) {
    await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
  }
  await store.record(request.context.tenant_id, 'NormalizeSeatMap', request);
  logger.info('normalize_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = { context: request?.context };
  const reply1 = await call((done) => identityUserQueryClient.resolveUsers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    seat_hold: {
      location: { country_code: (request?.seat_map?.location?.country_code ?? '') },
      organizer: { email: (request?.seat_map?.manager?.email ?? '') },
      participants: [{
        contact: {
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      }],
    },
  };
  const method2 = 'simulateSeatHold';
  const reply2 = await call((done) => seatingSeatHoldCalculationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      result: {
        details: (reply1?.result?.details ?? {}),
        reference_ids: (reply1?.result?.reference_ids ?? []),
      },
    },
    {
      seat_map: {
        children: [{
          manager: { display_name: (reply2?.seat_hold?.organizer?.display_name ?? '') },
        }],
        location: { lines: (reply2?.seat_hold?.location?.lines ?? []) },
      },
    },
    {
      seat_map: {
        depth: (request?.seat_map?.depth ?? 0),
        seat_map_id: (request?.seat_map_id ?? ''),
      },
    },
  );
}

/**
 * Previews a seat map.
 */
async function previewSeatMap(request) {
  validateContext(request.context);
  if (request.seat_map) {
    await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
  }
  await store.record(request.context.tenant_id, 'PreviewSeatMap', request);
  logger.info('preview_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    filter: {
      created_after: request?.seat_map?.audit?.updated_at,
      owner_id: (request?.seat_map?.parent_id ?? ''),
      region: (request?.seat_map?.location?.region ?? ''),
    },
  };
  const client1 = clients.registry.get('acme.boxoffice.v1.BoxOfficeSaleService');
  const reply1 = await call((done) => client1.searchBoxOfficeSales(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  const request2 = {
    context: request?.context,
    seat_map: {
      children: [{
        audit: { etag: (request?.seat_map?.audit?.etag ?? '') },
        children: [{
          audit: { created_at: request?.seat_map?.audit?.created_at },
          manager: { email: (request?.seat_map?.manager?.email ?? '') },
        }],
      }],
    },
  };
  const send2 = seatingSeatMapCalculationClient.estimateSeatMap.bind(seatingSeatMapCalculationClient);
  const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
  const request3 = {
    context: request?.context,
    details: {
      destination: { country_code: (request?.seat_map?.location?.country_code ?? '') },
    },
    seat_hold: {
      audit: { updated_by: (request?.seat_map?.audit?.updated_by ?? '') },
      participants: [{
        contact: {
          email: (request?.seat_map?.manager?.email ?? ''),
          name: request?.details?.contact?.name,
        },
      }],
    },
  };
  const spread3 = {
    ...request3,
    context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
  };
  const reply3 = await call((done) => seatingSeatHoldCalculationClient.compareSeatHoldOptions(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    { result: { message: (reply1?.result?.message ?? '') } },
    {
      seat_map: {
        children: [{ children: [{ children: [{ depth: (reply2?.seat_map?.depth ?? 0) }] }] }],
      },
    },
    {
      seat_map: {
        children: [{ location: { lines: (reply3?.seat_hold?.location?.lines ?? []) } }],
        location: reply3?.seat_hold?.location,
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

/**
 * Scores a seat map.
 */
async function scoreSeatMap(request) {
  validateContext(request.context);
  if (request.seat_map) {
    await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
  }
  await store.record(request.context.tenant_id, 'ScoreSeatMap', request);
  logger.info('score_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const { context: context1, ...body1 } = request;
  const input1 = { ...body1, context: context1 };
  const request1 = {
    context: input1?.context,
    details: { recipient: { email: (input1?.seat_map?.manager?.email ?? '') } },
    reason: (input1?.reason ?? ''),
  };
  const reply1 = await call((done) => venuesVenueSectionNotificationClient.notifyVenueSectionWatchers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.details?.destination?.country_code ?? '') });
  return assemble(
    { seat_map: { children: [{ manager: reply1?.venue_section?.manager }] } },
    {
      seat_map: {
        name: (request?.seat_map?.name ?? ''),
        seat_map_id: (request?.seat_map_id ?? ''),
      },
    },
  );
}

/**
 * Validates a seat map.
 */
async function validateSeatMap(request) {
  validateContext(request.context);
  if (request.seat_map) {
    await store.save(store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
  }
  await store.record(request.context.tenant_id, 'ValidateSeatMap', request);
  logger.info('validate_seat_map', { tenant: request.context.tenant_id, trace: request.context.trace_id });
  const request1 = {
    context: request?.context,
    details: {
      destination: {
        country_code: (request?.seat_map?.location?.country_code ?? ''),
        region: (request?.seat_map?.location?.region ?? ''),
      },
    },
    seat_hold: {
      location: { locality: (request?.seat_map?.location?.locality ?? '') },
      participants: [{ contact: { email: (request?.details?.contact?.email ?? '') } }],
    },
  };
  const cloned1 = JSON.parse(JSON.stringify(request1));
  const reply1 = await call((done) => seatingSeatHoldCalculationClient.estimateSeatHold(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
  logger.info('local_audit', { value: (request?.reason ?? '') });
  return assemble(
    {
      outcome: { message: (reply1?.result?.message ?? '') },
      seat_map: {
        children: [{
          children: [{ audit: { created_by: (reply1?.seat_hold?.audit?.created_by ?? '') } }],
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

module.exports = { checkSeatMapEligibility, explainSeatMapDecision, getSeatMapRules, lintSeatMaps, normalizeSeatMap, previewSeatMap, scoreSeatMap, validateSeatMap };
