'use strict';

/**
 * Computes the figures of a seat hold, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve SeatHoldCalculationService, recording every operation of the seating team.
 */
class SeatHoldCalculationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'SeatHoldCalculationService' });
  }

  /**
   * Calculates a seat hold.
   */
  calculateSeatHold(call, callback) {
    this.executeCalculateSeatHold(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCalculateSeatHold(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await this.store.save(this.store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await this.store.record(request.context.tenant_id, 'CalculateSeatHold', request);
    this.logger.info('calculate_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: { created_at: request?.seat_hold?.audit?.created_at },
            location: { postal_code: (request?.details?.destination?.postal_code ?? '') },
            manager: {
              display_name: (request?.seat_hold?.organizer?.display_name ?? ''),
              user_id: (request?.seat_hold?.organizer?.user_id ?? ''),
            },
          }],
        }],
      },
    };
    const compareSeatMapOptions1 = promisify(this.clients.seatingSeatMapCalculationClient.compareSeatMapOptions.bind(this.clients.seatingSeatMapCalculationClient));
    const reply1 = await compareSeatMapOptions1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: { updated_at: request?.seat_hold?.audit?.updated_at },
            location: { lines: [...(reply1?.seat_map?.location?.lines ?? [])] },
          }],
        }],
      },
    };
    const reply2 = await call((done) => this.clients.seatingSeatMapCalculationClient.estimateSeatMap(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: { created_at: request?.seat_hold?.audit?.created_at },
            location: { locality: (request?.seat_hold?.location?.locality ?? '') },
          }],
        }],
      },
    };
    const method3 = 'quoteSeatMap';
    const reply3 = await call((done) => this.clients.seatingSeatMapCalculationClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_hold: {
          location: { recipient: (reply1?.seat_map?.location?.recipient ?? '') },
        },
      },
      {
        seat_hold: {
          audit: {
            etag: (reply2?.seat_map?.audit?.etag ?? ''),
            updated_by: (reply2?.seat_map?.audit?.updated_by ?? ''),
          },
        },
      },
      {
        seat_hold: { organizer: { email: (reply3?.seat_map?.manager?.email ?? '') } },
      },
      {
        seat_hold: {
          seat_hold_id: (request?.seat_hold_id ?? ''),
          title: (request?.seat_hold?.title ?? ''),
        },
      },
    );
  }

  /**
   * Compares a seat hold.
   */
  compareSeatHoldOptions(call, callback) {
    this.executeCompareSeatHoldOptions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCompareSeatHoldOptions(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await this.store.save(this.store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await this.store.record(request.context.tenant_id, 'CompareSeatHoldOptions', request);
    this.logger.info('compare_seat_hold_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.seat_hold?.audit?.created_at,
        labels: { ...(request?.seat_hold?.metadata ?? {}) },
        owner_id: (request?.seat_hold?.organizer?.user_id ?? ''),
        region: (request?.seat_hold?.location?.region ?? ''),
      },
    };
    const messages1 = await collect(this.clients.performersPerformerQueryClient.exportPerformers(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const results2 = await Promise.all((request?.seat_hold?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        details: {
          contact: {
            email: (item?.contact?.email ?? ''),
            name: item?.contact?.name,
            phone: (item?.contact?.phone ?? ''),
          },
        },
      };
      const reply2 = await call((done) => this.clients.sponsorshipsActivationValidationClient.validateActivation(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      sponsor: {
        audit: {
          created_by: (request?.seat_hold?.audit?.created_by ?? ''),
          etag: (request?.seat_hold?.audit?.etag ?? ''),
        },
        contact: { email: (reply2?.activation?.assignee?.email ?? '') },
        relationship_manager: { email: (request?.seat_hold?.organizer?.email ?? '') },
      },
    };
    const client3 = this.clients.registry.get('acme.sponsorships.v1.SponsorService');
    const reply3 = await call((done) => client3.cloneSponsor(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            location: { lines: [...(request?.seat_hold?.location?.lines ?? [])] },
            manager: { display_name: (reply2?.activation?.assignee?.display_name ?? '') },
          }],
        }],
      },
    };
    const send4 = this.clients.seatingSeatMapCalculationClient.quoteSeatMap.bind(this.clients.seatingSeatMapCalculationClient);
    const reply4 = await call((done) => send4(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_hold: {
          location: { postal_code: (reply3?.sponsor?.primary_address?.postal_code ?? '') },
        },
      },
      {
        breakdown: { currency_code: (reply4?.breakdown?.currency_code ?? '') },
        seat_hold: { audit: { updated_by: (reply4?.seat_map?.audit?.updated_by ?? '') } },
      },
      {
        seat_hold: {
          metadata: (request?.seat_hold?.metadata ?? {}),
          seat_hold_id: (request?.seat_hold_id ?? ''),
        },
      },
    );
  }

  /**
   * Estimates a seat hold.
   */
  estimateSeatHold(call, callback) {
    this.executeEstimateSeatHold(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEstimateSeatHold(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await this.store.save(this.store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await this.store.record(request.context.tenant_id, 'EstimateSeatHold', request);
    this.logger.info('estimate_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      box_office_sale: {
        audit: { etag: (request?.seat_hold?.audit?.etag ?? '') },
        counterparty: { display_name: (request?.seat_hold?.organizer?.display_name ?? '') },
      },
      context: request?.context,
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.boxofficeBoxOfficeSaleClient.updateBoxOfficeSale(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const settled2 = await Promise.allSettled((request?.seat_hold?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        seat_map: {
          children: [{
            children: [{
              audit: {
                created_by: (reply1?.box_office_sale?.audit?.created_by ?? ''),
                updated_by: (request?.seat_hold?.audit?.updated_by ?? ''),
              },
            }],
            manager: { email: (item?.contact?.email ?? '') },
          }],
        },
      };
      const reply2 = await call((done) => this.clients.seatingSeatMapCalculationClient.calculateSeatMap(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      context: input3?.context,
      filter: { region: (reply2?.seat_map?.location?.region ?? '') },
    };
    const reply3 = await call((done) => this.clients.venuesVenueSectionQueryClient.browseVenueSections(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_hold: {
          audit: {
            etag: (reply1?.box_office_sale?.audit?.etag ?? ''),
            updated_by: (reply1?.box_office_sale?.audit?.updated_by ?? ''),
          },
        },
      },
      { result: { reference_ids: (reply3?.result?.reference_ids ?? []) } },
      {
        seat_hold: {
          seat_hold_id: (request?.seat_hold_id ?? ''),
          window: request?.seat_hold?.window,
        },
      },
    );
  }

  /**
   * Explains a seat hold.
   */
  explainSeatHoldCalculation(call, callback) {
    this.executeExplainSeatHoldCalculation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainSeatHoldCalculation(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await this.store.save(this.store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await this.store.record(request.context.tenant_id, 'ExplainSeatHoldCalculation', request);
    this.logger.info('explain_seat_hold_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{ manager: { user_id: (request?.seat_hold?.organizer?.user_id ?? '') } }],
        }],
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.seatingSeatMapCalculationClient.recalculateSeatMaps(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const results2 = [];
    for (const item of (request?.seat_hold?.participants ?? [])) {
      const request2 = {
        context: request?.context,
        details: { contact: { name: item?.contact?.name } },
        venue: { owner: { email: (item?.contact?.email ?? '') } },
      };
      const reply2 = await call((done) => this.clients.venuesVenueValidationClient.validateVenue(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      results2.push(reply2);
    }
    const reply2 = results2.at(-1) ?? {};
    const results3 = await Promise.all((request?.seat_hold?.participants ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        details: {
          contact: {
            name: item?.contact?.name,
            phone: (item?.contact?.phone ?? ''),
          },
        },
        seat_map: { children: [{ manager: { email: (item?.contact?.email ?? '') } }] },
      };
      const reply3 = await call((done) => this.clients.seatingSeatMapCalculationClient.explainSeatMapCalculation(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_hold: { organizer: { email: (reply1?.seat_map?.manager?.email ?? '') } },
      },
      {
        seat_hold: {
          seat_hold_id: (request?.seat_hold_id ?? ''),
          title: (request?.seat_hold?.title ?? ''),
        },
      },
    );
  }

  /**
   * Returns a seat hold.
   */
  getSeatHoldRates(call, callback) {
    this.executeGetSeatHoldRates(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetSeatHoldRates(request) {
    validateContext(request.context);
    if (!request.seat_hold_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_hold_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetSeatHoldRates', request);
    this.logger.info('get_seat_hold_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { seat_hold: { seat_hold_id: (request?.seat_hold_id ?? '') } };
  }

  /**
   * Quotes a seat hold.
   */
  quoteSeatHold(call, callback) {
    this.executeQuoteSeatHold(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeQuoteSeatHold(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await this.store.save(this.store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await this.store.record(request.context.tenant_id, 'QuoteSeatHold', request);
    this.logger.info('quote_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{ audit: { created_at: request?.seat_hold?.audit?.created_at } }],
        }],
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.seatingSeatMapCalculationClient.recalculateSeatMaps(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: {
              etag: (reply1?.seat_map?.audit?.etag ?? ''),
              updated_by: (reply1?.seat_map?.audit?.updated_by ?? ''),
            },
            manager: { email: (reply1?.seat_map?.manager?.email ?? '') },
          }],
        }],
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.seatingSeatMapCalculationClient.explainSeatMapCalculation(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      { seat_hold: { audit: { etag: (reply2?.seat_map?.audit?.etag ?? '') } } },
      {
        seat_hold: {
          seat_hold_id: (request?.seat_hold_id ?? ''),
          window: request?.seat_hold?.window,
        },
      },
    );
  }

  /**
   * Recalculates a seat hold.
   */
  recalculateSeatHolds(call, callback) {
    this.executeRecalculateSeatHolds(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecalculateSeatHolds(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await this.store.save(this.store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await this.store.record(request.context.tenant_id, 'RecalculateSeatHolds', request);
    this.logger.info('recalculate_seat_holds', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsorship_deal: {
        billing_address: { locality: (request?.details?.destination?.locality ?? '') },
        counterparty: { email: (request?.seat_hold?.organizer?.email ?? '') },
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.sponsorshipsSponsorshipDealValidationClient.validateSponsorshipDeal(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      box_office_sale: {
        billing_address: { country_code: (request?.seat_hold?.location?.country_code ?? '') },
      },
      context: request?.context,
    };
    const task2 = deferred(() => call((done) => this.clients.boxofficeBoxOfficeSaleClient.cloneBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_hold: {
          audit: { updated_by: (reply1?.sponsorship_deal?.audit?.updated_by ?? '') },
        },
      },
      {
        seat_hold: {
          audit: { etag: (reply2?.box_office_sale?.audit?.etag ?? '') },
          location: {
            recipient: (reply2?.box_office_sale?.billing_address?.recipient ?? ''),
          },
        },
      },
      {
        seat_hold: {
          organizer: request?.seat_hold?.organizer,
          seat_hold_id: (request?.seat_hold_id ?? ''),
        },
      },
    );
  }

  /**
   * Simulates a seat hold.
   */
  simulateSeatHold(call, callback) {
    this.executeSimulateSeatHold(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSimulateSeatHold(request) {
    validateContext(request.context);
    if (request.seat_hold) {
      await this.store.save(this.store.toSeatHoldRecord(request.context.tenant_id, request.seat_hold));
    }
    await this.store.record(request.context.tenant_id, 'SimulateSeatHold', request);
    this.logger.info('simulate_seat_hold', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue: {
        address: {
          locality: (request?.details?.destination?.locality ?? ''),
          region: (request?.details?.destination?.region ?? ''),
        },
        owner: { user_id: (request?.seat_hold?.organizer?.user_id ?? '') },
      },
    };
    const reply1 = await this.bus.request('acme.seating.v1.SeatHoldCalculationService/SimulateSeatHold:0', request1);
    const settled2 = await Promise.allSettled((request?.seat_hold?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        sponsorship_deal: { counterparty: { email: (item?.contact?.email ?? '') } },
      };
      const reply2 = await call((done) => this.clients.sponsorshipsSponsorshipDealCalculationClient.compareSponsorshipDealOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            location: {
              recipient: ((reply1?.venue?.locator === 'address' ? reply1?.venue.address : undefined)?.recipient ?? ''),
              region: (reply2?.sponsorship_deal?.billing_address?.region ?? ''),
            },
          }],
        }],
      },
    };
    const reply3 = await runInContext(request.context, () => call((done) => this.clients.venuesVenueSectionSyncClient.syncVenueSections(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const request4 = {
      context: request?.context,
      menu_item: { audit: { created_at: reply2?.sponsorship_deal?.audit?.created_at } },
    };
    const reply4 = await call((done) => this.clients.concessionsMenuItemValidationClient.validateMenuItem(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { message: (reply1?.outcome?.message ?? '') },
        seat_hold: { audit: { updated_by: (reply1?.venue?.audit?.updated_by ?? '') } },
      },
      {
        result: { message: (reply3?.result?.message ?? '') },
        seat_hold: {
          participants: [{ contact: { email: (reply3?.venue_section?.manager?.email ?? '') } }],
        },
      },
      {
        result: {
          details: (reply4?.result?.details ?? {}),
          reference_ids: (reply4?.result?.reference_ids ?? []),
        },
      },
      {
        seat_hold: {
          metadata: (request?.seat_hold?.metadata ?? {}),
          seat_hold_id: (request?.seat_hold_id ?? ''),
        },
      },
    );
  }
}

module.exports = { SeatHoldCalculationService };
