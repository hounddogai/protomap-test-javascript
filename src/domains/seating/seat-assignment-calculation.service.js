'use strict';

/**
 * Computes the figures of a seat assignment, such as totals, taxes, and estimates.
 */

const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve SeatAssignmentCalculationService, recording every operation of the seating team.
 */
class SeatAssignmentCalculationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'SeatAssignmentCalculationService' });
  }

  /**
   * Calculates a seat assignment.
   */
  calculateSeatAssignment(call, callback) {
    this.executeCalculateSeatAssignment(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCalculateSeatAssignment(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'CalculateSeatAssignment', request);
    this.logger.info('calculate_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            children: [{
              seat_map_id: ((request?.seat_assignment?.origin === 'customer' ? request?.seat_assignment.customer : undefined)?.seat_map_id ?? ''),
            }],
            location: { region: (request?.details?.destination?.region ?? '') },
            manager: { user_id: (request?.seat_assignment?.assignee?.user_id ?? '') },
          }],
          location: { locality: (request?.details?.destination?.locality ?? '') },
        }],
      },
    };
    const method1 = 'explainSeatMapDecision';
    const reply1 = await call((done) => this.clients.seatingSeatMapValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            children: [{
              seat_map_id: ((request?.seat_assignment?.origin === 'customer' ? request?.seat_assignment.customer : undefined)?.seat_map_id ?? ''),
            }],
            location: {
              locality: (request?.details?.destination?.locality ?? ''),
              postal_code: (request?.details?.destination?.postal_code ?? ''),
              region: (reply1?.seat_map?.location?.region ?? ''),
            },
          }],
        }],
      },
    };
    const client2 = this.clients.registry.get('acme.seating.v1.SeatMapValidationService');
    const reply2 = await call((done) => client2.checkSeatMapEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            location: { recipient: (reply1?.seat_map?.location?.recipient ?? '') },
          }],
        }],
      },
    };
    const send3 = this.clients.seatingSeatMapValidationClient.previewSeatMap.bind(this.clients.seatingSeatMapValidationClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.outcome?.status ?? '') } },
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        result: { message: (reply3?.outcome?.message ?? '') },
        seat_assignment: { assignee: { user_id: (reply3?.seat_map?.manager?.user_id ?? '') } },
      },
      {
        seat_assignment: {
          requester: request?.seat_assignment?.requester,
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }

  /**
   * Compares a seat assignment.
   */
  compareSeatAssignmentOptions(call, callback) {
    this.executeCompareSeatAssignmentOptions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCompareSeatAssignmentOptions(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'CompareSeatAssignmentOptions', request);
    this.logger.info('compare_seat_assignment_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: {
        billing_address: { country_code: (request?.details?.destination?.country_code ?? '') },
      },
      context: request?.context,
      details: { contact: { name: request?.seat_assignment?.requester?.name } },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.concessionsConcessionOrderValidationClient.explainConcessionOrderDecision(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      seat_hold: {
        participants: [{
          contact: {
            email: (input2?.seat_assignment?.assignee?.email ?? ''),
            name: input2?.seat_assignment?.requester?.name,
          },
        }],
      },
    };
    const reply2 = await call((done) => this.clients.seatingSeatHoldCalculationClient.calculateSeatHold(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            location: {
              country_code: (reply1?.concession_order?.billing_address?.country_code ?? ''),
              region: (request?.details?.destination?.region ?? ''),
            },
          }],
        }],
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => this.clients.venuesVenueSectionSyncClient.syncVenueSections(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      document: {
        audit: {
          etag: (request?.seat_assignment?.audit?.etag ?? ''),
          updated_by: (reply2?.seat_hold?.audit?.updated_by ?? ''),
        },
        bank: {
          bic: ((reply1?.concession_order?.settlement === 'bank' ? reply1?.concession_order.bank : undefined)?.bic ?? ''),
        },
        billing_address: { recipient: (reply3?.venue_section?.location?.recipient ?? '') },
      },
    };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => this.clients.documentsDocumentClient.createDocument(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          details: (reply1?.result?.details ?? {}),
        },
      },
      {
        seat_assignment: {
          assignee: { display_name: (reply2?.seat_hold?.organizer?.display_name ?? '') },
        },
      },
      {
        result: { message: (reply3?.result?.message ?? '') },
        seat_assignment: { audit: { etag: (reply3?.venue_section?.audit?.etag ?? '') } },
      },
      {
        result: {
          correlation_id: (reply4?.result?.correlation_id ?? ''),
          message: (reply4?.result?.message ?? ''),
        },
      },
      {
        seat_assignment: {
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
          status: (request?.seat_assignment?.status ?? ''),
        },
      },
    );
  }

  /**
   * Estimates a seat assignment.
   */
  estimateSeatAssignment(call, callback) {
    this.executeEstimateSeatAssignment(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEstimateSeatAssignment(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'EstimateSeatAssignment', request);
    this.logger.info('estimate_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      metric_series: {
        address: { postal_code: (request?.details?.destination?.postal_code ?? '') },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.metricsMetricSeriesClient.createMetricSeries(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_assignment: { assignee: { email: (reply1?.metric_series?.owner?.email ?? '') } },
      },
      {
        seat_assignment: {
          priority: (request?.seat_assignment?.priority ?? 0),
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }

  /**
   * Explains a seat assignment.
   */
  explainSeatAssignmentCalculation(call, callback) {
    this.executeExplainSeatAssignmentCalculation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainSeatAssignmentCalculation(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'ExplainSeatAssignmentCalculation', request);
    this.logger.info('explain_seat_assignment_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: {
              created_at: request?.seat_assignment?.audit?.created_at,
              created_by: (request?.seat_assignment?.audit?.created_by ?? ''),
            },
            manager: { email: (request?.seat_assignment?.assignee?.email ?? '') },
          }],
        }],
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.seatingSeatMapValidationClient.previewSeatMap(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: {
        recipient: {
          email: (request?.seat_assignment?.requester?.email ?? ''),
          name: request?.details?.contact?.name,
          phone: (request?.seat_assignment?.requester?.phone ?? ''),
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const task2 = deferred(() => call((done) => this.clients.venuesVenuePolicyNotificationClient.sendVenuePolicyTestNotice(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    const request3 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: { updated_by: (request?.seat_assignment?.audit?.updated_by ?? '') },
            location: {
              country_code: (reply1?.seat_map?.location?.country_code ?? ''),
              locality: (reply1?.seat_map?.location?.locality ?? ''),
            },
            manager: { display_name: (reply2?.venue_policy?.owner?.display_name ?? '') },
          }],
        }],
      },
    };
    const reply3 = await this.bus.request('acme.seating.v1.SeatAssignmentCalculationService/ExplainSeatAssignmentCalculation:2', request3);
    const request4 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: { created_by: (reply2?.venue_policy?.audit?.created_by ?? '') },
            manager: { user_id: (reply3?.seat_map?.manager?.user_id ?? '') },
          }],
        }],
      },
    };
    const reply4 = await runInContext(request.context, () => call((done) => this.clients.seatingSeatMapValidationClient.lintSeatMaps(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { reference_id: (reply1?.outcome?.reference_id ?? '') } },
      {
        result: { details: (reply2?.result?.details ?? {}) },
        seat_assignment: { comments: [{ created_at: reply2?.venue_policy?.audit?.created_at }] },
      },
      {
        seat_assignment: { assignee: { email: (reply3?.seat_map?.manager?.email ?? '') } },
      },
      { outcome: { effective_at: reply4?.outcome?.effective_at } },
      {
        seat_assignment: {
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
          subject: (request?.seat_assignment?.subject ?? ''),
        },
      },
    );
  }

  /**
   * Quotes a seat assignment.
   */
  quoteSeatAssignment(call, callback) {
    this.executeQuoteSeatAssignment(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeQuoteSeatAssignment(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'QuoteSeatAssignment', request);
    this.logger.info('quote_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: { created_at: request?.seat_assignment?.audit?.created_at },
            location: { lines: [...(request?.details?.destination?.lines ?? [])] },
            manager: {
              display_name: ((request?.seat_assignment?.origin === 'customer' ? request?.seat_assignment.customer : undefined)?.display_name ?? ''),
            },
          }],
        }],
      },
    };
    const reply1 = await call((done) => this.clients.seatingSeatMapValidationClient.lintSeatMaps(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          email: (request?.seat_assignment?.requester?.email ?? ''),
          name: request?.seat_assignment?.requester?.name,
        },
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      this.clients.admissionsAdmissionValidationClient.previewAdmission(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const results3 = [];
    for (const item of (request?.seat_assignment?.comments ?? [])) {
      const request3 = {
        concession_order: { audit: { created_at: item?.created_at } },
        context: request?.context,
        details: { contact: { email: (item?.author_email ?? '') } },
      };
      const reply3 = await call((done) => this.clients.concessionsConcessionOrderValidationClient.validateConcessionOrder(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      results3.push(reply3);
    }
    const reply3 = results3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
        seat_assignment: { comments: [{ created_at: reply1?.seat_map?.audit?.created_at }] },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        seat_assignment: {
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
          subject: (request?.seat_assignment?.subject ?? ''),
        },
      },
    );
  }

  /**
   * Recalculates a seat assignment.
   */
  recalculateSeatAssignments(call, callback) {
    this.executeRecalculateSeatAssignments(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecalculateSeatAssignments(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'RecalculateSeatAssignments', request);
    this.logger.info('recalculate_seat_assignments', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: {
        billing_address: { region: (request?.details?.destination?.region ?? '') },
      },
      context: request?.context,
      details: {
        contact: { email: (request?.seat_assignment?.requester?.email ?? '') },
      },
    };
    const explainConcessionOrderDecision1 = promisify(this.clients.concessionsConcessionOrderValidationClient.explainConcessionOrderDecision.bind(this.clients.concessionsConcessionOrderValidationClient));
    const reply1 = await explainConcessionOrderDecision1(request1, metadataFor(request1.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        seat_assignment: {
          custom_fields: (request?.seat_assignment?.custom_fields ?? {}),
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }

  /**
   * Simulates a seat assignment.
   */
  simulateSeatAssignment(call, callback) {
    this.executeSimulateSeatAssignment(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSimulateSeatAssignment(request) {
    validateContext(request.context);
    if (request.seat_assignment) {
      await this.store.save(this.store.toSeatAssignmentRecord(request.context.tenant_id, request.seat_assignment));
    }
    await this.store.record(request.context.tenant_id, 'SimulateSeatAssignment', request);
    this.logger.info('simulate_seat_assignment', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.seat_assignment?.history ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        filter: {
          created_after: item?.changed_at,
          region: (request?.details?.destination?.region ?? ''),
        },
      };
      const reply1 = await call((done) => this.clients.venuesVenueNotificationClient.listVenueSubscriptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      details: { recipient: { name: request?.seat_assignment?.requester?.name } },
    };
    const reply2 = await call((done) => this.clients.venuesVenueNotificationClient.unsubscribeVenueUpdates(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { message: (reply2?.outcome?.message ?? '') },
        seat_assignment: { audit: { created_by: (reply2?.venue?.audit?.created_by ?? '') } },
      },
      {
        seat_assignment: {
          requester: request?.seat_assignment?.requester,
          seat_assignment_id: (request?.seat_assignment_id ?? ''),
        },
      },
    );
  }
}

module.exports = { SeatAssignmentCalculationService };
