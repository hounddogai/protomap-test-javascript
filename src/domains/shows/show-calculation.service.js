'use strict';

/**
 * Computes the figures of a show, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve ShowCalculationService, recording every operation of the shows team.
 */
class ShowCalculationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'ShowCalculationService' });
  }

  /**
   * Calculates a show.
   */
  calculateShow(call, callback) {
    this.executeCalculateShow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCalculateShow(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'CalculateShow', request);
    this.logger.info('calculate_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        subject: { display_name: (request?.show?.organizer?.display_name ?? '') },
      },
      context: request?.context,
    };
    const reply1 = await call((done) => this.clients.admissionsAdmissionClient.cloneAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      notification: {
        customer: { display_name: (reply1?.admission?.subject?.display_name ?? '') },
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      this.clients.notificationsNotificationValidationClient.previewNotification(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request3 = {
      context: request?.context,
      notification: {
        customer: { display_name: (reply1?.admission?.subject?.display_name ?? '') },
        requester: { email: (request?.show?.organizer?.email ?? '') },
        sla: { due_at: reply2?.notification?.sla?.due_at },
      },
    };
    const scoreNotification3 = promisify(this.clients.notificationsNotificationValidationClient.scoreNotification.bind(this.clients.notificationsNotificationValidationClient));
    const reply3 = await scoreNotification3(request3, metadataFor(request3.context), { deadline: deadline() });
    const request4 = {
      context: request?.context,
      venue_policy: {
        audit: { updated_at: reply2?.notification?.audit?.updated_at },
        owner: {
          email: (reply3?.notification?.requester?.email ?? ''),
          user_id: (reply2?.notification?.assignee?.user_id ?? ''),
        },
      },
    };
    const reply4 = await call((done) => this.clients.venuesVenuePolicySyncClient.resolveVenuePolicyConflict(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        show: {
          organizer: { user_id: (reply2?.notification?.assignee?.user_id ?? '') },
          participants: [{ contact: { phone: (reply2?.notification?.requester?.phone ?? '') } }],
        },
      },
      {
        show: {
          participants: [{ contact: { email: (reply3?.notification?.assignee?.email ?? '') } }],
        },
      },
      {
        result: { details: (reply4?.result?.details ?? {}) },
        show: { audit: { created_at: reply4?.venue_policy?.audit?.created_at } },
      },
      {
        show: {
          show_id: (request?.show_id ?? ''),
          window: request?.show?.window,
        },
      },
    );
  }

  /**
   * Compares a show.
   */
  compareShowOptions(call, callback) {
    this.executeCompareShowOptions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCompareShowOptions(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'CompareShowOptions', request);
    this.logger.info('compare_show_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      document: {
        audit: { updated_at: request?.show?.audit?.updated_at },
        billing_address: { postal_code: (request?.show?.location?.postal_code ?? '') },
        counterparty: { display_name: (request?.show?.organizer?.display_name ?? '') },
      },
    };
    const method1 = 'previewDocument';
    const reply1 = await call((done) => this.clients.documentsDocumentValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      venue_policy: {
        audit: {
          created_at: reply1?.document?.audit?.created_at,
          created_by: (request?.show?.audit?.created_by ?? ''),
          updated_by: (reply1?.document?.audit?.updated_by ?? ''),
        },
        owner: { display_name: (reply1?.document?.counterparty?.display_name ?? '') },
      },
    };
    const client2 = this.clients.registry.get('acme.venues.v1.VenuePolicyValidationService');
    const reply2 = await call((done) => client2.lintVenuePolicies(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.document?.audit?.updated_at,
        owner_id: (reply2?.venue_policy?.owner?.user_id ?? ''),
        region: (reply1?.document?.billing_address?.region ?? ''),
      },
    };
    const send3 = this.clients.documentsDocumentClient.listDocuments.bind(this.clients.documentsDocumentClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { status: (reply1?.outcome?.status ?? '') },
        show: {
          organizer: { display_name: (reply1?.document?.counterparty?.display_name ?? '') },
        },
      },
      {
        show: {
          audit: { updated_at: reply2?.venue_policy?.audit?.updated_at },
          organizer: { user_id: (reply2?.venue_policy?.owner?.user_id ?? '') },
        },
      },
      {
        result: {
          details: (reply3?.result?.details ?? {}),
          message: (reply3?.result?.message ?? ''),
        },
      },
      {
        show: {
          location: request?.show?.location,
          show_id: (request?.show_id ?? ''),
        },
      },
    );
  }

  /**
   * Estimates a show.
   */
  estimateShow(call, callback) {
    this.executeEstimateShow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEstimateShow(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'EstimateShow', request);
    this.logger.info('estimate_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.show?.audit?.updated_at,
        owner_id: (request?.show?.organizer?.user_id ?? ''),
        region: (request?.details?.destination?.region ?? ''),
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.broadcastsBroadcastQueryClient.countBroadcasts(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      filter: {
        created_after: input2?.show?.audit?.updated_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (input2?.show?.organizer?.user_id ?? ''),
        region: (input2?.details?.destination?.region ?? ''),
      },
    };
    const reply2 = await call((done) => this.clients.identityUserQueryClient.browseUsers(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: { recipient: { email: (request?.show?.organizer?.email ?? '') } },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => this.clients.venuesVenueNotificationClient.notifyVenueWatchers(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      filter: {
        region: ((reply3?.venue?.locator === 'address' ? reply3?.venue.address : undefined)?.region ?? ''),
      },
    };
    const messages4 = await collect(this.clients.broadcastsBroadcastQueryClient.exportBroadcasts(request4, metadataFor(request4.context), { deadline: deadline() }));
    const reply4 = messages4.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          details: (reply1?.result?.details ?? {}),
        },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      { show: { audit: { created_at: reply3?.venue?.audit?.created_at } } },
      {
        show: {
          show_id: (request?.show_id ?? ''),
          window: request?.show?.window,
        },
      },
    );
  }

  /**
   * Explains a show.
   */
  explainShowCalculation(call, callback) {
    this.executeExplainShowCalculation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainShowCalculation(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'ExplainShowCalculation', request);
    this.logger.info('explain_show_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.concessionsConcessionStandQueryClient.summarizeConcessionStands(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.contact?.preferred_channel ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      {
        show: {
          metadata: (request?.show?.metadata ?? {}),
          show_id: (request?.show_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a show.
   */
  getShowRates(call, callback) {
    this.executeGetShowRates(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetShowRates(request) {
    validateContext(request.context);
    if (!request.show_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'show_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetShowRates', request);
    this.logger.info('get_show_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { show: { show_id: (request?.show_id ?? '') } };
  }

  /**
   * Quotes a show.
   */
  quoteShow(call, callback) {
    this.executeQuoteShow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeQuoteShow(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'QuoteShow', request);
    this.logger.info('quote_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.show?.location?.region ?? '') },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.toursTourStopQueryClient.findTourStops(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        show: {
          organizer: request?.show?.organizer,
          show_id: (request?.show_id ?? ''),
        },
      },
    );
  }

  /**
   * Recalculates a show.
   */
  recalculateShows(call, callback) {
    this.executeRecalculateShows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecalculateShows(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'RecalculateShows', request);
    this.logger.info('recalculate_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.show?.participants ?? []).map(async (item) => {
      const request1 = {
        concession_stand: {
          address: { lines: [...(request?.show?.location?.lines ?? [])] },
          owner: {
            display_name: (request?.show?.organizer?.display_name ?? ''),
            email: (item?.contact?.email ?? ''),
          },
        },
        context: request?.context,
      };
      const reply1 = await call((done) => this.clients.concessionsConcessionStandClient.cloneConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    const results2 = [];
    for (const item of (request?.show?.participants ?? [])) {
      const request2 = {
        context: request?.context,
        notification: {
          requester: {
            email: (item?.contact?.email ?? ''),
            phone: (item?.contact?.phone ?? ''),
          },
        },
      };
      const reply2 = await call((done) => this.clients.notificationsNotificationValidationClient.normalizeNotification(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      results2.push(reply2);
    }
    const reply2 = results2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      details: { contact: { email: (reply2?.notification?.requester?.email ?? '') } },
      rider: { audit: { updated_by: (request?.show?.audit?.updated_by ?? '') } },
    };
    const reply3 = await withRetry(() => call((done) => this.clients.performersRiderValidationClient.normalizeRider(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply3?.result?.details ?? {}) } },
      {
        show: {
          show_id: (request?.show_id ?? ''),
          status: (request?.show?.status ?? ''),
        },
      },
    );
  }

  /**
   * Simulates a show.
   */
  simulateShow(call, callback) {
    this.executeSimulateShow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSimulateShow(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'SimulateShow', request);
    this.logger.info('simulate_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_order: { billing_address: { region: (request?.show?.location?.region ?? '') } },
      context: request?.context,
    };
    const task1 = deferred(() => call((done) => this.clients.concessionsConcessionOrderValidationClient.scoreConcessionOrder(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const results2 = await Promise.all((request?.show?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        details: {
          contact: {
            email: (item?.contact?.email ?? ''),
            phone: (item?.contact?.phone ?? ''),
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply2 = await call((done) => this.clients.searchSearchIndexValidationClient.checkSearchIndexEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    let selected3_4;
    switch (reply2?.search_index?.locator) {
      case 'address':
        selected3_4 = ((reply2?.search_index?.locator === 'address' ? reply2?.search_index.address : undefined)?.recipient ?? '');
        break;
      default:
        selected3_4 = '';
    }
    const request3 = {
      admission: {
        measurements: [{ unit: (reply2?.search_index?.dimensions?.unit ?? '') }],
      },
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.details?.destination?.country_code ?? ''),
          recipient: selected3_4,
          region: (request?.details?.destination?.region ?? ''),
        },
      },
    };
    const reply3 = await call((done) => this.clients.admissionsAdmissionCalculationClient.simulateAdmission(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        result: {
          reference_ids: (reply3?.result?.reference_ids ?? []),
          status: (reply3?.result?.status ?? ''),
        },
      },
      {
        show: {
          location: request?.show?.location,
          show_id: (request?.show_id ?? ''),
        },
      },
    );
  }
}

module.exports = { ShowCalculationService };
