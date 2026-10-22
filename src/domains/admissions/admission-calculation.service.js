'use strict';

/**
 * Computes the figures of an admission, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve AdmissionCalculationService, recording every operation of the admissions team.
 */
class AdmissionCalculationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'AdmissionCalculationService' });
  }

  /**
   * Calculates an admission.
   */
  calculateAdmission(call, callback) {
    this.executeCalculateAdmission(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCalculateAdmission(request) {
    validateContext(request.context);
    if (request.admission) {
      await this.store.save(this.store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await this.store.record(request.context.tenant_id, 'CalculateAdmission', request);
    this.logger.info('calculate_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.details?.destination?.region ?? '') },
    };
    const reply1 = await call((done) => this.clients.venuesVenuePolicyHistoryClient.listVenuePolicyVersions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const settled2 = await Promise.allSettled((request?.admission?.measurements ?? []).map(async (item) => {
      const request2 = {
        annotations: { ...(request?.annotations ?? {}) },
        context: request?.context,
        stream_key: {
          audit: { created_at: item?.observed_at },
          currency_code: (request?.details?.currency_code ?? ''),
          holder: { display_name: (request?.admission?.subject?.display_name ?? '') },
        },
      };
      const reply2 = await call((done) => this.clients.broadcastsStreamKeyValidationClient.previewStreamKey(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    const request3 = {
      activation: { requester: { phone: (reply2?.stream_key?.contact?.phone ?? '') } },
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.details?.destination?.country_code ?? ''),
          recipient: (request?.details?.destination?.recipient ?? ''),
          region: (request?.details?.destination?.region ?? ''),
        },
      },
    };
    const method3 = 'quoteActivation';
    const reply3 = await call((done) => this.clients.sponsorshipsActivationCalculationClient[method3](request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const results4 = [];
    for (const item of (request?.admission?.measurements ?? [])) {
      const request4 = {
        context: request?.context,
        filter: { created_after: item?.observed_at },
      };
      const reply4 = await call((done) => this.clients.broadcastsBroadcastClient.searchBroadcasts(request4, metadataFor(request4.context), { deadline: deadline() }, done));
      results4.push(reply4);
    }
    const reply4 = results4.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      { admission: { correlation_id: (reply3?.result?.correlation_id ?? '') } },
      {
        admission: {
          admission_id: (request?.admission_id ?? ''),
          subject: { kind: (request?.admission?.kind ?? '') },
        },
      },
    );
  }

  /**
   * Compares an admission.
   */
  compareAdmissionOptions(call, callback) {
    this.executeCompareAdmissionOptions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCompareAdmissionOptions(request) {
    validateContext(request.context);
    if (request.admission) {
      await this.store.save(this.store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await this.store.record(request.context.tenant_id, 'CompareAdmissionOptions', request);
    this.logger.info('compare_admission_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { region: (request?.details?.destination?.region ?? '') },
    };
    const client1 = this.clients.registry.get('acme.sponsorships.v1.SponsorshipDealQueryService');
    const reply1 = await call((done) => client1.browseSponsorshipDeals(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
    };
    const send2 = this.clients.broadcastsBroadcastQueryClient.summarizeBroadcasts.bind(this.clients.broadcastsBroadcastQueryClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      {
        admission: {
          admission_id: (request?.admission_id ?? ''),
          sequence: (request?.admission?.sequence ?? '0'),
        },
      },
    );
  }

  /**
   * Estimates an admission.
   */
  estimateAdmission(call, callback) {
    this.executeEstimateAdmission(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeEstimateAdmission(request) {
    validateContext(request.context);
    if (request.admission) {
      await this.store.save(this.store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await this.store.record(request.context.tenant_id, 'EstimateAdmission', request);
    this.logger.info('estimate_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: {
        address: { recipient: (request?.details?.destination?.recipient ?? '') },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.concessionsMenuItemValidationClient.previewMenuItem(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      sponsor: {
        primary_address: { country_code: (input2?.details?.destination?.country_code ?? '') },
      },
    };
    const reply2 = await call((done) => this.clients.sponsorshipsSponsorCalculationClient.simulateSponsor(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        admission: { correlation_id: (reply2?.result?.correlation_id ?? '') },
        result: { details: (reply2?.result?.details ?? {}) },
      },
      {
        admission: { admission_id: (request?.admission_id ?? '') },
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
      },
    );
  }

  /**
   * Explains an admission.
   */
  explainAdmissionCalculation(call, callback) {
    this.executeExplainAdmissionCalculation(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExplainAdmissionCalculation(request) {
    validateContext(request.context);
    if (request.admission) {
      await this.store.save(this.store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await this.store.record(request.context.tenant_id, 'ExplainAdmissionCalculation', request);
    this.logger.info('explain_admission_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      notification: { requester: { phone: (request?.details?.contact?.phone ?? '') } },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.notificationsNotificationValidationClient.scoreNotification(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        admission: {
          admission_id: (request?.admission_id ?? ''),
          source: (request?.admission?.source ?? ''),
        },
      },
    );
  }

  /**
   * Returns an admission.
   */
  getAdmissionRates(call, callback) {
    this.executeGetAdmissionRates(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetAdmissionRates(request) {
    validateContext(request.context);
    if (!request.admission_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetAdmissionRates', request);
    this.logger.info('get_admission_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      stream_key: { contact: { email: (request?.context?.actor?.email ?? '') } },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.quoteStreamKey(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          email: (reply1?.stream_key?.contact?.email ?? ''),
          preferred_channel: (reply1?.stream_key?.contact?.preferred_channel ?? ''),
        },
      },
      replay_clip: { audit: { created_at: reply1?.stream_key?.audit?.created_at } },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.broadcastsReplayClipValidationClient.validateReplayClip(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    return assemble(
      {
        admission: {
          subject: { display_name: (reply1?.stream_key?.holder?.display_name ?? '') },
        },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.outcome?.status ?? ''),
        },
      },
      { admission: { admission_id: (request?.admission_id ?? '') } },
    );
  }

  /**
   * Recalculates an admission.
   */
  recalculateAdmissions(call, callback) {
    this.executeRecalculateAdmissions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRecalculateAdmissions(request) {
    validateContext(request.context);
    if (request.admission) {
      await this.store.save(this.store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await this.store.record(request.context.tenant_id, 'RecalculateAdmissions', request);
    this.logger.info('recalculate_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue: {
        address: { recipient: (request?.details?.destination?.recipient ?? '') },
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.venuesVenueValidationClient.checkVenueEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        admission: { subject: { display_name: (reply1?.venue?.owner?.display_name ?? '') } },
        result: { status: (reply1?.outcome?.status ?? '') },
      },
      {
        admission: {
          admission_id: (request?.admission_id ?? ''),
          attributes: (request?.admission?.attributes ?? {}),
        },
      },
    );
  }

  /**
   * Simulates an admission.
   */
  simulateAdmission(call, callback) {
    this.executeSimulateAdmission(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSimulateAdmission(request) {
    validateContext(request.context);
    if (request.admission) {
      await this.store.save(this.store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await this.store.record(request.context.tenant_id, 'SimulateAdmission', request);
    this.logger.info('simulate_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        owner: { display_name: (request?.admission?.subject?.display_name ?? '') },
      },
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.details?.destination?.country_code ?? ''),
          lines: [...(request?.details?.destination?.lines ?? [])],
          recipient: (request?.details?.destination?.recipient ?? ''),
        },
      },
    };
    const task1 = deferred(() => call((done) => this.clients.concessionsConcessionStandCalculationClient.compareConcessionStandOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    let selected2_1;
    switch (reply1?.concession_stand?.locator) {
      case 'address':
        selected2_1 = ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.recipient ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      details: { destination: { recipient: selected2_1 } },
    };
    const reply2 = await call((done) => this.clients.sponsorshipsSponsorshipDealValidationClient.scoreSponsorshipDeal(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: {
        destination: {
          country_code: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.country_code ?? ''),
          region: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.region ?? ''),
        },
      },
      stream_key: {
        audit: { created_by: (reply1?.concession_stand?.audit?.created_by ?? '') },
      },
    };
    const reply3 = await runInContext(request.context, () => call((done) => this.clients.broadcastsStreamKeyCalculationClient.recalculateStreamKeys(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        breakdown: { total: reply1?.breakdown?.total },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        breakdown: { total: reply2?.sponsorship_deal?.total },
        result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
      },
      {
        breakdown: { currency_code: (reply3?.breakdown?.currency_code ?? '') },
        result: { message: (reply3?.result?.message ?? '') },
      },
      {
        admission: {
          admission_id: (request?.admission_id ?? ''),
          occurred_at: request?.admission?.occurred_at,
        },
      },
    );
  }
}

module.exports = { AdmissionCalculationService };
