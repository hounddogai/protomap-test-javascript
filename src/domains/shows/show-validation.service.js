'use strict';

/**
 * Checks shows against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve ShowValidationService, recording every operation of the shows team.
 */
class ShowValidationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'ShowValidationService' });
  }

  /**
   * Checks a show.
   */
  checkShowEligibility(call, callback) {
    this.executeCheckShowEligibility(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCheckShowEligibility(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'CheckShowEligibility', request);
    this.logger.info('check_show_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        location: { locality: (request?.show?.location?.locality ?? '') },
        participants: [{
          contact: {
            preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
          },
        }],
        weekly: {
          days: [...((request?.show?.recurrence === 'weekly' ? request?.show.weekly : undefined)?.days ?? [])],
        },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.showsShowCalculationClient.recalculateShows(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      show: {
        participants: [{ contact: { phone: (input2?.details?.contact?.phone ?? '') } }],
      },
    };
    const reply2 = await call((done) => this.clients.showsShowCalculationClient.calculateShow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        show: { audit: { created_at: reply1?.show?.audit?.created_at } },
      },
      {
        show: {
          audit: { updated_by: (reply2?.show?.audit?.updated_by ?? '') },
          location: { postal_code: (reply2?.show?.location?.postal_code ?? '') },
        },
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
   * Returns a show.
   */
  getShowRules(call, callback) {
    this.executeGetShowRules(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetShowRules(request) {
    validateContext(request.context);
    if (!request.show_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'show_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetShowRules', request);
    this.logger.info('get_show_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        audit: { updated_at: request?.context?.deadline },
        organizer: { user_id: (request?.context?.actor?.user_id ?? '') },
        participants: [{ contact: { email: (request?.context?.actor?.email ?? '') } }],
        show_id: (request?.show_id ?? ''),
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.showsShowCalculationClient.compareShowOptions(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    return assemble(
      {
        show: { location: { locality: (reply1?.show?.location?.locality ?? '') } },
      },
      { show: { show_id: (request?.show_id ?? '') } },
    );
  }

  /**
   * Lints a show.
   */
  lintShows(call, callback) {
    this.executeLintShows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLintShows(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'LintShows', request);
    this.logger.info('lint_shows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.show?.audit?.updated_at,
        region: (request?.show?.location?.region ?? ''),
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.performersPerformerContractQueryClient.browsePerformerContracts(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const settled2 = await Promise.allSettled((request?.show?.participants ?? []).map(async (item) => {
      const request2 = {
        audit_event: {
          correlation_id: (item?.participant_id ?? ''),
          measurements: [{ observed_at: request?.show?.audit?.updated_at }],
          occurred_at: request?.show?.audit?.created_at,
          subject: { display_name: (request?.show?.organizer?.display_name ?? '') },
        },
        context: request?.context,
      };
      const reply2 = await call((done) => this.clients.auditAuditEventClient.cloneAuditEvent(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      filter: { region: (request?.show?.location?.region ?? '') },
    };
    const merged3 = Object.assign({}, request3, { context: request3.context });
    const reply3 = await call((done) => this.clients.auditAuditEventQueryClient.suggestAuditEvents(merged3, metadataFor(merged3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      show: {
        participants: [{ contact: { phone: (request?.details?.contact?.phone ?? '') } }],
      },
    };
    const reply4 = await withRetry(() => call((done) => this.clients.showsShowCalculationClient.compareShowOptions(request4, metadataFor(request4.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        outcome: { status: (reply3?.result?.status ?? '') },
        result: { correlation_id: (reply3?.result?.correlation_id ?? '') },
      },
      {
        show: {
          audit: { updated_at: reply4?.show?.audit?.updated_at },
          window: reply4?.show?.window,
        },
      },
      {
        show: {
          show_id: (request?.show_id ?? ''),
          status: (request?.show?.status ?? ''),
        },
      },
    );
  }

  /**
   * Previews a show.
   */
  previewShow(call, callback) {
    this.executePreviewShow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePreviewShow(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'PreviewShow', request);
    this.logger.info('preview_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        participants: [{
          contact: {
            preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const task1 = deferred(() => call((done) => this.clients.showsShowCalculationClient.calculateShow(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const results2 = [];
    for (const item of (request?.show?.participants ?? [])) {
      const request2 = {
        context: request?.context,
        sponsorship_deal: { counterparty: { email: (item?.contact?.email ?? '') } },
      };
      const reply2 = await call((done) => this.clients.sponsorshipsSponsorshipDealCalculationClient.quoteSponsorshipDeal(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      results2.push(reply2);
    }
    const reply2 = results2.at(-1) ?? {};
    const results3 = await Promise.all((request?.show?.participants ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        show: { participants: [{ contact: { phone: (item?.contact?.phone ?? '') } }] },
      };
      const reply3 = await call((done) => this.clients.showsShowCalculationClient.estimateShow(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        show: { location: { region: (reply1?.show?.location?.region ?? '') } },
      },
      {
        show: {
          metadata: (request?.show?.metadata ?? {}),
          show_id: (request?.show_id ?? ''),
        },
      },
    );
  }

  /**
   * Scores a show.
   */
  scoreShow(call, callback) {
    this.executeScoreShow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScoreShow(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'ScoreShow', request);
    this.logger.info('score_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        participants: [{ contact: { email: (request?.show?.organizer?.email ?? '') } }],
      },
    };
    const reply1 = await this.bus.request('acme.shows.v1.ShowValidationService/ScoreShow:0', request1);
    const request2 = {
      context: request?.context,
      filter: { region: (request?.show?.location?.region ?? '') },
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.auditAuditEventQueryClient.countAuditEvents(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        show: {
          participants: [{ contact: { email: (reply1?.show?.organizer?.email ?? '') } }],
        },
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          message: (reply2?.result?.message ?? ''),
        },
      },
      {
        show: {
          show_id: (request?.show_id ?? ''),
          status: (request?.show?.status ?? ''),
        },
      },
    );
  }

  /**
   * Validates a show.
   */
  validateShow(call, callback) {
    this.executeValidateShow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeValidateShow(request) {
    validateContext(request.context);
    if (request.show) {
      await this.store.save(this.store.toShowRecord(request.context.tenant_id, request.show));
    }
    await this.store.record(request.context.tenant_id, 'ValidateShow', request);
    this.logger.info('validate_show', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        participants: [{
          contact: {
            email: (request?.show?.organizer?.email ?? ''),
            name: request?.details?.contact?.name,
            preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const reply1 = await call((done) => this.clients.showsShowCalculationClient.recalculateShows(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      show: {
        participants: [{
          contact: {
            name: request?.details?.contact?.name,
            phone: (request?.details?.contact?.phone ?? ''),
          },
        }],
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      this.clients.showsShowCalculationClient.explainShowCalculation(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { show: { location: reply1?.show?.location } },
      { show: { location: reply2?.show?.location } },
      {
        show: {
          show_id: (request?.show_id ?? ''),
          title: (request?.show?.title ?? ''),
        },
      },
    );
  }
}

module.exports = { ShowValidationService };
