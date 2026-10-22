'use strict';

/**
 * Moves a concession stand between the states of its lifecycle.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve ConcessionStandLifecycleService, recording every operation of the concessions team.
 */
class ConcessionStandLifecycleService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'ConcessionStandLifecycleService' });
  }

  /**
   * Activates a concession stand.
   */
  activateConcessionStand(call, callback) {
    this.executeActivateConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeActivateConcessionStand(request) {
    validateContext(request.context);
    if (!request.concession_stand_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
    }
    await this.store.record(request.context.tenant_id, 'ActivateConcessionStand', request);
    this.logger.info('activate_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: {
        participants: [{
          contact: {
            name: request?.details?.notify_contact?.name,
            preferred_channel: (request?.details?.notify_contact?.preferred_channel ?? ''),
          },
        }],
      },
      context: request?.context,
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.broadcastsBroadcastApprovalClient.delegateBroadcastApproval(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_order: {
        billing_address: {
          lines: [...(reply1?.broadcast?.location?.lines ?? [])],
          recipient: (reply1?.broadcast?.location?.recipient ?? ''),
          region: (reply1?.broadcast?.location?.region ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: { country_code: (reply1?.broadcast?.location?.country_code ?? '') },
      },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.concessionsConcessionOrderCalculationClient.calculateConcessionOrder(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply2?.concession_order?.audit?.created_at,
        region: (reply1?.broadcast?.location?.region ?? ''),
      },
    };
    const task3 = deferred(() => call((done) => this.clients.concessionsConcessionOrderQueryClient.findConcessionOrders(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          message: (reply1?.outcome?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        concession_stand: {
          audit: { updated_by: (reply2?.concession_order?.audit?.updated_by ?? '') },
        },
      },
      { result: { status: (reply3?.result?.status ?? '') } },
      {
        concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Expires a concession stand.
   */
  expireConcessionStand(call, callback) {
    this.executeExpireConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeExpireConcessionStand(request) {
    validateContext(request.context);
    if (!request.concession_stand_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
    }
    await this.store.record(request.context.tenant_id, 'ExpireConcessionStand', request);
    this.logger.info('expire_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        owner: { display_name: (request?.details?.new_owner?.display_name ?? '') },
      },
      context: request?.context,
      details: {
        contact: {
          name: request?.details?.notify_contact?.name,
          phone: (request?.details?.notify_contact?.phone ?? ''),
        },
      },
    };
    const reply1 = await this.bus.request('acme.concessions.v1.ConcessionStandLifecycleService/ExpireConcessionStand:0', request1);
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.concession_stand?.audit?.updated_at,
        owner_id: (reply1?.result?.correlation_id ?? ''),
        region: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.region ?? ''),
      },
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.notificationsNotificationClient.searchNotifications(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: {
          audit: { updated_at: reply1?.concession_stand?.audit?.updated_at },
          dimensions: { width: (reply1?.concession_stand?.dimensions?.width ?? 0) },
        },
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Returns a concession stand.
   */
  getConcessionStandLifecycle(call, callback) {
    this.executeGetConcessionStandLifecycle(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetConcessionStandLifecycle(request) {
    validateContext(request.context);
    if (!request.concession_stand_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetConcessionStandLifecycle', request);
    this.logger.info('get_concession_stand_lifecycle', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        concession_stand_id: (request?.concession_stand_id ?? ''),
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
      context: request?.context,
    };
    const reply1 = await call((done) => this.clients.concessionsConcessionStandCalculationClient.quoteConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    let selected2_1;
    switch (reply1?.concession_stand?.locator) {
      case 'address':
        selected2_1 = ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.region ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.concession_stand?.audit?.created_at,
        region: selected2_1,
      },
    };
    const reply2 = await call((done) => this.clients.sponsorshipsSponsorApprovalClient.listPendingSponsors(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      { concession_stand: { name: (reply1?.concession_stand?.name ?? '') } },
      { result: { details: (reply2?.result?.details ?? {}) } },
      {
        concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
      },
    );
  }

  /**
   * Reinstates a concession stand.
   */
  reinstateConcessionStand(call, callback) {
    this.executeReinstateConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeReinstateConcessionStand(request) {
    validateContext(request.context);
    if (!request.concession_stand_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
    }
    await this.store.record(request.context.tenant_id, 'ReinstateConcessionStand', request);
    this.logger.info('reinstate_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { created_after: request?.details?.effective_at },
    };
    const browseDemandForecasts1 = promisify(this.clients.forecastingDemandForecastQueryClient.browseDemandForecasts.bind(this.clients.forecastingDemandForecastQueryClient));
    const reply1 = await browseDemandForecasts1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      concession_stand_id: (request?.concession_stand_id ?? ''),
      context: request?.context,
    };
    const reply2 = await call((done) => this.clients.concessionsConcessionStandClient.getConcessionStand(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      {
        concession_stand: {
          audit: { updated_by: (reply2?.concession_stand?.audit?.updated_by ?? '') },
        },
      },
      {
        concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Renews a concession stand.
   */
  renewConcessionStand(call, callback) {
    this.executeRenewConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRenewConcessionStand(request) {
    validateContext(request.context);
    if (!request.concession_stand_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
    }
    await this.store.record(request.context.tenant_id, 'RenewConcessionStand', request);
    this.logger.info('renew_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: {
        owner: {
          display_name: (request?.details?.new_owner?.display_name ?? ''),
          user_id: (request?.details?.new_owner?.user_id ?? ''),
        },
      },
    };
    const method1 = 'commentReplayClipApproval';
    const reply1 = await call((done) => this.clients.broadcastsReplayClipApprovalClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        region: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? ''),
      },
    };
    const client2 = this.clients.registry.get('acme.search.v1.SearchIndexService');
    const reply2 = await call((done) => client2.searchSearchIndexes(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: {
        approver: {
          display_name: (request?.details?.new_owner?.display_name ?? ''),
          email: (request?.details?.notify_contact?.email ?? ''),
        },
      },
      sponsorship_deal: {
        audit: {
          etag: (reply1?.replay_clip?.audit?.etag ?? ''),
          updated_at: reply1?.replay_clip?.audit?.updated_at,
        },
      },
    };
    const send3 = this.clients.sponsorshipsSponsorshipDealApprovalClient.submitSponsorshipDeal.bind(this.clients.sponsorshipsSponsorshipDealApprovalClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      sponsor: {
        audit: {
          etag: (reply1?.replay_clip?.audit?.etag ?? ''),
          updated_at: reply1?.replay_clip?.audit?.updated_at,
        },
        contact: { email: (request?.details?.new_owner?.email ?? '') },
      },
    };
    const spread4 = {
      ...request4,
      context: request4.context ? { ...request4.context, baggage: { ...request4.context.baggage } } : null,
    };
    const reply4 = await call((done) => this.clients.sponsorshipsSponsorApprovalClient.approveSponsor(spread4, metadataFor(spread4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: {
          dimensions: { height: (reply1?.replay_clip?.dimensions?.height ?? 0) },
          owner: { user_id: (reply1?.replay_clip?.owner?.user_id ?? '') },
        },
      },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { message: (reply2?.result?.message ?? '') },
      },
      {
        concession_stand: {
          audit: { created_by: (reply3?.sponsorship_deal?.audit?.created_by ?? '') },
        },
      },
      {
        outcome: { status: (reply4?.result?.status ?? '') },
        result: { status: (reply4?.outcome?.status ?? '') },
      },
      {
        concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Schedules a concession stand.
   */
  scheduleConcessionStandExpiry(call, callback) {
    this.executeScheduleConcessionStandExpiry(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScheduleConcessionStandExpiry(request) {
    validateContext(request.context);
    if (request.concession_stand) {
      await this.store.save(this.store.toConcessionStandRecord(request.context.tenant_id, request.concession_stand));
    }
    await this.store.record(request.context.tenant_id, 'ScheduleConcessionStandExpiry', request);
    this.logger.info('schedule_concession_stand_expiry', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      concession_stand: {
        components: { primary: {
          quantity: (input1?.concession_stand?.components?.primary?.quantity ?? '0'),
        } },
        owner: { email: (input1?.details?.notify?.email ?? '') },
      },
      context: input1?.context,
      details: { contact: { email: (input1?.concession_stand?.owner?.email ?? '') } },
    };
    const reply1 = await call((done) => this.clients.concessionsConcessionStandValidationClient.checkConcessionStandEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.concession_stand?.audit?.updated_at,
        owner_id: (request?.concession_stand?.components?.primary?.component_id ?? ''),
        region: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.region ?? ''),
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.boxofficeBoxOfficeSaleQueryClient.browseBoxOfficeSales(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: {
          dimensions: { unit: (reply1?.concession_stand?.dimensions?.unit ?? '') },
        },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        concession_stand: {
          concession_stand_id: (request?.concession_stand_id ?? ''),
          name: (request?.concession_stand?.name ?? ''),
        },
      },
    );
  }

  /**
   * Suspends a concession stand.
   */
  suspendConcessionStand(call, callback) {
    this.executeSuspendConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuspendConcessionStand(request) {
    validateContext(request.context);
    if (!request.concession_stand_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
    }
    await this.store.record(request.context.tenant_id, 'SuspendConcessionStand', request);
    this.logger.info('suspend_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: {
        owner: {
          display_name: (request?.details?.new_owner?.display_name ?? ''),
          email: (request?.details?.notify_contact?.email ?? ''),
        },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.concessionsMenuItemValidationClient.validateMenuItem(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.concessionsConcessionOrderReportingClient.generateConcessionOrderReport(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      stream_key: {
        audit: {
          created_at: reply1?.menu_item?.audit?.created_at,
          created_by: (reply1?.menu_item?.audit?.created_by ?? ''),
        },
        contact: {
          name: request?.details?.notify_contact?.name,
          phone: (request?.details?.notify_contact?.phone ?? ''),
        },
      },
    };
    const reply3 = await withRetry(() => call((done) => this.clients.broadcastsStreamKeyWorkflowClient.signalStreamKeyWorkflow(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.details?.notify_contact?.preferred_channel ?? '') });
    return assemble(
      {
        concession_stand: {
          audit: { created_by: (reply1?.menu_item?.audit?.created_by ?? '') },
          capacity: (reply1?.menu_item?.capacity ?? '0'),
        },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        concession_stand: { audit: { created_at: reply3?.stream_key?.audit?.created_at } },
        outcome: { message: (reply3?.outcome?.message ?? '') },
      },
      {
        concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }

  /**
   * Terminates a concession stand.
   */
  terminateConcessionStand(call, callback) {
    this.executeTerminateConcessionStand(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeTerminateConcessionStand(request) {
    validateContext(request.context);
    if (!request.concession_stand_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'concession_stand_id is required');
    }
    await this.store.record(request.context.tenant_id, 'TerminateConcessionStand', request);
    this.logger.info('terminate_concession_stand', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      work_order: { requester: { email: (request?.details?.notify_contact?.email ?? '') } },
    };
    const task1 = deferred(() => call((done) => this.clients.maintenanceWorkOrderClient.createWorkOrder(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        concession_stand: { owner: { email: (reply1?.work_order?.assignee?.email ?? '') } },
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      },
      {
        concession_stand: { concession_stand_id: (request?.concession_stand_id ?? '') },
        outcome: { effective_at: request?.details?.effective_at },
      },
    );
  }
}

module.exports = { ConcessionStandLifecycleService };
