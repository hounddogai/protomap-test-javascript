'use strict';

/**
 * Checks riders against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve RiderValidationService, recording every operation of the performers team.
 */
class RiderValidationService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'RiderValidationService' });
  }

  /**
   * Returns a rider.
   */
  getRiderRules(call, callback) {
    this.executeGetRiderRules(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetRiderRules(request) {
    validateContext(request.context);
    if (!request.rider_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'rider_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetRiderRules', request);
    this.logger.info('get_rider_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const batchGetNotifications1 = promisify(this.clients.notificationsNotificationClient.batchGetNotifications.bind(this.clients.notificationsNotificationClient));
    const reply1 = await batchGetNotifications1(request1, metadataFor(request1.context), { deadline: deadline() });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { rider: { rider_id: (request?.rider_id ?? '') } },
    );
  }

  /**
   * Lints a rider.
   */
  lintRiders(call, callback) {
    this.executeLintRiders(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLintRiders(request) {
    validateContext(request.context);
    if (request.rider) {
      await this.store.save(this.store.toRiderRecord(request.context.tenant_id, request.rider));
    }
    await this.store.record(request.context.tenant_id, 'LintRiders', request);
    this.logger.info('lint_riders', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            audit: {
              created_at: request?.rider?.audit?.created_at,
              updated_at: request?.rider?.audit?.updated_at,
            },
            manager: { user_id: (request?.rider?.owner?.user_id ?? '') },
          }],
        }],
      },
    };
    const reply1 = await call((done) => this.clients.venuesVenueSectionHistoryClient.annotateVenueSectionVersion(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.venue_section?.audit?.created_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        region: (reply1?.venue_section?.location?.region ?? ''),
      },
    };
    const method2 = 'listVenueSectionConflicts';
    const reply2 = await call((done) => this.clients.venuesVenueSectionSyncClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { status: (reply1?.outcome?.status ?? '') },
        rider: { audit: { updated_at: reply1?.venue_section?.audit?.updated_at } },
      },
      {
        outcome: { status: (reply2?.result?.status ?? '') },
        result: { status: (reply2?.result?.status ?? '') },
      },
      {
        rider: {
          rider_id: (request?.rider_id ?? ''),
          status: (request?.rider?.status ?? ''),
        },
      },
    );
  }

  /**
   * Normalizes a rider.
   */
  normalizeRider(call, callback) {
    this.executeNormalizeRider(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeNormalizeRider(request) {
    validateContext(request.context);
    if (request.rider) {
      await this.store.save(this.store.toRiderRecord(request.context.tenant_id, request.rider));
    }
    await this.store.record(request.context.tenant_id, 'NormalizeRider', request);
    this.logger.info('normalize_rider', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: {
        audit: {
          created_by: (request?.rider?.audit?.created_by ?? ''),
          etag: (request?.rider?.audit?.etag ?? ''),
        },
        owner: { email: (request?.rider?.owner?.email ?? '') },
      },
    };
    const client1 = this.clients.registry.get('acme.concessions.v1.MenuItemCalculationService');
    const reply1 = await call((done) => client1.recalculateMenuItems(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          email: (reply1?.menu_item?.owner?.email ?? ''),
          name: request?.details?.contact?.name,
        },
      },
      gate_scan: { measurements: [{ unit: (reply1?.menu_item?.dimensions?.unit ?? '') }] },
    };
    const send2 = this.clients.admissionsGateScanValidationClient.explainGateScanDecision.bind(this.clients.admissionsGateScanValidationClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      {
        result: {
          correlation_id: (reply2?.gate_scan?.correlation_id ?? ''),
          message: (reply2?.result?.message ?? ''),
        },
      },
      {
        rider: {
          rider_id: (request?.rider_id ?? ''),
          version: (request?.rider?.version ?? '0'),
        },
      },
    );
  }

  /**
   * Previews a rider.
   */
  previewRider(call, callback) {
    this.executePreviewRider(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePreviewRider(request) {
    validateContext(request.context);
    if (request.rider) {
      await this.store.save(this.store.toRiderRecord(request.context.tenant_id, request.rider));
    }
    await this.store.record(request.context.tenant_id, 'PreviewRider', request);
    this.logger.info('preview_rider', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: {
        components: { primary: { description: (request?.rider?.description ?? '') } },
        owner: { email: (request?.details?.contact?.email ?? '') },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.concessionsMenuItemCalculationClient.recalculateMenuItems(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      notification: { requester: { phone: (input2?.details?.contact?.phone ?? '') } },
    };
    const reply2 = await call((done) => this.clients.notificationsNotificationValidationClient.normalizeNotification(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            audit: {
              updated_at: reply2?.notification?.audit?.updated_at,
              updated_by: (request?.rider?.audit?.updated_by ?? ''),
            },
            location: {
              country_code: ((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.country_code ?? ''),
              locality: ((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.locality ?? ''),
            },
          }],
        }],
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => this.clients.venuesVenueSectionValidationClient.validateVenueSection(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = {
      audit_event: {
        subject: { display_name: (request?.rider?.owner?.display_name ?? '') },
      },
      context: request?.context,
      details: {
        contact: {
          name: request?.details?.contact?.name,
          preferred_channel: (reply2?.notification?.requester?.preferred_channel ?? ''),
        },
      },
    };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => this.clients.auditAuditEventValidationClient.explainAuditEventDecision(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { message: (reply1?.result?.message ?? '') },
        rider: { audit: { etag: (reply1?.menu_item?.audit?.etag ?? '') } },
      },
      {
        result: { message: (reply2?.outcome?.message ?? '') },
        rider: {
          owner: {
            display_name: ((reply2?.notification?.origin === 'customer' ? reply2?.notification.customer : undefined)?.display_name ?? ''),
          },
        },
      },
      {
        rider: {
          owner: { display_name: (reply3?.venue_section?.manager?.display_name ?? '') },
        },
      },
      { outcome: { reference_id: (reply4?.outcome?.reference_id ?? '') } },
      {
        rider: {
          rider_id: (request?.rider_id ?? ''),
          status: (request?.rider?.status ?? ''),
        },
      },
    );
  }

  /**
   * Scores a rider.
   */
  scoreRider(call, callback) {
    this.executeScoreRider(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeScoreRider(request) {
    validateContext(request.context);
    if (request.rider) {
      await this.store.save(this.store.toRiderRecord(request.context.tenant_id, request.rider));
    }
    await this.store.record(request.context.tenant_id, 'ScoreRider', request);
    this.logger.info('score_rider', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: { owner: { display_name: (request?.rider?.owner?.display_name ?? '') } },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.broadcastsReplayClipValidationClient.previewReplayClip(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      notification: {
        assignee: { user_id: (request?.rider?.owner?.user_id ?? '') },
        requester: { email: (reply1?.replay_clip?.owner?.email ?? '') },
      },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.notificationsNotificationClient.createNotification(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      filter: {
        region: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? ''),
      },
    };
    const task3 = deferred(() => call((done) => this.clients.auditAuditEventQueryClient.browseAuditEvents(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        rider: {
          owner: {
            display_name: (reply1?.replay_clip?.owner?.display_name ?? ''),
            email: (reply1?.replay_clip?.owner?.email ?? ''),
          },
        },
      },
      {
        rider: { owner: { email: (reply2?.notification?.assignee?.email ?? '') } },
      },
      {
        result: {
          details: (reply3?.result?.details ?? {}),
          reference_ids: (reply3?.result?.reference_ids ?? []),
        },
      },
      {
        rider: {
          rider_id: (request?.rider_id ?? ''),
          status: (request?.rider?.status ?? ''),
        },
      },
    );
  }

  /**
   * Validates a rider.
   */
  validateRider(call, callback) {
    this.executeValidateRider(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeValidateRider(request) {
    validateContext(request.context);
    if (request.rider) {
      await this.store.save(this.store.toRiderRecord(request.context.tenant_id, request.rider));
    }
    await this.store.record(request.context.tenant_id, 'ValidateRider', request);
    this.logger.info('validate_rider', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.rider?.audit?.updated_at,
        labels: { ...(request?.rider?.parameters ?? {}) },
      },
    };
    const reply1 = await this.bus.request('acme.performers.v1.RiderValidationService/ValidateRider:0', request1);
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        rider: {
          description: (request?.rider?.description ?? ''),
          rider_id: (request?.rider_id ?? ''),
        },
      },
    );
  }
}

module.exports = { RiderValidationService };
