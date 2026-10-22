'use strict';

/**
 * Manages performers: creates, reads, changes, and lists them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { clientStream, serverStream } = require('../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve PerformerService, recording every operation of the performers team.
 */
class PerformerService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'PerformerService' });
  }

  /**
   * Takes a stream of performers and answers once at the end.
   */
  batchCreatePerformers(call, callback) {
    clientStream((request) => this.executeBatchCreatePerformers(request), () => ({}))(call, callback);
  }

  async executeBatchCreatePerformers(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'BatchCreatePerformers', request);
    this.logger.info('batch_create_performers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.chunk) {
      case 'header':
        selected1_1 = ((request?.chunk === 'header' ? request.header : undefined)?.format ?? '');
        break;
      default:
        selected1_1 = '';
    }
    const request1 = {
      context: request?.context,
      details: { format: selected1_1 },
    };
    const reply1 = await call((done) => this.clients.performersPerformerQueryClient.summarizePerformers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          email: ((request?.chunk === 'record' ? request.record : undefined)?.contact?.email ?? ''),
          name: (request?.chunk === 'record' ? request.record : undefined)?.contact?.name,
          phone: ((request?.chunk === 'record' ? request.record : undefined)?.contact?.phone ?? ''),
        },
      },
      rider: {
        owner: {
          email: ((request?.chunk === 'header' ? request.header : undefined)?.submitted_by?.email ?? ''),
        },
      },
    };
    const reply2 = await withRetry(() => call((done) => this.clients.performersRiderValidationClient.previewRider(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: ((request?.chunk === 'record' ? request.record : undefined)?.identity_document === 'passport' ? (request?.chunk === 'record' ? request.record : undefined).passport : undefined)?.expires_at,
        owner_id: ((request?.chunk === 'record' ? request.record : undefined)?.relationship_manager?.user_id ?? ''),
        region: ((request?.chunk === 'record' ? request.record : undefined)?.primary_address?.region ?? ''),
      },
    };
    const task3 = deferred(() => call((done) => this.clients.venuesVenueSectionExportClient.listVenueSectionExports(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    this.logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        result: {
          correlation_id: (reply2?.result?.correlation_id ?? ''),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      { result: { details: (reply3?.result?.details ?? {}) } },
    );
  }

  /**
   * Returns several performers at once.
   */
  batchGetPerformers(call, callback) {
    this.executeBatchGetPerformers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBatchGetPerformers(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'BatchGetPerformers', request);
    this.logger.info('batch_get_performers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: {
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const reply1 = await this.bus.request('acme.performers.v1.PerformerService/BatchGetPerformers:0', request1);
    return { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } };
  }

  /**
   * Creates a performer.
   */
  clonePerformer(call, callback) {
    this.executeClonePerformer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeClonePerformer(request) {
    validateContext(request.context);
    if (request.performer) {
      await this.store.save(this.store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await this.store.record(request.context.tenant_id, 'ClonePerformer', request);
    this.logger.info('clone_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.admissionsAdmissionQueryClient.resolveAdmissions(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: { period: request?.performer?.preferences?.quiet_hours },
    };
    const reply2 = await call((done) => this.clients.venuesVenueSectionExportClient.scheduleVenueSectionExport(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        performer: {
          name: request?.performer?.name,
          performer_id: (request?.performer?.performer_id ?? ''),
        },
      },
    );
  }

  /**
   * Creates a performer.
   */
  createPerformer(call, callback) {
    this.executeCreatePerformer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCreatePerformer(request) {
    validateContext(request.context);
    if (request.performer) {
      await this.store.save(this.store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await this.store.record(request.context.tenant_id, 'CreatePerformer', request);
    this.logger.info('create_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: {
        participants: [{
          contact: {
            email: (request?.performer?.contact?.email ?? ''),
            name: request?.performer?.contact?.name,
          },
        }],
      },
      context: request?.context,
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.broadcastsBroadcastValidationClient.explainBroadcastDecision(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      filter: { region: (request?.performer?.primary_address?.region ?? '') },
    };
    const messages2 = await collect(this.clients.admissionsAdmissionQueryClient.exportAdmissions(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      {
        performer: {
          audit: { updated_by: (reply1?.broadcast?.audit?.updated_by ?? '') },
          relationship_manager: { display_name: (reply1?.broadcast?.organizer?.display_name ?? '') },
        },
      },
      {
        performer: {
          name: request?.performer?.name,
          status: (request?.performer?.status ?? ''),
        },
      },
    );
  }

  /**
   * Deletes a performer.
   */
  deletePerformer(call, callback) {
    this.executeDeletePerformer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDeletePerformer(request) {
    validateContext(request.context);
    if (!request.performer_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performer_id is required');
    }
    await this.store.record(request.context.tenant_id, 'DeletePerformer', request);
    this.logger.info('delete_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.context?.baggage ?? {}) },
      context: request?.context,
      details: { recipient: { email: (request?.context?.actor?.email ?? '') } },
      reason: (request?.reason ?? ''),
    };
    const sendVenuePolicyTestNotice1 = promisify(this.clients.venuesVenuePolicyNotificationClient.sendVenuePolicyTestNotice.bind(this.clients.venuesVenuePolicyNotificationClient));
    const reply1 = await sendVenuePolicyTestNotice1(request1, metadataFor(request1.context), { deadline: deadline() });
    this.logger.info('local_audit', { value: (request?.etag ?? '') });
    return { result: { status: (reply1?.result?.status ?? '') } };
  }

  /**
   * Returns a performer.
   */
  getPerformer(call, callback) {
    this.executeGetPerformer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetPerformer(request) {
    validateContext(request.context);
    if (!request.performer_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performer_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetPerformer', request);
    this.logger.info('get_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
    };
    const reply1 = await call((done) => this.clients.admissionsGateScanValidationClient.lintGateScans(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      venue: {
        owner: { display_name: (reply1?.gate_scan?.subject?.display_name ?? '') },
      },
    };
    const method2 = 'retryVenueExport';
    const reply2 = await call((done) => this.clients.venuesVenueExportClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          message: (reply1?.result?.message ?? ''),
        },
      },
      {
        performer: {
          primary_address: {
            country_code: ((reply2?.venue?.locator === 'address' ? reply2?.venue.address : undefined)?.country_code ?? ''),
          },
        },
      },
      { performer: { performer_id: (request?.performer_id ?? '') } },
    );
  }

  /**
   * Lists performers a page at a time.
   */
  listPerformers(call, callback) {
    this.executeListPerformers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListPerformers(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListPerformers', request);
    this.logger.info('list_performers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
    };
    const client1 = this.clients.registry.get('acme.performers.v1.RiderValidationService');
    const reply1 = await call((done) => client1.scoreRider(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      performers: [{
        audit: {
          created_at: reply1?.rider?.audit?.created_at,
          created_by: (reply1?.rider?.audit?.created_by ?? ''),
        },
      }],
    };
  }

  /**
   * Lists performers a page at a time.
   */
  searchPerformers(call, callback) {
    this.executeSearchPerformers(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSearchPerformers(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SearchPerformers', request);
    this.logger.info('search_performers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
      gate_scan: { occurred_at: request?.filter?.created_after },
    };
    const send1 = this.clients.admissionsGateScanValidationClient.normalizeGateScan.bind(this.clients.admissionsGateScanValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        details: (reply1?.result?.details ?? {}),
        message: (reply1?.outcome?.message ?? ''),
      },
    };
  }

  /**
   * Changes a performer.
   */
  updatePerformer(call, callback) {
    this.executeUpdatePerformer(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeUpdatePerformer(request) {
    validateContext(request.context);
    if (request.performer) {
      await this.store.save(this.store.toPerformerRecord(request.context.tenant_id, request.performer));
    }
    await this.store.record(request.context.tenant_id, 'UpdatePerformer', request);
    this.logger.info('update_performer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: {
          postal_code: (request?.performer?.primary_address?.postal_code ?? ''),
          region: (request?.performer?.primary_address?.region ?? ''),
        },
      },
      rider: {
        audit: { created_at: request?.performer?.audit?.created_at },
        owner: { user_id: (request?.performer?.relationship_manager?.user_id ?? '') },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.performersRiderValidationClient.scoreRider(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      details: {
        destination: { locality: (input2?.performer?.primary_address?.locality ?? '') },
      },
    };
    const reply2 = await call((done) => this.clients.broadcastsStreamKeyValidationClient.scoreStreamKey(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.performer?.audit?.etag ?? '') });
    return assemble(
      {
        performer: { relationship_manager: { email: (reply1?.rider?.owner?.email ?? '') } },
      },
      {
        performer: { contact: { email: (reply2?.stream_key?.contact?.email ?? '') } },
      },
      {
        performer: {
          attributes: (request?.performer?.attributes ?? {}),
          performer_id: (request?.performer?.performer_id ?? ''),
        },
      },
    );
  }

  /**
   * Streams the changes of performers as they happen.
   */
  watchPerformers(call) {
    serverStream((request) => this.executeWatchPerformers(request))(call);
  }

  async executeWatchPerformers(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'WatchPerformers', request);
    this.logger.info('watch_performers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          email: (request?.subscriber?.email ?? ''),
          name: request?.subscriber?.name,
          phone: (request?.subscriber?.phone ?? ''),
          preferred_channel: (request?.subscriber?.preferred_channel ?? ''),
        },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.admissionsGateScanValidationClient.lintGateScans(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => this.clients.performersPerformerContractQueryClient.resolvePerformerContracts(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.filter?.owner_id ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.outcome?.message ?? ''),
          status: (reply1?.outcome?.status ?? ''),
        },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          message: (reply2?.result?.message ?? ''),
        },
      },
      {
        performer: {
          primary_address: { region: (request?.filter?.region ?? '') },
          relationship_manager: { email: (request?.subscriber?.email ?? '') },
        },
      },
    );
  }
}

module.exports = { PerformerService };
