'use strict';

/**
 * Answers read-only questions about fan sessions, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve FanSessionQueryService, recording every operation of the fanportal team.
 */
class FanSessionQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'FanSessionQueryService' });
  }

  /**
   * Lists fan sessions a page at a time.
   */
  browseFanSessions(call, callback) {
    this.executeBrowseFanSessions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseFanSessions(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseFanSessions', request);
    this.logger.info('browse_fan_sessions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.context?.baggage ?? {}) },
      context: request?.context,
      details: {
        recipient: { email: (request?.context?.actor?.email ?? '') },
        variables: { ...(request?.filter?.labels ?? {}) },
      },
    };
    const method1 = 'notifyEventPageWatchers';
    const reply1 = await call((done) => this.clients.fanportalEventPageNotificationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      fan_sessions: [{ audit: { created_by: (reply1?.event_page?.audit?.created_by ?? '') } }],
    };
  }

  /**
   * Counts fan sessions by status.
   */
  countFanSessions(call, callback) {
    this.executeCountFanSessions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountFanSessions(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountFanSessions', request);
    this.logger.info('count_fan_sessions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.filter?.created_after,
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const messages1 = await collect(this.clients.fanportalEventPageNotificationClient.streamEventPageNotices(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Lists fan sessions a page at a time.
   */
  findFanSessions(call, callback) {
    this.executeFindFanSessions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindFanSessions(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindFanSessions', request);
    this.logger.info('find_fan_sessions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const client1 = this.clients.registry.get('acme.anomalies.v1.AnomalyService');
    const reply1 = await call((done) => client1.batchGetAnomalies(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } };
  }

  /**
   * Returns a fan session.
   */
  lookupFanSession(call, callback) {
    this.executeLookupFanSession(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupFanSession(request) {
    validateContext(request.context);
    if (!request.fan_session_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'fan_session_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupFanSession', request);
    this.logger.info('lookup_fan_session', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.fan_session_id ?? ''),
      },
      since: request?.context?.deadline,
      subscriber: { email: (request?.context?.actor?.email ?? '') },
    };
    const messages1 = await collect(this.clients.fanportalEventPageNotificationClient.streamEventPageNotices(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    return { fan_session: { fan_session_id: (request?.fan_session_id ?? '') } };
  }

  /**
   * Returns a fan session.
   */
  peekFanSession(call, callback) {
    this.executePeekFanSession(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekFanSession(request) {
    validateContext(request.context);
    if (!request.fan_session_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'fan_session_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekFanSession', request);
    this.logger.info('peek_fan_session', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        recipient: { email: (request?.context?.actor?.email ?? '') },
        variables: { ...(request?.context?.baggage ?? {}) },
      },
    };
    const send1 = this.clients.fanportalEventPageNotificationClient.notifyEventPageWatchers.bind(this.clients.fanportalEventPageNotificationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: {
          message: (reply1?.outcome?.message ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      { fan_session: { fan_session_id: (request?.fan_session_id ?? '') } },
    );
  }

  /**
   * Returns several fan sessions at once.
   */
  resolveFanSessions(call, callback) {
    this.executeResolveFanSessions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveFanSessions(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveFanSessions', request);
    this.logger.info('resolve_fan_sessions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        recipient: { email: (request?.context?.actor?.email ?? '') },
        variables: { ...(request?.context?.baggage ?? {}) },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.fanportalEventPageNotificationClient.unsubscribeEventPageUpdates(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    return {
      fan_sessions: [{ audit: { etag: (reply1?.event_page?.audit?.etag ?? '') } }],
      result: { message: (reply1?.result?.message ?? '') },
    };
  }
}

module.exports = { FanSessionQueryService };
