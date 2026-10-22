'use strict';

/**
 * Answers read-only questions about stream keys, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call } = require('../../platform/rpc');

/**
 * Serve StreamKeyQueryService, recording every operation of the broadcasts team.
 */
class StreamKeyQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'StreamKeyQueryService' });
  }

  /**
   * Lists stream keys a page at a time.
   */
  browseStreamKeys(call, callback) {
    this.executeBrowseStreamKeys(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseStreamKeys(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseStreamKeys', request);
    this.logger.info('browse_stream_keys', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      broadcast: { location: { region: (request?.filter?.region ?? '') } },
      context: request?.context,
    };
    const reply1 = await this.bus.request('acme.broadcasts.v1.StreamKeyQueryService/BrowseStreamKeys:0', request1);
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        details: (reply1?.result?.details ?? {}),
        message: (reply1?.result?.message ?? ''),
      },
    };
  }

  /**
   * Streams every matching stream key.
   */
  exportStreamKeys(call) {
    serverStream((request) => this.executeExportStreamKeys(request))(call);
  }

  async executeExportStreamKeys(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportStreamKeys', request);
    this.logger.info('export_stream_keys', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.broadcastsReplayClipCalculationClient.compareReplayClipOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    let selected2_1;
    switch (reply1?.replay_clip?.locator) {
      case 'address':
        selected2_1 = ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.recipient ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      details: { destination: { recipient: selected2_1 } },
      stream_key: { audit: { updated_by: (reply1?.replay_clip?.audit?.updated_by ?? '') } },
    };
    const reply2 = await call((done) => this.clients.broadcastsStreamKeyValidationClient.scoreStreamKey(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      replay_clip: {
        address: {
          postal_code: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.postal_code ?? ''),
        },
      },
    };
    const reply3 = await new Promise((resolve, reject) => {
      this.clients.broadcastsReplayClipValidationClient.normalizeReplayClip(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        stream_key: { audit: { created_at: reply2?.stream_key?.audit?.created_at } },
      },
      {
        result: { reference_ids: (reply3?.result?.reference_ids ?? []) },
        stream_key: { audit: { updated_at: reply3?.replay_clip?.audit?.updated_at } },
      },
      { stream_key: { labels: (request?.filter?.labels ?? {}) } },
    );
  }

  /**
   * Lists stream keys a page at a time.
   */
  findStreamKeys(call, callback) {
    this.executeFindStreamKeys(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindStreamKeys(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindStreamKeys', request);
    this.logger.info('find_stream_keys', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Returns a stream key.
   */
  lookupStreamKey(call, callback) {
    this.executeLookupStreamKey(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupStreamKey(request) {
    validateContext(request.context);
    if (!request.stream_key_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'stream_key_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupStreamKey', request);
    this.logger.info('lookup_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      stream_key: { stream_key_id: (request?.stream_key_id ?? '') },
    };
    const validateStreamKey1 = promisify(this.clients.broadcastsStreamKeyValidationClient.validateStreamKey.bind(this.clients.broadcastsStreamKeyValidationClient));
    const reply1 = await validateStreamKey1(request1, metadataFor(request1.context), { deadline: deadline() });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          status: (reply1?.outcome?.status ?? ''),
        },
      },
      { stream_key: { stream_key_id: (request?.stream_key_id ?? '') } },
    );
  }

  /**
   * Returns a stream key.
   */
  peekStreamKey(call, callback) {
    this.executePeekStreamKey(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekStreamKey(request) {
    validateContext(request.context);
    if (!request.stream_key_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'stream_key_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekStreamKey', request);
    this.logger.info('peek_stream_key', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: { owner: { user_id: (request?.context?.actor?.user_id ?? '') } },
    };
    const reply1 = await call((done) => this.clients.broadcastsReplayClipValidationClient.checkReplayClipEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      {
        stream_key: {
          audit: { created_at: reply1?.replay_clip?.audit?.created_at },
          holder: { display_name: (reply1?.replay_clip?.owner?.display_name ?? '') },
        },
      },
      { stream_key: { stream_key_id: (request?.stream_key_id ?? '') } },
    );
  }

  /**
   * Returns several stream keys at once.
   */
  resolveStreamKeys(call, callback) {
    this.executeResolveStreamKeys(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveStreamKeys(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveStreamKeys', request);
    this.logger.info('resolve_stream_keys', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const method1 = 'batchGetDocuments';
    const reply1 = await call((done) => this.clients.documentsDocumentClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      stream_key: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        labels: { ...(reply1?.result?.details ?? {}) },
      },
    };
    const client2 = this.clients.registry.get('acme.broadcasts.v1.StreamKeyCalculationService');
    const reply2 = await call((done) => client2.quoteStreamKey(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      { result: { details: (reply2?.result?.details ?? {}) } },
    );
  }

  /**
   * Lists stream keys a page at a time.
   */
  suggestStreamKeys(call, callback) {
    this.executeSuggestStreamKeys(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuggestStreamKeys(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SuggestStreamKeys', request);
    this.logger.info('suggest_stream_keys', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
    };
    const send1 = this.clients.featureflagsFeatureFlagValidationClient.previewFeatureFlag.bind(this.clients.featureflagsFeatureFlagValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      stream_key: {
        audit: {
          etag: (reply1?.feature_flag?.audit?.etag ?? ''),
          updated_by: (reply1?.feature_flag?.audit?.updated_by ?? ''),
        },
        holder: { display_name: (reply1?.feature_flag?.owner?.display_name ?? '') },
        limits: [{ name: (reply1?.feature_flag?.name ?? '') }],
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.explainStreamKeyCalculation(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        stream_keys: [{ audit: { etag: (reply2?.stream_key?.audit?.etag ?? '') } }],
      },
    );
  }

  /**
   * Summarizes a stream key.
   */
  summarizeStreamKeys(call, callback) {
    this.executeSummarizeStreamKeys(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeStreamKeys(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeStreamKeys', request);
    this.logger.info('summarize_stream_keys', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      details: { contact: { email: (input1?.context?.actor?.email ?? '') } },
      stream_key_id: (input1?.stream_key_id ?? ''),
    };
    const reply1 = await call((done) => this.clients.broadcastsStreamKeyCalculationClient.explainStreamKeyCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            audit: { updated_at: reply1?.stream_key?.audit?.updated_at },
            manager: { display_name: (reply1?.stream_key?.holder?.display_name ?? '') },
          }],
        }],
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => this.clients.venuesVenueSectionValidationClient.normalizeVenueSection(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          message: (reply1?.result?.message ?? ''),
        },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
      { period: request?.details?.period },
    );
  }
}

module.exports = { StreamKeyQueryService };
