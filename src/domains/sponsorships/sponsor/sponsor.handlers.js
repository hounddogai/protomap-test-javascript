'use strict';

/**
 * Manages sponsors: creates, reads, changes, and lists them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { clientStream, serverStream, unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of SponsorService, over the dependencies the sponsorships team is given.
 */
function createSponsorHandlers({ store, clients, bus, logger }) {
  /**
   * Takes a stream of sponsors and answers once at the end.
   */
  async function batchCreateSponsors(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'BatchCreateSponsors', request);
    logger.info('batch_create_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          name: (request?.chunk === 'header' ? request.header : undefined)?.submitted_by?.name,
        },
      },
      replay_clip: {
        audit: {
          updated_by: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.updated_by ?? ''),
        },
        owner: {
          user_id: ((request?.chunk === 'record' ? request.record : undefined)?.relationship_manager?.user_id ?? ''),
        },
      },
    };
    const method1 = 'previewReplayClip';
    const reply1 = await call((done) => clients.broadcastsReplayClipValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            audit: {
              etag: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? ''),
              updated_at: (request?.chunk === 'record' ? request.record : undefined)?.audit?.updated_at,
            },
            location: {
              region: ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? ''),
            },
          }],
        }],
      },
    };
    const client2 = clients.registry.get('acme.venues.v1.VenueSectionHistoryService');
    const reply2 = await call((done) => client2.annotateVenueSectionVersion(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      sponsor: {
        audit: { created_by: (reply2?.venue_section?.audit?.created_by ?? '') },
      },
    };
    const send3 = clients.sponsorshipsSponsorCalculationClient.recalculateSponsors.bind(clients.sponsorshipsSponsorCalculationClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      filter: {
        created_after: reply2?.venue_section?.audit?.created_at,
        owner_id: (reply1?.replay_clip?.components?.primary?.component_id ?? ''),
        region: (reply3?.sponsor?.primary_address?.region ?? ''),
      },
    };
    const spread4 = {
      ...request4,
      context: request4.context ? { ...request4.context, baggage: { ...request4.context.baggage } } : null,
    };
    const reply4 = await call((done) => clients.sponsorshipsSponsorQueryClient.findSponsors(spread4, metadataFor(spread4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.created_by ?? '') });
    return assemble(
      { result: { message: (reply1?.outcome?.message ?? '') } },
      { result: { status: (reply2?.outcome?.status ?? '') } },
      {
        result: {
          correlation_id: (reply3?.result?.correlation_id ?? ''),
          message: (reply3?.result?.message ?? ''),
        },
      },
      { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
    );
  }

  /**
   * Creates a sponsor.
   */
  async function cloneSponsor(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await store.record(request.context.tenant_id, 'CloneSponsor', request);
    logger.info('clone_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      details: {
        contact: { email: (input1?.sponsor?.relationship_manager?.email ?? '') },
      },
    };
    const reply1 = await call((done) => clients.venuesVenuePolicyValidationClient.previewVenuePolicy(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.sponsor?.audit?.created_at,
        labels: { ...(reply1?.venue_policy?.parameters ?? {}) },
        owner_id: (reply1?.result?.correlation_id ?? ''),
        region: (request?.sponsor?.primary_address?.region ?? ''),
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.sponsorshipsSponsorQueryClient.browseSponsors(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      {
        sponsor: {
          audit: { created_by: (reply1?.venue_policy?.audit?.created_by ?? '') },
        },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
      {
        sponsor: {
          attributes: (request?.sponsor?.attributes ?? {}),
          status: (request?.sponsor?.status ?? ''),
        },
      },
    );
  }

  /**
   * Creates a sponsor.
   */
  async function createSponsor(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await store.record(request.context.tenant_id, 'CreateSponsor', request);
    logger.info('create_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: { postal_code: (request?.sponsor?.primary_address?.postal_code ?? '') },
      },
      stream_key: {
        audit: { etag: (request?.sponsor?.audit?.etag ?? '') },
        contact: { email: (request?.sponsor?.contact?.email ?? '') },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.broadcastsStreamKeyCalculationClient.estimateStreamKey(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      sponsor: {
        passport: {
          number: ((request?.sponsor?.identity_document === 'national_id' ? request?.sponsor.national_id : undefined)?.number ?? ''),
        },
        primary_address: { locality: (request?.sponsor?.primary_address?.locality ?? '') },
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => clients.sponsorshipsSponsorValidationClient.explainSponsorDecision(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      { sponsor: { name: reply1?.stream_key?.contact?.name } },
      {
        sponsor: { contact: { phone: (reply2?.sponsor?.contact?.phone ?? '') } },
      },
      {
        sponsor: {
          attributes: (request?.sponsor?.attributes ?? {}),
          contact: { name: request?.sponsor?.name },
        },
      },
    );
  }

  /**
   * Deletes a sponsor.
   */
  async function deleteSponsor(request) {
    validateContext(request.context);
    if (!request.sponsor_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsor_id is required');
    }
    await store.record(request.context.tenant_id, 'DeleteSponsor', request);
    logger.info('delete_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      sponsorship_deal: {
        audit: { etag: (request?.etag ?? '') },
        counterparty: { sponsor_id: (request?.sponsor_id ?? '') },
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.sponsorshipsSponsorshipDealValidationClient.validateSponsorshipDeal(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } };
  }

  /**
   * Returns a sponsor.
   */
  async function getSponsor(request) {
    validateContext(request.context);
    if (!request.sponsor_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'sponsor_id is required');
    }
    await store.record(request.context.tenant_id, 'GetSponsor', request);
    logger.info('get_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: {
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const task1 = deferred(() => call((done) => clients.broadcastsReplayClipValidationClient.validateReplayClip(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { sponsor: { sponsor_id: (request?.sponsor_id ?? '') } },
    );
  }

  /**
   * Lists sponsors a page at a time.
   */
  async function listSponsors(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListSponsors', request);
    logger.info('list_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.context?.baggage ?? {}) },
      context: request?.context,
      details: { filters: { ...(request?.filter?.labels ?? {}) } },
    };
    const reply1 = await bus.request('acme.sponsorships.v1.SponsorService/ListSponsors:0', request1);
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { message: (reply1?.result?.message ?? '') } };
  }

  /**
   * Changes a sponsor.
   */
  async function updateSponsor(request) {
    validateContext(request.context);
    if (request.sponsor) {
      await store.save(store.toSponsorRecord(request.context.tenant_id, request.sponsor));
    }
    await store.record(request.context.tenant_id, 'UpdateSponsor', request);
    logger.info('update_sponsor', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_4;
    switch (request?.sponsor?.identity_document) {
      case 'national_id':
        selected1_4 = ((request?.sponsor?.identity_document === 'national_id' ? request?.sponsor.national_id : undefined)?.number ?? '');
        break;
      default:
        selected1_4 = '';
    }
    const request1 = {
      context: request?.context,
      details: { contact: { name: request?.sponsor?.contact?.name } },
      sponsor: {
        audit: { etag: (request?.sponsor?.audit?.etag ?? '') },
        national_id: { number: selected1_4 },
        primary_address: { recipient: (request?.sponsor?.primary_address?.recipient ?? '') },
      },
    };
    const reply1 = await call((done) => clients.sponsorshipsSponsorValidationClient.previewSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      sponsor: {
        preferences: { channels: [...(reply1?.sponsor?.preferences?.channels ?? [])] },
      },
    };
    const reply2 = await call((done) => clients.sponsorshipsSponsorValidationClient.scoreSponsor(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.sponsor?.audit?.updated_by ?? '') });
    return assemble(
      {
        sponsor: {
          contact: {
            preferred_channel: (reply1?.sponsor?.contact?.preferred_channel ?? ''),
          },
          primary_address: reply1?.sponsor?.primary_address,
        },
      },
      {
        sponsor: {
          audit: { created_by: (reply2?.sponsor?.audit?.created_by ?? '') },
          primary_address: reply2?.sponsor?.primary_address,
        },
      },
      {
        sponsor: {
          name: request?.sponsor?.name,
          relationship_manager: request?.sponsor?.relationship_manager,
        },
      },
    );
  }

  /**
   * Streams the changes of sponsors as they happen.
   */
  async function watchSponsors(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'WatchSponsors', request);
    logger.info('watch_sponsors', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          name: request?.subscriber?.name,
          phone: (request?.subscriber?.phone ?? ''),
          preferred_channel: (request?.subscriber?.preferred_channel ?? ''),
        },
      },
      metric_series: { address: { region: (request?.filter?.region ?? '') } },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.metricsMetricSeriesValidationClient.validateMetricSeries(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      filter: {
        owner_id: (request?.filter?.owner_id ?? ''),
        region: ((reply1?.metric_series?.locator === 'address' ? reply1?.metric_series.address : undefined)?.region ?? ''),
      },
    };
    const messages2 = await collect(clients.featureflagsFeatureFlagQueryClient.exportFeatureFlags(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    const request3 = {
      context: request?.context,
      stream_key: {
        audit: {
          created_at: reply1?.metric_series?.audit?.created_at,
          created_by: (reply1?.metric_series?.audit?.created_by ?? ''),
          etag: (reply1?.metric_series?.audit?.etag ?? ''),
        },
        contact: { phone: (request?.subscriber?.phone ?? '') },
      },
    };
    const validateStreamKey3 = promisify(clients.broadcastsStreamKeyValidationClient.validateStreamKey.bind(clients.broadcastsStreamKeyValidationClient));
    const reply3 = await validateStreamKey3(request3, metadataFor(request3.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.subscriber?.email ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.outcome?.message ?? ''),
          status: (reply1?.outcome?.status ?? ''),
        },
      },
      {
        sponsor: {
          audit: { etag: (reply3?.stream_key?.audit?.etag ?? '') },
          contact: reply3?.stream_key?.contact,
        },
      },
      {
        sponsor: {
          name: request?.subscriber?.name,
          primary_address: { region: (request?.filter?.region ?? '') },
        },
      },
    );
  }

  return {
    batchCreateSponsors: clientStream(batchCreateSponsors, () => ({})),
    cloneSponsor: unary(cloneSponsor),
    createSponsor: unary(createSponsor),
    deleteSponsor: unary(deleteSponsor),
    getSponsor: unary(getSponsor),
    listSponsors: unary(listSponsors),
    updateSponsor: unary(updateSponsor),
    watchSponsors: serverStream(watchSponsors),
  };
}

module.exports = { createSponsorHandlers };
