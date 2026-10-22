'use strict';

/**
 * Manages tours: creates, reads, changes, and lists them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { clientStream, serverStream, unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of TourService, over the dependencies the tours team is given.
 */
function createTourHandlers({ store, clients, bus, logger }) {
  /**
   * Takes a stream of tours and answers once at the end.
   */
  async function batchCreateTours(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'BatchCreateTours', request);
    logger.info('batch_create_tours', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: ((request?.chunk === 'header' ? request.header : undefined)?.submitted_by?.preferred_channel ?? ''),
        },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.toursTourBudgetValidationClient.normalizeTourBudget(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (reply1?.tour_budget?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const reply2 = await call((done) => clients.toursTourBudgetValidationClient.explainTourBudgetDecision(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.audit?.etag ?? '') });
    return assemble(
      {
        result: {
          message: (reply1?.outcome?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { result: { message: (reply2?.result?.message ?? '') } },
    );
  }

  /**
   * Creates a tour.
   */
  async function createTour(request) {
    validateContext(request.context);
    if (request.tour) {
      await store.save(store.toTourRecord(request.context.tenant_id, request.tour));
    }
    await store.record(request.context.tenant_id, 'CreateTour', request);
    logger.info('create_tour', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.tour?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        details: { contact: { name: item?.contact?.name } },
        tour_budget: {
          contact: {
            email: (item?.contact?.email ?? ''),
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply1 = await call((done) => clients.toursTourBudgetValidationClient.lintTourBudgets(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      sponsor: {
        contact: {
          email: (reply1?.tour_budget?.contact?.email ?? ''),
          name: reply1?.tour_budget?.contact?.name,
        },
        primary_address: { region: (request?.tour?.location?.region ?? '') },
        relationship_manager: { email: (request?.tour?.organizer?.email ?? '') },
      },
    };
    const reply2 = await new Promise((resolve, reject) => {
      clients.sponsorshipsSponsorValidationClient.normalizeSponsor(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      {
        result: { details: (reply2?.result?.details ?? {}) },
        tour: { audit: { etag: (reply2?.sponsor?.audit?.etag ?? '') } },
      },
      {
        tour: {
          organizer: request?.tour?.organizer,
          status: (request?.tour?.status ?? ''),
        },
      },
    );
  }

  /**
   * Deletes a tour.
   */
  async function deleteTour(request) {
    validateContext(request.context);
    if (!request.tour_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tour_id is required');
    }
    await store.record(request.context.tenant_id, 'DeleteTour', request);
    logger.info('delete_tour', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.context?.baggage ?? {}) },
        owner_id: (request?.tour_id ?? ''),
      },
    };
    const searchSearchIndexes1 = promisify(clients.searchSearchIndexClient.searchSearchIndexes.bind(clients.searchSearchIndexClient));
    const reply1 = await searchSearchIndexes1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      reason: (request?.reason ?? ''),
      tour_budget: {
        audit: { etag: (request?.etag ?? '') },
        contact: { email: (request?.context?.actor?.email ?? '') },
      },
    };
    const reply2 = await call((done) => clients.toursTourBudgetValidationClient.scoreTourBudget(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          message: (reply1?.result?.message ?? ''),
        },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
    );
  }

  /**
   * Returns a tour.
   */
  async function getTour(request) {
    validateContext(request.context);
    if (!request.tour_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tour_id is required');
    }
    await store.record(request.context.tenant_id, 'GetTour', request);
    logger.info('get_tour', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      search_index: {
        audit: { created_at: request?.context?.deadline },
        components: { primary: { component_id: (request?.tour_id ?? '') } },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const method1 = 'updateSearchIndex';
    const reply1 = await call((done) => clients.searchSearchIndexClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { email: (reply1?.search_index?.owner?.email ?? '') } },
      tour_budget: {
        audit: {
          created_by: (reply1?.search_index?.audit?.created_by ?? ''),
          etag: (reply1?.search_index?.audit?.etag ?? ''),
        },
      },
    };
    const client2 = clients.registry.get('acme.tours.v1.TourBudgetValidationService');
    const reply2 = await call((done) => client2.previewTourBudget(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
        tour: {
          location: {
            recipient: ((reply1?.search_index?.locator === 'address' ? reply1?.search_index.address : undefined)?.recipient ?? ''),
          },
        },
      },
      {
        tour: {
          participants: [{ contact: { name: reply2?.tour_budget?.contact?.name } }],
        },
      },
      { tour: { tour_id: (request?.tour_id ?? '') } },
    );
  }

  /**
   * Lists tours a page at a time.
   */
  async function listTours(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListTours', request);
    logger.info('list_tours', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      tour_budget: { labels: { ...(request?.filter?.labels ?? {}) } },
    };
    const send1 = clients.toursTourBudgetValidationClient.normalizeTourBudget.bind(clients.toursTourBudgetValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      tours: [{
        participants: [{
          contact: {
            preferred_channel: (reply1?.tour_budget?.contact?.preferred_channel ?? ''),
          },
        }],
      }],
    };
  }

  /**
   * Lists tours a page at a time.
   */
  async function searchTours(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'SearchTours', request);
    logger.info('search_tours', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      tour_budget: { labels: { ...(request?.filter?.labels ?? {}) } },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.toursTourBudgetValidationClient.previewTourBudget(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: { message: (reply1?.result?.message ?? '') },
      tours: [{ audit: { created_by: (reply1?.tour_budget?.audit?.created_by ?? '') } }],
    };
  }

  /**
   * Changes a tour.
   */
  async function updateTour(request) {
    validateContext(request.context);
    if (request.tour) {
      await store.save(store.toTourRecord(request.context.tenant_id, request.tour));
    }
    await store.record(request.context.tenant_id, 'UpdateTour', request);
    logger.info('update_tour', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      access_policy: { audit: { updated_by: (input1?.tour?.audit?.updated_by ?? '') } },
      context: input1?.context,
    };
    const reply1 = await call((done) => clients.authorizationAccessPolicyValidationClient.scoreAccessPolicy(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      sponsor: {
        audit: { etag: (reply1?.access_policy?.audit?.etag ?? '') },
        relationship_manager: { display_name: (reply1?.access_policy?.owner?.display_name ?? '') },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.sponsorshipsSponsorValidationClient.normalizeSponsor(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const settled3 = await Promise.allSettled((request?.tour?.participants ?? []).map(async (item) => {
      const request3 = {
        context: request?.context,
        details: { contact: { email: (item?.contact?.email ?? '') } },
        tour_budget: {
          contact: { preferred_channel: (item?.contact?.preferred_channel ?? '') },
        },
      };
      const reply3 = await call((done) => clients.toursTourBudgetValidationClient.explainTourBudgetDecision(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const results3 = settled3.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply3 = results3.at(-1) ?? {};
    const request4 = {
      context: request?.context,
      details: {
        contact: { email: (reply2?.sponsor?.relationship_manager?.email ?? '') },
      },
      tour_budget: { contact: { email: (request?.tour?.organizer?.email ?? '') } },
    };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => clients.toursTourBudgetValidationClient.checkTourBudgetEligibility(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.tour?.audit?.etag ?? '') });
    return assemble(
      {
        tour: {
          organizer: { display_name: (reply1?.access_policy?.owner?.display_name ?? '') },
        },
      },
      {
        tour: {
          location: { recipient: (reply2?.sponsor?.primary_address?.recipient ?? '') },
        },
      },
      {
        result: {
          reference_ids: (reply4?.result?.reference_ids ?? []),
          status: (reply4?.outcome?.status ?? ''),
        },
      },
      {
        tour: {
          status: (request?.tour?.status ?? ''),
          window: request?.tour?.window,
        },
      },
    );
  }

  /**
   * Streams the changes of tours as they happen.
   */
  async function watchTours(request) {
    validateContext(request.context);
    await store.record(request.context.tenant_id, 'WatchTours', request);
    logger.info('watch_tours', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: {
          name: request?.subscriber?.name,
          phone: (request?.subscriber?.phone ?? ''),
        },
      },
      tour_budget: {
        contact: { email: (request?.subscriber?.email ?? '') },
        labels: { ...(request?.filter?.labels ?? {}) },
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.toursTourBudgetValidationClient.validateTourBudget(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const reply2 = await withRetry(() => call((done) => clients.searchSearchIndexClient.batchGetSearchIndexes(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const request3 = { context: request?.context };
    const task3 = deferred(() => call((done) => clients.authorizationAccessPolicyQueryClient.resolveAccessPolicies(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const reply3 = await task3();
    logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
    return assemble(
      {
        tour: {
          participants: [{
            contact: {
              preferred_channel: (reply1?.tour_budget?.contact?.preferred_channel ?? ''),
            },
          }],
        },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          status: (reply2?.result?.status ?? ''),
        },
      },
      { result: { status: (reply3?.result?.status ?? '') } },
      {
        tour: {
          location: { region: (request?.filter?.region ?? '') },
          organizer: { email: (request?.subscriber?.email ?? '') },
        },
      },
    );
  }

  return {
    batchCreateTours: clientStream(batchCreateTours, () => ({})),
    createTour: unary(createTour),
    deleteTour: unary(deleteTour),
    getTour: unary(getTour),
    listTours: unary(listTours),
    searchTours: unary(searchTours),
    updateTour: unary(updateTour),
    watchTours: serverStream(watchTours),
  };
}

module.exports = { createTourHandlers };
