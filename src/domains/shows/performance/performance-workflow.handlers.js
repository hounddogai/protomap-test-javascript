'use strict';

/**
 * Runs the long-lived workflow that takes a performance from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { exchange } = require('../../../platform/streams');

/**
 * The handlers of PerformanceWorkflowService, over the dependencies the shows team is given.
 */
function createPerformanceWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a performance.
   */
  async function advancePerformanceWorkflow(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'AdvancePerformanceWorkflow', request);
    logger.info('advance_performance_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.performance?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        details: { notify: { email: (item?.contact?.email ?? '') } },
        show: {
          participants: [{
            contact: {
              name: item?.contact?.name,
              preferred_channel: (item?.contact?.preferred_channel ?? ''),
            },
          }],
        },
      };
      const reply1 = await call((done) => clients.showsShowWorkflowClient.pauseShowWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      performance_id: (request?.performance?.performance_id ?? ''),
    };
    const reply2 = await withRetry(() => call((done) => clients.showsPerformanceApprovalClient.getPerformanceApprovalStatus(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply2?.result?.status ?? '') },
        performance: {
          location: { recipient: (reply2?.performance?.location?.recipient ?? '') },
        },
      },
      {
        performance: {
          organizer: request?.performance?.organizer,
          performance_id: (request?.performance_id ?? ''),
        },
      },
    );
  }

  /**
   * Lists performances a page at a time.
   */
  async function listPerformanceWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListPerformanceWorkflows', request);
    logger.info('list_performance_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      search_index: { address: { region: (request?.filter?.region ?? '') } },
    };
    const task1 = deferred(() => call((done) => clients.searchSearchIndexClient.updateSearchIndex(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      performances: [{
        audit: { created_by: (reply1?.search_index?.audit?.created_by ?? '') },
      }],
    };
  }

  /**
   * Pauses a performance.
   */
  async function pausePerformanceWorkflow(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'PausePerformanceWorkflow', request);
    logger.info('pause_performance_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      show: {
        participants: [{ contact: { email: (request?.performance?.organizer?.email ?? '') } }],
      },
    };
    const reply1 = await bus.request('acme.shows.v1.PerformanceWorkflowService/PausePerformanceWorkflow:0', request1);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { message: (reply1?.outcome?.message ?? '') } },
      {
        performance: {
          organizer: request?.performance?.organizer,
          performance_id: (request?.performance_id ?? ''),
        },
      },
    );
  }

  /**
   * Resumes a performance.
   */
  async function resumePerformanceWorkflow(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'ResumePerformanceWorkflow', request);
    logger.info('resume_performance_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.performance?.audit?.updated_at,
        owner_id: (request?.performance?.organizer?.user_id ?? ''),
        region: (request?.performance?.location?.region ?? ''),
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.searchSearchIndexClient.searchSearchIndexes(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      show: {
        participants: [{ contact: { phone: (request?.details?.notify?.phone ?? '') } }],
      },
    };
    const reply2 = await call((done) => clients.showsShowApprovalClient.recallShow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        performance: { organizer: { email: (reply2?.show?.organizer?.email ?? '') } },
      },
      {
        performance: {
          performance_id: (request?.performance_id ?? ''),
          title: (request?.performance?.title ?? ''),
        },
      },
    );
  }

  /**
   * Retrys a performance.
   */
  async function retryPerformanceWorkflow(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'RetryPerformanceWorkflow', request);
    logger.info('retry_performance_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_hold: {
        location: { country_code: (request?.performance?.location?.country_code ?? '') },
        participants: [{
          contact: {
            preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.seatingSeatHoldApprovalClient.commentSeatHoldApproval(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = { context: request?.context };
    const resolvePerformances2 = promisify(clients.showsPerformanceQueryClient.resolvePerformances.bind(clients.showsPerformanceQueryClient));
    const reply2 = await resolvePerformances2(request2, metadataFor(request2.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      { result: { details: (reply2?.result?.details ?? {}) } },
      {
        performance: {
          performance_id: (request?.performance_id ?? ''),
          window: request?.performance?.window,
        },
      },
    );
  }

  /**
   * Signals a performance.
   */
  async function signalPerformanceWorkflow(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'SignalPerformanceWorkflow', request);
    logger.info('signal_performance_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      lineup: {
        billing_address: {
          locality: (request?.performance?.location?.locality ?? ''),
          postal_code: (request?.performance?.location?.postal_code ?? ''),
        },
        counterparty: { display_name: (request?.performance?.organizer?.display_name ?? '') },
      },
    };
    const reply1 = await call((done) => clients.showsLineupApprovalClient.submitLineup(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: {
        filters: { ...(reply1?.result?.details ?? {}) },
        period: request?.performance?.window,
      },
      reason: (request?.reason ?? ''),
    };
    const method2 = 'summarizeShows';
    const reply2 = await call((done) => clients.showsShowQueryClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      change: {
        children: [{
          children: [{ parent_id: (request?.context?.actor?.user_id ?? '') }],
          parent_id: (request?.context?.actor?.user_id ?? ''),
        }],
        parent_id: (request?.context?.actor?.user_id ?? ''),
      },
      context: request?.context,
    };
    const messages3 = await exchange(clients.venuesVenueSectionSyncClient.streamVenueSectionChanges(metadataFor(request3.context), { deadline: deadline() }), [request3]);
    const reply3 = messages3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
    return assemble(
      {
        performance: {
          organizer: { display_name: (reply1?.lineup?.counterparty?.display_name ?? '') },
        },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
      },
      {
        performance: {
          location: request?.performance?.location,
          performance_id: (request?.performance_id ?? ''),
        },
      },
    );
  }

  /**
   * Starts a performance.
   */
  async function startPerformanceWorkflow(request) {
    validateContext(request.context);
    if (request.performance) {
      await store.save(store.toPerformanceRecord(request.context.tenant_id, request.performance));
    }
    await store.record(request.context.tenant_id, 'StartPerformanceWorkflow', request);
    logger.info('start_performance_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const client1 = clients.registry.get('acme.pipelines.v1.PipelineQueryService');
    const reply1 = await call((done) => client1.resolvePipelines(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { approver: { email: (request?.performance?.organizer?.email ?? '') } },
      lineup: { audit: { etag: (request?.performance?.audit?.etag ?? '') } },
    };
    const send2 = clients.showsLineupApprovalClient.delegateLineupApproval.bind(clients.showsLineupApprovalClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          reference_ids: (reply1?.result?.reference_ids ?? []),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        result: {
          message: (reply2?.outcome?.message ?? ''),
          status: (reply2?.outcome?.status ?? ''),
        },
      },
      {
        performance: {
          location: request?.performance?.location,
          performance_id: (request?.performance_id ?? ''),
        },
      },
    );
  }

  return {
    advancePerformanceWorkflow: unary(advancePerformanceWorkflow),
    listPerformanceWorkflows: unary(listPerformanceWorkflows),
    pausePerformanceWorkflow: unary(pausePerformanceWorkflow),
    resumePerformanceWorkflow: unary(resumePerformanceWorkflow),
    retryPerformanceWorkflow: unary(retryPerformanceWorkflow),
    signalPerformanceWorkflow: unary(signalPerformanceWorkflow),
    startPerformanceWorkflow: unary(startPerformanceWorkflow),
  };
}

module.exports = { createPerformanceWorkflowHandlers };
