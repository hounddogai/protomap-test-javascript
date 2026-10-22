'use strict';

/**
 * Runs the long-lived workflow that takes a tour stop from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');
const { collect, exchange } = require('../../../platform/streams');

/**
 * The handlers of TourStopWorkflowService, over the dependencies the tours team is given.
 */
function createTourStopWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a tour stop.
   */
  async function advanceTourStopWorkflow(request) {
    validateContext(request.context);
    if (request.tour_stop) {
      await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
    }
    await store.record(request.context.tenant_id, 'AdvanceTourStopWorkflow', request);
    logger.info('advance_tour_stop_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          phone: (request?.details?.notify?.phone ?? ''),
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
      venue_policy: { audit: { created_at: request?.tour_stop?.audit?.created_at } },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => clients.venuesVenuePolicyExportClient.retryVenuePolicyExport(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.outcome?.message ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        tour_stop: {
          status: (request?.tour_stop?.status ?? ''),
          tour_stop_id: (request?.tour_stop_id ?? ''),
        },
      },
    );
  }

  /**
   * Cancels a tour stop.
   */
  async function cancelTourStopWorkflow(request) {
    validateContext(request.context);
    if (request.tour_stop) {
      await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
    }
    await store.record(request.context.tenant_id, 'CancelTourStopWorkflow', request);
    logger.info('cancel_tour_stop_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      tour_budget: { audit: { etag: (input1?.tour_stop?.audit?.etag ?? '') } },
    };
    const reply1 = await call((done) => clients.toursTourBudgetApprovalClient.submitTourBudget(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        approver: {
          display_name: (request?.tour_stop?.organizer?.display_name ?? ''),
          email: (request?.details?.notify?.email ?? ''),
        },
      },
      wristband: {
        audit: {
          created_at: reply1?.tour_budget?.audit?.created_at,
          updated_at: request?.tour_stop?.audit?.updated_at,
        },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.admissionsWristbandApprovalClient.submitWristband(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        tour_stop: { audit: { created_by: (reply1?.tour_budget?.audit?.created_by ?? '') } },
      },
      { outcome: { status: (reply2?.result?.status ?? '') } },
      {
        tour_stop: {
          organizer: request?.tour_stop?.organizer,
          tour_stop_id: (request?.tour_stop_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a tour stop.
   */
  async function inspectTourStopWorkflow(request) {
    validateContext(request.context);
    if (!request.tour_stop_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tour_stop_id is required');
    }
    await store.record(request.context.tenant_id, 'InspectTourStopWorkflow', request);
    logger.info('inspect_tour_stop_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      wristband: {
        audit: { updated_at: request?.context?.deadline },
        components: { primary: { component_id: (request?.tour_stop_id ?? '') } },
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.admissionsWristbandApprovalClient.approveWristband(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    return assemble(
      {
        tour_stop: { audit: { updated_at: reply1?.wristband?.audit?.updated_at } },
      },
      { tour_stop: { tour_stop_id: (request?.tour_stop_id ?? '') } },
    );
  }

  /**
   * Lists tour stops a page at a time.
   */
  async function listTourStopWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListTourStopWorkflows', request);
    logger.info('list_tour_stop_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Pauses a tour stop.
   */
  async function pauseTourStopWorkflow(request) {
    validateContext(request.context);
    if (request.tour_stop) {
      await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
    }
    await store.record(request.context.tenant_id, 'PauseTourStopWorkflow', request);
    logger.info('pause_tour_stop_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.tour_stop?.audit?.updated_at,
        region: (request?.tour_stop?.location?.region ?? ''),
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.benefitsBenefitPlanQueryClient.browseBenefitPlans(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const results2 = await Promise.all((request?.tour_stop?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        subscriber: { preferred_channel: (item?.contact?.preferred_channel ?? '') },
      };
      const messages2 = await collect(clients.toursTourClient.watchTours(request2, metadataFor(request2.context), { deadline: deadline() }));
      const reply2 = messages2.at(-1) ?? {};
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply1?.result?.status ?? '') } },
      {
        tour_stop: {
          organizer: request?.tour_stop?.organizer,
          tour_stop_id: (request?.tour_stop_id ?? ''),
        },
      },
    );
  }

  /**
   * Resumes a tour stop.
   */
  async function resumeTourStopWorkflow(request) {
    validateContext(request.context);
    if (request.tour_stop) {
      await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
    }
    await store.record(request.context.tenant_id, 'ResumeTourStopWorkflow', request);
    logger.info('resume_tour_stop_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.tour_stop?.participants ?? [])) {
      const request1 = {
        context: request?.context,
        tour: {
          organizer: { email: (item?.contact?.email ?? '') },
          participants: [{
            contact: {
              name: item?.contact?.name,
              phone: (item?.contact?.phone ?? ''),
              preferred_channel: (item?.contact?.preferred_channel ?? ''),
            },
          }],
        },
      };
      const reply1 = await call((done) => clients.toursTourWorkflowClient.cancelTourWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    const results2 = await Promise.all((request?.tour_stop?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        details: {
          contact: {
            name: item?.contact?.name,
            phone: (item?.contact?.phone ?? ''),
          },
        },
      };
      const reply2 = await call((done) => clients.metricsMetricSeriesValidationClient.checkMetricSeriesEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      tour_stop: {
        metadata: (request?.tour_stop?.metadata ?? {}),
        tour_stop_id: (request?.tour_stop_id ?? ''),
      },
    };
  }

  /**
   * Retrys a tour stop.
   */
  async function retryTourStopWorkflow(request) {
    validateContext(request.context);
    if (request.tour_stop) {
      await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
    }
    await store.record(request.context.tenant_id, 'RetryTourStopWorkflow', request);
    logger.info('retry_tour_stop_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      tour_stop: {
        daily: {
          interval: ((request?.tour_stop?.recurrence === 'weekly' ? request?.tour_stop.weekly : undefined)?.interval ?? 0),
        },
        participants: [{
          contact: {
            email: (request?.tour_stop?.organizer?.email ?? ''),
            phone: (request?.details?.notify?.phone ?? ''),
          },
        }],
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.toursTourStopApprovalClient.submitTourStop(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: { notify: { email: (reply1?.tour_stop?.organizer?.email ?? '') } },
    };
    const task2 = deferred(() => call((done) => clients.admissionsWristbandWorkflowClient.cancelWristbandWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    const settled3 = await Promise.allSettled((request?.tour_stop?.participants ?? []).map(async (item) => {
      const request3 = {
        concession_order: {
          audit: { updated_by: (reply2?.wristband?.audit?.updated_by ?? '') },
          counterparty: { display_name: (reply2?.wristband?.owner?.display_name ?? '') },
        },
        context: request?.context,
        details: { approver: { email: (item?.contact?.email ?? '') } },
      };
      const reply3 = await call((done) => clients.concessionsConcessionOrderApprovalClient.rejectConcessionOrder(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const results3 = settled3.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply3 = results3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          effective_at: reply1?.outcome?.effective_at,
          next_steps: (reply1?.outcome?.next_steps ?? []),
        },
      },
      {
        tour_stop: {
          location: {
            recipient: ((reply2?.wristband?.locator === 'address' ? reply2?.wristband.address : undefined)?.recipient ?? ''),
          },
          organizer: { email: (reply2?.wristband?.owner?.email ?? '') },
        },
      },
      {
        tour_stop: {
          metadata: (request?.tour_stop?.metadata ?? {}),
          tour_stop_id: (request?.tour_stop_id ?? ''),
        },
      },
    );
  }

  /**
   * Signals a tour stop.
   */
  async function signalTourStopWorkflow(request) {
    validateContext(request.context);
    if (request.tour_stop) {
      await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
    }
    await store.record(request.context.tenant_id, 'SignalTourStopWorkflow', request);
    logger.info('signal_tour_stop_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.tour_stop?.participants ?? [])) {
      const request1 = {
        context: request?.context,
        gift_card: {
          contact: {
            email: (item?.contact?.email ?? ''),
            name: item?.contact?.name,
            phone: (item?.contact?.phone ?? ''),
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply1 = await call((done) => clients.giftcardsGiftCardCalculationClient.simulateGiftCard(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      tour_stop: {
        audit: { etag: (request?.tour_stop?.audit?.etag ?? '') },
        participants: [{
          contact: {
            email: (reply1?.gift_card?.contact?.email ?? ''),
            name: request?.details?.notify?.name,
          },
        }],
      },
    };
    const reply2 = await bus.request('acme.tours.v1.TourStopWorkflowService/SignalTourStopWorkflow:1', request2);
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply2?.result?.status ?? '') } },
      {
        tour_stop: {
          location: request?.tour_stop?.location,
          tour_stop_id: (request?.tour_stop_id ?? ''),
        },
      },
    );
  }

  /**
   * Starts a tour stop.
   */
  async function startTourStopWorkflow(request) {
    validateContext(request.context);
    if (request.tour_stop) {
      await store.save(store.toTourStopRecord(request.context.tenant_id, request.tour_stop));
    }
    await store.record(request.context.tenant_id, 'StartTourStopWorkflow', request);
    logger.info('start_tour_stop_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.tour_stop?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        filter: {
          created_after: request?.tour_stop?.audit?.updated_at,
          owner_id: (item?.participant_id ?? ''),
          region: (request?.tour_stop?.location?.region ?? ''),
        },
      };
      const reply1 = await call((done) => clients.venuesVenuePolicyHistoryClient.listVenuePolicyVersions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const settled2 = await Promise.allSettled((request?.tour_stop?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        details: {
          recipient: {
            email: (item?.contact?.email ?? ''),
            name: item?.contact?.name,
            phone: (item?.contact?.phone ?? ''),
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply2 = await call((done) => clients.concessionsConcessionOrderReportingClient.shareConcessionOrderReport(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    const request3 = { context: request?.context };
    const reply3 = await runInContext(request.context, () => call((done) => clients.featureflagsFeatureFlagClient.batchGetFeatureFlags(request3, metadataFor(request3.context), { deadline: deadline() }, done)));
    const request4 = {
      change: {
        block: {
          notify_email: (request?.context?.actor?.email ?? ''),
          reason: (request?.reason ?? ''),
        },
        effective: request?.tour_stop?.window,
      },
      context: request?.context,
    };
    const messages4 = await exchange(clients.venuesVenuePolicySyncClient.streamVenuePolicyChanges(metadataFor(request4.context), { deadline: deadline() }), [request4]);
    const reply4 = messages4.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
    return assemble(
      { outcome: { message: (reply3?.result?.message ?? '') } },
      {
        tour_stop: {
          status: (request?.tour_stop?.status ?? ''),
          tour_stop_id: (request?.tour_stop_id ?? ''),
        },
      },
    );
  }

  return {
    advanceTourStopWorkflow: unary(advanceTourStopWorkflow),
    cancelTourStopWorkflow: unary(cancelTourStopWorkflow),
    inspectTourStopWorkflow: unary(inspectTourStopWorkflow),
    listTourStopWorkflows: unary(listTourStopWorkflows),
    pauseTourStopWorkflow: unary(pauseTourStopWorkflow),
    resumeTourStopWorkflow: unary(resumeTourStopWorkflow),
    retryTourStopWorkflow: unary(retryTourStopWorkflow),
    signalTourStopWorkflow: unary(signalTourStopWorkflow),
    startTourStopWorkflow: unary(startTourStopWorkflow),
  };
}

module.exports = { createTourStopWorkflowHandlers };
