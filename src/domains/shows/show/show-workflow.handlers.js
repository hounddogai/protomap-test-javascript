'use strict';

/**
 * Runs the long-lived workflow that takes a show from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of ShowWorkflowService, over the dependencies the shows team is given.
 */
function createShowWorkflowHandlers({ store, clients, bus, logger }) {
  /**
   * Advances a show.
   */
  async function advanceShowWorkflow(request) {
    validateContext(request.context);
    if (request.show) {
      await store.save(store.toShowRecord(request.context.tenant_id, request.show));
    }
    await store.record(request.context.tenant_id, 'AdvanceShowWorkflow', request);
    logger.info('advance_show_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.show?.audit?.created_at,
        owner_id: (request?.show?.show_id ?? ''),
        region: (request?.show?.location?.region ?? ''),
      },
    };
    const listPendingTourStops1 = promisify(clients.toursTourStopApprovalClient.listPendingTourStops.bind(clients.toursTourStopApprovalClient));
    const reply1 = await listPendingTourStops1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      etag: (request?.show?.audit?.etag ?? ''),
      reason: (request?.reason ?? ''),
      show_id: (request?.show?.show_id ?? ''),
    };
    const reply2 = await call((done) => clients.showsShowClient.deleteShow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
    return assemble(
      {
        outcome: { status: (reply1?.result?.status ?? '') },
        result: { status: (reply1?.result?.status ?? '') },
      },
      {
        outcome: { effective_at: reply2?.effective_at },
        result: { details: (reply2?.result?.details ?? {}) },
      },
      {
        show: {
          organizer: request?.show?.organizer,
          show_id: (request?.show_id ?? ''),
        },
      },
    );
  }

  /**
   * Cancels a show.
   */
  async function cancelShowWorkflow(request) {
    validateContext(request.context);
    if (request.show) {
      await store.save(store.toShowRecord(request.context.tenant_id, request.show));
    }
    await store.record(request.context.tenant_id, 'CancelShowWorkflow', request);
    logger.info('cancel_show_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.show?.participants ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        details: {
          notify: {
            name: item?.contact?.name,
            preferred_channel: (item?.contact?.preferred_channel ?? ''),
          },
        },
      };
      const reply1 = await call((done) => clients.sponsorshipsSponsorshipDealWorkflowClient.retrySponsorshipDealWorkflow(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.show?.audit?.updated_at,
        owner_id: (reply1?.sponsorship_deal?.counterparty?.sponsor_id ?? ''),
        region: (reply1?.sponsorship_deal?.billing_address?.region ?? ''),
      },
    };
    const method2 = 'listVenuePolicyConflicts';
    const reply2 = await call((done) => clients.venuesVenuePolicySyncClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.sponsorship_deal?.audit?.updated_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (reply1?.sponsorship_deal?.sponsorship_deal_id ?? ''),
        region: (reply1?.sponsorship_deal?.billing_address?.region ?? ''),
      },
    };
    const client3 = clients.registry.get('acme.shows.v1.PerformanceQueryService');
    const reply3 = await call((done) => client3.browsePerformances(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
      },
      { result: { message: (reply3?.result?.message ?? '') } },
      {
        show: {
          show_id: (request?.show_id ?? ''),
          status: (request?.show?.status ?? ''),
        },
      },
    );
  }

  /**
   * Returns a show.
   */
  async function inspectShowWorkflow(request) {
    validateContext(request.context);
    if (!request.show_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'show_id is required');
    }
    await store.record(request.context.tenant_id, 'InspectShowWorkflow', request);
    logger.info('inspect_show_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { show: { show_id: (request?.show_id ?? '') } };
  }

  /**
   * Lists shows a page at a time.
   */
  async function listShowWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await store.record(request.context.tenant_id, 'ListShowWorkflows', request);
    logger.info('list_show_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Pauses a show.
   */
  async function pauseShowWorkflow(request) {
    validateContext(request.context);
    if (request.show) {
      await store.save(store.toShowRecord(request.context.tenant_id, request.show));
    }
    await store.record(request.context.tenant_id, 'PauseShowWorkflow', request);
    logger.info('pause_show_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.show?.participants ?? [])) {
      const request1 = {
        context: request?.context,
        show: {
          organizer: { email: (item?.contact?.email ?? '') },
          participants: [{
            contact: {
              name: item?.contact?.name,
              preferred_channel: (item?.contact?.preferred_channel ?? ''),
            },
            required: (item?.required ?? false),
          }],
        },
      };
      const reply1 = await call((done) => clients.showsShowApprovalClient.delegateShowApproval(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.show?.audit?.created_at,
        region: (reply1?.show?.location?.region ?? ''),
      },
    };
    const send2 = clients.venuesVenueHistoryClient.listVenueVersions.bind(clients.venuesVenueHistoryClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      audit_event: {
        subject: { display_name: (request?.show?.organizer?.display_name ?? '') },
      },
      context: request?.context,
    };
    const spread3 = {
      ...request3,
      context: request3.context ? { ...request3.context, baggage: { ...request3.context.baggage } } : null,
    };
    const reply3 = await call((done) => clients.auditAuditEventClient.updateAuditEvent(spread3, metadataFor(spread3.context), { deadline: deadline() }, done));
    const { context: context4, ...body4 } = request;
    const input4 = { ...body4, context: context4 };
    const request4 = {
      context: input4?.context,
      details: {
        destination: {
          country_code: (input4?.show?.location?.country_code ?? ''),
          lines: [...(input4?.show?.location?.lines ?? [])],
        },
      },
      shipment: {
        audit: { updated_at: input4?.show?.audit?.updated_at },
        billing_address: { region: (input4?.show?.location?.region ?? '') },
      },
    };
    const reply4 = await call((done) => clients.shippingShipmentCalculationClient.estimateShipment(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { reference_ids: (reply2?.result?.reference_ids ?? []) },
      },
      {
        result: { details: (reply3?.result?.details ?? {}) },
        show: {
          organizer: { display_name: (reply3?.audit_event?.subject?.display_name ?? '') },
        },
      },
      {
        result: { reference_ids: (reply4?.result?.reference_ids ?? []) },
        show: { audit: { created_at: reply4?.shipment?.audit?.created_at } },
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
   * Resumes a show.
   */
  async function resumeShowWorkflow(request) {
    validateContext(request.context);
    if (request.show) {
      await store.save(store.toShowRecord(request.context.tenant_id, request.show));
    }
    await store.record(request.context.tenant_id, 'ResumeShowWorkflow', request);
    logger.info('resume_show_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      menu_item: {
        audit: {
          created_by: (request?.show?.audit?.created_by ?? ''),
          updated_at: request?.show?.audit?.updated_at,
        },
        owner: {
          email: (request?.show?.organizer?.email ?? ''),
          user_id: (request?.show?.organizer?.user_id ?? ''),
        },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.concessionsMenuItemApprovalClient.rejectMenuItem(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        show: {
          location: {
            lines: ((reply1?.menu_item?.locator === 'address' ? reply1?.menu_item.address : undefined)?.lines ?? []),
          },
        },
      },
      {
        show: {
          organizer: request?.show?.organizer,
          show_id: (request?.show_id ?? ''),
        },
      },
    );
  }

  /**
   * Retrys a show.
   */
  async function retryShowWorkflow(request) {
    validateContext(request.context);
    if (request.show) {
      await store.save(store.toShowRecord(request.context.tenant_id, request.show));
    }
    await store.record(request.context.tenant_id, 'RetryShowWorkflow', request);
    logger.info('retry_show_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      metric_series: {
        audit: {
          created_at: request?.show?.audit?.created_at,
          updated_at: request?.show?.audit?.updated_at,
        },
        owner: { display_name: (request?.show?.organizer?.display_name ?? '') },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => clients.metricsMetricSeriesClient.updateMetricSeries(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        show: {
          location: {
            region: ((reply1?.metric_series?.locator === 'address' ? reply1?.metric_series.address : undefined)?.region ?? ''),
          },
          organizer: { email: (reply1?.metric_series?.owner?.email ?? '') },
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
   * Signals a show.
   */
  async function signalShowWorkflow(request) {
    validateContext(request.context);
    if (request.show) {
      await store.save(store.toShowRecord(request.context.tenant_id, request.show));
    }
    await store.record(request.context.tenant_id, 'SignalShowWorkflow', request);
    logger.info('signal_show_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_hold: {
        location: {
          country_code: (request?.show?.location?.country_code ?? ''),
          recipient: (request?.show?.location?.recipient ?? ''),
        },
        participants: [{ contact: { email: (request?.details?.notify?.email ?? '') } }],
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => clients.seatingSeatHoldApprovalClient.approveSeatHold(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    const results2 = await Promise.all((request?.show?.participants ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        performance: { participants: [{ contact: { phone: (item?.contact?.phone ?? '') } }] },
      };
      const reply2 = await call((done) => clients.showsPerformanceApprovalClient.rejectPerformance(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        show: { organizer: { email: (reply1?.seat_hold?.organizer?.email ?? '') } },
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
   * Starts a show.
   */
  async function startShowWorkflow(request) {
    validateContext(request.context);
    if (request.show) {
      await store.save(store.toShowRecord(request.context.tenant_id, request.show));
    }
    await store.record(request.context.tenant_id, 'StartShowWorkflow', request);
    logger.info('start_show_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        destination: { country_code: (request?.show?.location?.country_code ?? '') },
      },
      performance: {
        audit: { updated_at: request?.show?.audit?.updated_at },
        daily: {
          interval: ((request?.show?.recurrence === 'daily' ? request?.show.daily : undefined)?.interval ?? 0),
        },
        participants: [{ contact: { name: request?.details?.notify?.name } }],
      },
    };
    const reply1 = await withRetry(() => call((done) => clients.showsPerformanceCalculationClient.recalculatePerformances(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      show: {
        location: { region: (reply1?.performance?.location?.region ?? '') },
        participants: [{
          contact: {
            email: (reply1?.performance?.organizer?.email ?? ''),
            name: request?.details?.notify?.name,
          },
        }],
      },
    };
    const task2 = deferred(() => call((done) => clients.showsShowApprovalClient.approveShow(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        show: {
          audit: { created_by: (reply1?.performance?.audit?.created_by ?? '') },
          location: { lines: (reply1?.performance?.location?.lines ?? []) },
        },
      },
      {
        outcome: { status: (reply2?.result?.status ?? '') },
        show: { status: (reply2?.show?.status ?? '') },
      },
      {
        show: {
          organizer: request?.show?.organizer,
          show_id: (request?.show_id ?? ''),
        },
      },
    );
  }

  return {
    advanceShowWorkflow: unary(advanceShowWorkflow),
    cancelShowWorkflow: unary(cancelShowWorkflow),
    inspectShowWorkflow: unary(inspectShowWorkflow),
    listShowWorkflows: unary(listShowWorkflows),
    pauseShowWorkflow: unary(pauseShowWorkflow),
    resumeShowWorkflow: unary(resumeShowWorkflow),
    retryShowWorkflow: unary(retryShowWorkflow),
    signalShowWorkflow: unary(signalShowWorkflow),
    startShowWorkflow: unary(startShowWorkflow),
  };
}

module.exports = { createShowWorkflowHandlers };
