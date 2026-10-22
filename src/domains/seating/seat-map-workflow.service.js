'use strict';

/**
 * Runs the long-lived workflow that takes a seat map from start to finish.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { upload } = require('../../platform/streams');

/**
 * Serve SeatMapWorkflowService, recording every operation of the seating team.
 */
class SeatMapWorkflowService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'SeatMapWorkflowService' });
  }

  /**
   * Advances a seat map.
   */
  advanceSeatMapWorkflow(call, callback) {
    this.executeAdvanceSeatMapWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeAdvanceSeatMapWorkflow(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'AdvanceSeatMapWorkflow', request);
    this.logger.info('advance_seat_map_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        notify: {
          preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
        },
      },
      sponsor: {
        name: request?.details?.notify?.name,
        primary_address: { country_code: (request?.seat_map?.location?.country_code ?? '') },
        relationship_manager: { email: (request?.seat_map?.manager?.email ?? '') },
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.sponsorshipsSponsorWorkflowClient.cancelSponsorWorkflow(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.seatingSeatAssignmentReportingClient.scheduleSeatAssignmentReport(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.notify?.phone ?? '') });
    return assemble(
      {
        seat_map: {
          children: [{
            children: [{
              location: { country_code: (reply1?.sponsor?.primary_address?.country_code ?? '') },
            }],
          }],
        },
      },
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { details: (reply2?.result?.details ?? {}) },
      },
      {
        seat_map: {
          attributes: (request?.seat_map?.attributes ?? {}),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a seat map.
   */
  inspectSeatMapWorkflow(call, callback) {
    this.executeInspectSeatMapWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeInspectSeatMapWorkflow(request) {
    validateContext(request.context);
    if (!request.seat_map_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'seat_map_id is required');
    }
    await this.store.record(request.context.tenant_id, 'InspectSeatMapWorkflow', request);
    this.logger.info('inspect_seat_map_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.context?.baggage ?? {}) },
      context: request?.context,
      details: {
        effective_at: request?.context?.deadline,
        new_owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
      seat_map_id: (request?.seat_map_id ?? ''),
    };
    const reply1 = await withRetry(() => call((done) => this.clients.seatingSeatMapLifecycleClient.terminateSeatMap(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: {
        new_owner: { display_name: (reply1?.seat_map?.manager?.display_name ?? '') },
        notify_contact: { email: (reply1?.seat_map?.manager?.email ?? '') },
      },
      seat_map_id: (request?.seat_map_id ?? ''),
    };
    const task2 = deferred(() => call((done) => this.clients.seatingSeatMapLifecycleClient.renewSeatMap(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    return assemble(
      {
        seat_map: {
          children: [{ children: [{ seat_map_id: (reply1?.seat_map?.seat_map_id ?? '') }] }],
        },
      },
      {
        seat_map: {
          children: [{
            children: [{ audit: { created_at: reply2?.seat_map?.audit?.created_at } }],
          }],
          location: reply2?.seat_map?.location,
        },
      },
      { seat_map: { seat_map_id: (request?.seat_map_id ?? '') } },
    );
  }

  /**
   * Lists seat maps a page at a time.
   */
  listSeatMapWorkflows(call, callback) {
    this.executeListSeatMapWorkflows(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListSeatMapWorkflows(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListSeatMapWorkflows', request);
    this.logger.info('list_seat_map_workflows', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Pauses a seat map.
   */
  pauseSeatMapWorkflow(call, callback) {
    this.executePauseSeatMapWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePauseSeatMapWorkflow(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'PauseSeatMapWorkflow', request);
    this.logger.info('pause_seat_map_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      ledger_account: {
        audit: { created_at: request?.seat_map?.audit?.created_at },
        limits: [{ name: (request?.seat_map?.name ?? '') }],
      },
    };
    const reply1 = await this.bus.request('acme.seating.v1.SeatMapWorkflowService/PauseSeatMapWorkflow:0', request1);
    const request2 = {
      context: request?.context,
      seat_map: {
        children: [{
          children: [{
            audit: {
              created_at: reply1?.ledger_account?.audit?.created_at,
              created_by: (reply1?.ledger_account?.audit?.created_by ?? ''),
            },
            location: {
              country_code: (request?.seat_map?.location?.country_code ?? ''),
              postal_code: (request?.seat_map?.location?.postal_code ?? ''),
            },
          }],
        }],
      },
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.seatingSeatMapCalculationClient.recalculateSeatMaps(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { status: (reply1?.result?.status ?? '') },
        seat_map: {
          audit: { created_by: (reply1?.ledger_account?.audit?.created_by ?? '') },
        },
      },
      {
        result: { details: (reply2?.result?.details ?? {}) },
        seat_map: {
          children: [{ audit: { created_by: (reply2?.seat_map?.audit?.created_by ?? '') } }],
        },
      },
      {
        seat_map: {
          parent_id: (request?.seat_map?.parent_id ?? ''),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Resumes a seat map.
   */
  resumeSeatMapWorkflow(call, callback) {
    this.executeResumeSeatMapWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResumeSeatMapWorkflow(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'ResumeSeatMapWorkflow', request);
    this.logger.info('resume_seat_map_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.seat_map?.audit?.updated_at,
        labels: { ...(request?.annotations ?? {}) },
        owner_id: (request?.seat_map?.seat_map_id ?? ''),
        region: (request?.seat_map?.location?.region ?? ''),
      },
    };
    const reply1 = await call((done) => this.clients.seatingSeatHoldReportingClient.listSeatHoldReports(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      record: {
        organizer: { user_id: (request?.seat_map?.manager?.user_id ?? '') },
        participants: [{
          contact: {
            email: (request?.details?.notify?.email ?? ''),
            name: request?.details?.notify?.name,
          },
        }],
      },
    };
    const reply2 = await upload(
      (done) => this.clients.pipelinesPipelineClient.batchCreatePipelines(metadataFor(request2.context), { deadline: deadline() }, done),
      [request2],
    );
    const request3 = {
      context: request?.context,
      seat_hold: {
        organizer: { display_name: (request?.seat_map?.manager?.display_name ?? '') },
      },
    };
    const reply3 = await new Promise((resolve, reject) => {
      this.clients.seatingSeatHoldApprovalClient.submitSeatHold(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { message: (reply1?.result?.message ?? '') } },
      {
        seat_map: {
          children: [{
            children: [{
              audit: { created_by: (reply3?.seat_hold?.audit?.created_by ?? '') },
              location: reply3?.seat_hold?.location,
            }],
          }],
        },
      },
      {
        seat_map: {
          seat_map_id: (request?.seat_map_id ?? ''),
          status: (request?.seat_map?.status ?? ''),
        },
      },
    );
  }

  /**
   * Retrys a seat map.
   */
  retrySeatMapWorkflow(call, callback) {
    this.executeRetrySeatMapWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeRetrySeatMapWorkflow(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'RetrySeatMapWorkflow', request);
    this.logger.info('retry_seat_map_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_map_id: (request?.seat_map?.seat_map_id ?? ''),
    };
    const peekSeatMap1 = promisify(this.clients.seatingSeatMapQueryClient.peekSeatMap.bind(this.clients.seatingSeatMapQueryClient));
    const reply1 = await peekSeatMap1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      rider: { audit: { created_by: (reply1?.seat_map?.audit?.created_by ?? '') } },
    };
    const reply2 = await call((done) => this.clients.performersRiderApprovalClient.commentRiderApproval(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        seat_map: {
          children: [{ children: [{ path: (reply1?.seat_map?.path ?? []) }] }],
          path: (reply1?.seat_map?.path ?? []),
        },
      },
      {
        seat_map: {
          children: [{ audit: { created_at: reply2?.rider?.audit?.created_at } }],
        },
      },
      {
        seat_map: {
          name: (request?.seat_map?.name ?? ''),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Signals a seat map.
   */
  signalSeatMapWorkflow(call, callback) {
    this.executeSignalSeatMapWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSignalSeatMapWorkflow(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'SignalSeatMapWorkflow', request);
    this.logger.info('signal_seat_map_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      etag: (request?.seat_map?.audit?.etag ?? ''),
      reason: (request?.reason ?? ''),
      seat_map_id: (request?.seat_map?.seat_map_id ?? ''),
    };
    const method1 = 'deleteSeatMap';
    const reply1 = await call((done) => this.clients.seatingSeatMapClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      concession_stand: {
        audit: {
          etag: (request?.seat_map?.audit?.etag ?? ''),
          updated_at: request?.seat_map?.audit?.updated_at,
        },
      },
      context: request?.context,
      details: { notify: { email: (request?.details?.notify?.email ?? '') } },
    };
    const client2 = this.clients.registry.get('acme.concessions.v1.ConcessionStandWorkflowService');
    const reply2 = await call((done) => client2.resumeConcessionStandWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      venue: {
        address: { postal_code: (request?.seat_map?.location?.postal_code ?? '') },
        audit: { created_by: (reply2?.concession_stand?.audit?.created_by ?? '') },
        owner: { display_name: (reply2?.concession_stand?.owner?.display_name ?? '') },
      },
    };
    const send3 = this.clients.venuesVenueAdminClient.repairVenue.bind(this.clients.venuesVenueAdminClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.notify?.preferred_channel ?? '') });
    return assemble(
      {
        outcome: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      { outcome: { status: (reply2?.result?.status ?? '') } },
      {
        seat_map: {
          location: {
            locality: ((reply3?.venue?.locator === 'address' ? reply3?.venue.address : undefined)?.locality ?? ''),
          },
        },
      },
      {
        seat_map: {
          parent_id: (request?.seat_map?.parent_id ?? ''),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }

  /**
   * Starts a seat map.
   */
  startSeatMapWorkflow(call, callback) {
    this.executeStartSeatMapWorkflow(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeStartSeatMapWorkflow(request) {
    validateContext(request.context);
    if (request.seat_map) {
      await this.store.save(this.store.toSeatMapRecord(request.context.tenant_id, request.seat_map));
    }
    await this.store.record(request.context.tenant_id, 'StartSeatMapWorkflow', request);
    this.logger.info('start_seat_map_workflow', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      seat_hold: {
        organizer: { user_id: (request?.seat_map?.manager?.user_id ?? '') },
        participants: [{
          contact: {
            preferred_channel: (request?.details?.notify?.preferred_channel ?? ''),
          },
        }],
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.seatingSeatHoldApprovalClient.rejectSeatHold(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    const { context: context2, ...body2 } = request;
    const input2 = { ...body2, context: context2 };
    const request2 = {
      context: input2?.context,
      tour_stop: {
        audit: { created_by: (input2?.seat_map?.audit?.created_by ?? '') },
        participants: [{
          contact: {
            email: (reply1?.seat_hold?.organizer?.email ?? ''),
            name: input2?.details?.notify?.name,
          },
        }],
      },
    };
    const reply2 = await call((done) => this.clients.toursTourStopWorkflowClient.retryTourStopWorkflow(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.seat_map?.audit?.updated_at,
        owner_id: (request?.seat_map?.manager?.user_id ?? ''),
        region: (reply2?.tour_stop?.location?.region ?? ''),
      },
    };
    const cloned3 = JSON.parse(JSON.stringify(request3));
    const reply3 = await call((done) => this.clients.seatingSeatMapApprovalClient.listPendingSeatMaps(cloned3, metadataFor(cloned3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      filter: {
        created_after: reply2?.tour_stop?.audit?.updated_at,
        owner_id: (reply2?.tour_stop?.tour_stop_id ?? ''),
        region: (reply2?.tour_stop?.location?.region ?? ''),
      },
    };
    const copied4 = structuredClone(request4);
    const reply4 = await call((done) => this.clients.venuesVenueSectionHistoryClient.listVenueSectionVersions(copied4, metadataFor(copied4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
        seat_map: {
          children: [{
            children: [{
              location: { country_code: (reply1?.seat_hold?.location?.country_code ?? '') },
            }],
          }],
        },
      },
      {
        seat_map: {
          children: [{
            location: { country_code: (reply2?.tour_stop?.location?.country_code ?? '') },
          }],
          manager: { email: (reply2?.tour_stop?.organizer?.email ?? '') },
        },
      },
      { result: { reference_ids: (reply3?.result?.reference_ids ?? []) } },
      { result: { message: (reply4?.result?.message ?? '') } },
      {
        seat_map: {
          depth: (request?.seat_map?.depth ?? 0),
          seat_map_id: (request?.seat_map_id ?? ''),
        },
      },
    );
  }
}

module.exports = { SeatMapWorkflowService };
