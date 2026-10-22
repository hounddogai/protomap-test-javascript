'use strict';

/**
 * Answers read-only questions about gate scans, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');
const { collect } = require('../../platform/streams');

/**
 * Serve GateScanQueryService, recording every operation of the admissions team.
 */
class GateScanQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'GateScanQueryService' });
  }

  /**
   * Lists gate scans a page at a time.
   */
  browseGateScans(call, callback) {
    this.executeBrowseGateScans(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowseGateScans(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowseGateScans', request);
    this.logger.info('browse_gate_scans', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.context?.baggage ?? {}) },
      context: request?.context,
      details: { filters: { ...(request?.filter?.labels ?? {}) } },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.venuesVenueExportClient.scheduleVenueExport(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.filter?.labels ?? {}) },
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const messages2 = await collect(this.clients.admissionsAdmissionQueryClient.exportAdmissions(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      gate_scans: [{ correlation_id: (reply1?.result?.correlation_id ?? '') }],
      result: { status: (reply1?.result?.status ?? '') },
    };
  }

  /**
   * Counts gate scans by status.
   */
  countGateScans(call, callback) {
    this.executeCountGateScans(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCountGateScans(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'CountGateScans', request);
    this.logger.info('count_gate_scans', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        owner_id: (request?.filter?.owner_id ?? ''),
        query: (request?.filter?.query ?? ''),
        region: (request?.filter?.region ?? ''),
      },
    };
    const merged1 = Object.assign({}, request1, { context: request1.context });
    const reply1 = await call((done) => this.clients.venuesVenueSectionSyncClient.listVenueSectionConflicts(merged1, metadataFor(merged1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } };
  }

  /**
   * Streams every matching gate scan.
   */
  exportGateScans(call) {
    serverStream((request) => this.executeExportGateScans(request))(call);
  }

  async executeExportGateScans(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportGateScans', request);
    this.logger.info('export_gate_scans', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
      gate_scan: {
        attributes: { ...(request?.filter?.labels ?? {}) },
        measurements: [{ observed_at: request?.filter?.created_after }],
        source: (request?.filter?.owner_id ?? ''),
      },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.admissionsGateScanCalculationClient.calculateGateScan(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      admission: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        subject: { gate_scan_id: (reply1?.gate_scan?.gate_scan_id ?? '') },
      },
      context: request?.context,
    };
    const task2 = deferred(() => call((done) => this.clients.admissionsAdmissionCalculationClient.recalculateAdmissions(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      { gate_scan: { occurred_at: reply1?.gate_scan?.occurred_at } },
      {
        gate_scan: {
          subject: {
            admission_id: (reply2?.admission?.admission_id ?? ''),
            kind: (reply2?.admission?.kind ?? ''),
          },
        },
      },
    );
  }

  /**
   * Lists gate scans a page at a time.
   */
  findGateScans(call, callback) {
    this.executeFindGateScans(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindGateScans(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindGateScans', request);
    this.logger.info('find_gate_scans', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { destination: { region: (request?.filter?.region ?? '') } },
    };
    const reply1 = await this.bus.request('acme.admissions.v1.GateScanQueryService/FindGateScans:0', request1);
    const request2 = {
      admission: {
        source: (reply1?.gate_scan?.source ?? ''),
        subject: {
          gate_scan_id: (reply1?.gate_scan?.gate_scan_id ?? ''),
          kind: (reply1?.gate_scan?.kind ?? ''),
        },
      },
      context: request?.context,
    };
    const reply2 = await runInContext(request.context, () => call((done) => this.clients.admissionsAdmissionValidationClient.checkAdmissionEligibility(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        gate_scans: [{ correlation_id: (reply1?.gate_scan?.correlation_id ?? '') }],
      },
      {
        gate_scans: [{ subject: { kind: (reply2?.admission?.subject?.kind ?? '') } }],
        result: { message: (reply2?.outcome?.message ?? '') },
      },
    );
  }

  /**
   * Returns a gate scan.
   */
  lookupGateScan(call, callback) {
    this.executeLookupGateScan(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupGateScan(request) {
    validateContext(request.context);
    if (!request.gate_scan_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupGateScan', request);
    this.logger.info('lookup_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { locale: (request?.context?.locale ?? '') },
      wristband: {
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
      },
    };
    const reply1 = await call((done) => this.clients.admissionsWristbandValidationClient.normalizeWristband(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return assemble(
      { gate_scan: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      { gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') } },
    );
  }

  /**
   * Returns a gate scan.
   */
  peekGateScan(call, callback) {
    this.executePeekGateScan(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executePeekGateScan(request) {
    validateContext(request.context);
    if (!request.gate_scan_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'gate_scan_id is required');
    }
    await this.store.record(request.context.tenant_id, 'PeekGateScan', request);
    this.logger.info('peek_gate_scan', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
      replay_clip: { owner: { user_id: (request?.context?.actor?.user_id ?? '') } },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.broadcastsReplayClipCalculationClient.explainReplayClipCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    let selected2_1;
    switch (reply1?.replay_clip?.locator) {
      case 'address':
        selected2_1 = ((reply1?.replay_clip?.locator === 'address' ? reply1?.replay_clip.address : undefined)?.region ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.replay_clip?.audit?.updated_at,
        labels: { ...(reply1?.result?.details ?? {}) },
        owner_id: (reply1?.replay_clip?.owner?.user_id ?? ''),
        region: selected2_1,
      },
    };
    const reply2 = await call((done) => this.clients.searchSearchIndexQueryClient.suggestSearchIndexes(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      { result: { reference_ids: (reply2?.result?.reference_ids ?? []) } },
      { gate_scan: { gate_scan_id: (request?.gate_scan_id ?? '') } },
    );
  }

  /**
   * Returns several gate scans at once.
   */
  resolveGateScans(call, callback) {
    this.executeResolveGateScans(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolveGateScans(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolveGateScans', request);
    this.logger.info('resolve_gate_scans', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
    };
    const reply1 = await call((done) => this.clients.authorizationAccessPolicyValidationClient.previewAccessPolicy(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return {
      gate_scans: [{ correlation_id: (reply1?.result?.correlation_id ?? '') }],
      result: { message: (reply1?.result?.message ?? '') },
    };
  }

  /**
   * Lists gate scans a page at a time.
   */
  suggestGateScans(call, callback) {
    this.executeSuggestGateScans(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSuggestGateScans(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SuggestGateScans', request);
    this.logger.info('suggest_gate_scans', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      audit_event: { occurred_at: request?.filter?.created_after },
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
    };
    const method1 = 'normalizeAuditEvent';
    const reply1 = await call((done) => this.clients.auditAuditEventValidationClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = { context: request?.context };
    const client2 = this.clients.registry.get('acme.admissions.v1.AdmissionQueryService');
    const reply2 = await call((done) => client2.resolveAdmissions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          message: (reply1?.outcome?.message ?? ''),
        },
      },
      { page_info: { total_size: (reply2?.page_info?.total_size ?? '0') } },
    );
  }

  /**
   * Summarizes a gate scan.
   */
  summarizeGateScans(call, callback) {
    this.executeSummarizeGateScans(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizeGateScans(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizeGateScans', request);
    this.logger.info('summarize_gate_scans', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      reason: (request?.reason ?? ''),
    };
    const send1 = this.clients.concessionsConcessionStandValidationClient.validateConcessionStand.bind(this.clients.concessionsConcessionStandValidationClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: reply1?.concession_stand?.audit?.updated_at,
        labels: { ...(request?.details?.filters ?? {}) },
        owner_id: (reply1?.concession_stand?.components?.primary?.component_id ?? ''),
        region: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.region ?? ''),
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => this.clients.notificationsNotificationClient.searchNotifications(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      admission: {
        measurements: [{ unit: (reply1?.concession_stand?.dimensions?.unit ?? '') }],
        subject: { display_name: (reply1?.concession_stand?.owner?.display_name ?? '') },
      },
      context: input3?.context,
    };
    const reply3 = await call((done) => this.clients.admissionsAdmissionValidationClient.checkAdmissionEligibility(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      gate_scan: {
        correlation_id: (reply2?.result?.correlation_id ?? ''),
        gate_scan_id: (reply3?.admission?.subject?.gate_scan_id ?? ''),
        subject: {
          display_name: (reply3?.admission?.subject?.display_name ?? ''),
          kind: (reply3?.admission?.subject?.kind ?? ''),
        },
      },
    };
    const cloned4 = JSON.parse(JSON.stringify(request4));
    const reply4 = await call((done) => this.clients.admissionsGateScanValidationClient.lintGateScans(cloned4, metadataFor(cloned4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          message: (reply1?.result?.message ?? ''),
        },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          message: (reply2?.result?.message ?? ''),
        },
      },
      { result: { message: (reply3?.result?.message ?? '') } },
      {
        result: {
          correlation_id: (reply4?.result?.correlation_id ?? ''),
          status: (reply4?.outcome?.status ?? ''),
        },
      },
      { period: request?.details?.period },
    );
  }
}

module.exports = { GateScanQueryService };
