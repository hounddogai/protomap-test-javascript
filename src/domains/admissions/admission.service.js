'use strict';

/**
 * Manages admissions: creates, reads, changes, and lists them.
 */

const grpc = require('@grpc/grpc-js');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { clientStream, serverStream } = require('../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../platform/rpc');

/**
 * Serve AdmissionService, recording every operation of the admissions team.
 */
class AdmissionService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'AdmissionService' });
  }

  /**
   * Takes a stream of admissions and answers once at the end.
   */
  batchCreateAdmissions(call, callback) {
    clientStream((request) => this.executeBatchCreateAdmissions(request), () => ({}))(call, callback);
  }

  async executeBatchCreateAdmissions(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'BatchCreateAdmissions', request);
    this.logger.info('batch_create_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      replay_clip: {
        owner: {
          email: ((request?.chunk === 'header' ? request.header : undefined)?.submitted_by?.email ?? ''),
        },
      },
    };
    const spread1 = {
      ...request1,
      context: request1.context ? { ...request1.context, baggage: { ...request1.context.baggage } } : null,
    };
    const reply1 = await call((done) => this.clients.broadcastsReplayClipCalculationClient.compareReplayClipOptions(spread1, metadataFor(spread1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: ((request?.chunk === 'record' ? request.record : undefined)?.subject?.kind ?? '') });
    return assemble(
      { result: { reference_ids: (reply1?.result?.reference_ids ?? []) } },
      {
        result: {
          correlation_id: ((request?.chunk === 'record' ? request.record : undefined)?.correlation_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns several admissions at once.
   */
  batchGetAdmissions(call, callback) {
    this.executeBatchGetAdmissions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBatchGetAdmissions(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'BatchGetAdmissions', request);
    this.logger.info('batch_get_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      details: { contact: { email: (input1?.context?.actor?.email ?? '') } },
      wristband: { owner: { user_id: (input1?.context?.actor?.user_id ?? '') } },
    };
    const reply1 = await call((done) => this.clients.admissionsWristbandValidationClient.validateWristband(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    return {
      result: {
        correlation_id: (reply1?.result?.correlation_id ?? ''),
        status: (reply1?.outcome?.status ?? ''),
      },
    };
  }

  /**
   * Creates an admission.
   */
  cloneAdmission(call, callback) {
    this.executeCloneAdmission(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCloneAdmission(request) {
    validateContext(request.context);
    if (request.admission) {
      await this.store.save(this.store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await this.store.record(request.context.tenant_id, 'CloneAdmission', request);
    this.logger.info('clone_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.admission?.occurred_at,
        labels: { ...(request?.admission?.attributes ?? {}) },
        owner_id: (request?.admission?.admission_id ?? ''),
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => this.clients.sponsorshipsSponsorshipDealQueryClient.countSponsorshipDeals(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      {
        result: {
          reference_ids: (reply1?.result?.reference_ids ?? []),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        admission: {
          occurred_at: request?.admission?.occurred_at,
          source: (request?.admission?.source ?? ''),
        },
      },
    );
  }

  /**
   * Creates an admission.
   */
  createAdmission(call, callback) {
    this.executeCreateAdmission(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeCreateAdmission(request) {
    validateContext(request.context);
    if (request.admission) {
      await this.store.save(this.store.toAdmissionRecord(request.context.tenant_id, request.admission));
    }
    await this.store.record(request.context.tenant_id, 'CreateAdmission', request);
    this.logger.info('create_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.admission?.occurred_at,
        labels: { ...(request?.admission?.attributes ?? {}) },
        owner_id: (request?.admission?.subject?.gate_scan_id ?? ''),
      },
    };
    const copied1 = structuredClone(request1);
    const reply1 = await call((done) => this.clients.admissionsAdmissionQueryClient.countAdmissions(copied1, metadataFor(copied1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: {
        recipient: { email: (request?.context?.actor?.email ?? '') },
        variables: { ...(request?.admission?.attributes ?? {}) },
      },
    };
    const merged2 = Object.assign({}, request2, { context: request2.context });
    const reply2 = await call((done) => this.clients.venuesVenuePolicyExportClient.shareVenuePolicyExport(merged2, metadataFor(merged2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.request_id ?? '') });
    return assemble(
      { result: { details: (reply1?.result?.details ?? {}) } },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.outcome?.status ?? ''),
        },
      },
      {
        admission: {
          attributes: (request?.admission?.attributes ?? {}),
          subject: { kind: (request?.admission?.kind ?? '') },
        },
      },
    );
  }

  /**
   * Deletes an admission.
   */
  deleteAdmission(call, callback) {
    this.executeDeleteAdmission(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeDeleteAdmission(request) {
    validateContext(request.context);
    if (!request.admission_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
    }
    await this.store.record(request.context.tenant_id, 'DeleteAdmission', request);
    this.logger.info('delete_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { contact: { email: (request?.context?.actor?.email ?? '') } },
      wristband: { audit: { etag: (request?.etag ?? '') } },
    };
    const reply1 = await withRetry(() => call((done) => this.clients.admissionsWristbandValidationClient.lintWristbands(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      admission: { measurements: [{ unit: (reply1?.wristband?.dimensions?.unit ?? '') }] },
      context: request?.context,
      details: { contact: { email: (reply1?.wristband?.owner?.email ?? '') } },
      reason: (request?.reason ?? ''),
    };
    const task2 = deferred(() => call((done) => this.clients.admissionsAdmissionValidationClient.lintAdmissions(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    const request3 = {
      context: request?.context,
      sponsor: {
        primary_address: {
          country_code: ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.country_code ?? ''),
          lines: [...((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.lines ?? [])],
          postal_code: ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.postal_code ?? ''),
        },
      },
    };
    const reply3 = await this.bus.request('acme.admissions.v1.AdmissionService/DeleteAdmission:2', request3);
    this.logger.info('local_audit', { value: (request?.admission_id ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          details: (reply1?.result?.details ?? {}),
        },
      },
      { result: { status: (reply2?.result?.status ?? '') } },
      { result: { correlation_id: (reply3?.result?.correlation_id ?? '') } },
    );
  }

  /**
   * Returns an admission.
   */
  getAdmission(call, callback) {
    this.executeGetAdmission(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeGetAdmission(request) {
    validateContext(request.context);
    if (!request.admission_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'admission_id is required');
    }
    await this.store.record(request.context.tenant_id, 'GetAdmission', request);
    this.logger.info('get_admission', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = { context: request?.context };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.documentsDocumentQueryClient.resolveDocuments(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    return assemble(
      {
        admission: { correlation_id: (reply1?.result?.correlation_id ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
      },
      { admission: { admission_id: (request?.admission_id ?? '') } },
    );
  }

  /**
   * Lists admissions a page at a time.
   */
  listAdmissions(call, callback) {
    this.executeListAdmissions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeListAdmissions(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'ListAdmissions', request);
    this.logger.info('list_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {};
  }

  /**
   * Lists admissions a page at a time.
   */
  searchAdmissions(call, callback) {
    this.executeSearchAdmissions(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSearchAdmissions(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'SearchAdmissions', request);
    this.logger.info('search_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      wristband: { address: { region: (request?.filter?.region ?? '') } },
    };
    const reply1 = await call((done) => this.clients.admissionsWristbandCalculationClient.quoteWristband(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      result: {
        reference_ids: (reply1?.result?.reference_ids ?? []),
        status: (reply1?.result?.status ?? ''),
      },
    };
  }

  /**
   * Streams the changes of admissions as they happen.
   */
  watchAdmissions(call) {
    serverStream((request) => this.executeWatchAdmissions(request))(call);
  }

  async executeWatchAdmissions(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'WatchAdmissions', request);
    this.logger.info('watch_admissions', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: {
        address: { region: (request?.filter?.region ?? '') },
        owner: { email: (request?.subscriber?.email ?? '') },
      },
      context: request?.context,
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.concessionsConcessionStandClient.createConcessionStand(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    let selected2_4;
    switch (reply1?.concession_stand?.locator) {
      case 'address':
        selected2_4 = ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.country_code ?? '');
        break;
      default:
        selected2_4 = '';
    }
    const request2 = {
      context: request?.context,
      menu_item: {
        address: { country_code: selected2_4 },
        audit: { etag: (reply1?.concession_stand?.audit?.etag ?? '') },
        dimensions: { weight: (reply1?.concession_stand?.dimensions?.weight ?? 0) },
        owner: { user_id: (reply1?.concession_stand?.owner?.user_id ?? '') },
      },
    };
    const reply2 = await call((done) => this.clients.concessionsMenuItemCalculationClient.explainMenuItemCalculation(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      wristband: {
        audit: { etag: (reply2?.menu_item?.audit?.etag ?? '') },
        dimensions: { width: (reply2?.menu_item?.dimensions?.width ?? 0) },
      },
    };
    const reply3 = await call((done) => this.clients.admissionsWristbandValidationClient.explainWristbandDecision(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      details: {
        destination: {
          locality: ((reply2?.menu_item?.locator === 'address' ? reply2?.menu_item.address : undefined)?.locality ?? ''),
        },
      },
      wristband: {
        address: {
          recipient: ((reply2?.menu_item?.locator === 'address' ? reply2?.menu_item.address : undefined)?.recipient ?? ''),
          region: ((reply2?.menu_item?.locator === 'address' ? reply2?.menu_item.address : undefined)?.region ?? ''),
        },
        components: { primary: { quantity: (reply2?.menu_item?.components?.primary?.quantity ?? '0') } },
      },
    };
    const method4 = 'compareWristbandOptions';
    const reply4 = await call((done) => this.clients.admissionsWristbandCalculationClient[method4](request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.subscriber?.preferred_channel ?? '') });
    return assemble(
      {
        admission: {
          subject: { display_name: (reply1?.concession_stand?.owner?.display_name ?? '') },
        },
      },
      {
        admission: {
          subject: { display_name: (reply2?.menu_item?.owner?.display_name ?? '') },
        },
        result: { status: (reply2?.result?.status ?? '') },
      },
      {
        result: {
          message: (reply3?.result?.message ?? ''),
          status: (reply3?.outcome?.status ?? ''),
        },
      },
      { result: { correlation_id: (reply4?.result?.correlation_id ?? '') } },
    );
  }
}

module.exports = { AdmissionService };
