'use strict';

/**
 * Answers read-only questions about performer contracts, for screens and reports.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../platform/context');
const { deadline, metadataFor } = require('../../platform/credentials');
const { RpcError, toServiceError } = require('../../platform/errors');
const { serverStream } = require('../../platform/handlers');
const { assemble, call, deferred } = require('../../platform/rpc');

/**
 * Serve PerformerContractQueryService, recording every operation of the performers team.
 */
class PerformerContractQueryService {
  constructor({ store, clients, bus, logger }) {
    this.store = store;
    this.clients = clients;
    this.bus = bus;
    this.logger = logger.child({ service: 'PerformerContractQueryService' });
  }

  /**
   * Lists performer contracts a page at a time.
   */
  browsePerformerContracts(call, callback) {
    this.executeBrowsePerformerContracts(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeBrowsePerformerContracts(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'BrowsePerformerContracts', request);
    this.logger.info('browse_performer_contracts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.filter?.labels ?? {}) },
        owner_id: (request?.filter?.owner_id ?? ''),
        query: (request?.filter?.query ?? ''),
      },
      page: { page_token: (request?.page?.page_token ?? '') },
    };
    const task1 = deferred(() => call((done) => this.clients.performersPerformerQueryClient.suggestPerformers(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const reply1 = await task1();
    const request2 = {
      context: request?.context,
      filter: {
        labels: { ...(request?.filter?.labels ?? {}) },
        owner_id: (request?.filter?.owner_id ?? ''),
        region: (request?.filter?.region ?? ''),
      },
      page: { page_token: (request?.page?.page_token ?? '') },
    };
    const reply2 = await this.bus.request('acme.performers.v1.PerformerContractQueryService/BrowsePerformerContracts:1', request2);
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return assemble(
      { page_info: { total_size: (reply1?.page_info?.total_size ?? '0') } },
      { result: { details: (reply2?.result?.details ?? {}) } },
    );
  }

  /**
   * Streams every matching performer contract.
   */
  exportPerformerContracts(call) {
    serverStream((request) => this.executeExportPerformerContracts(request))(call);
  }

  async executeExportPerformerContracts(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ExportPerformerContracts', request);
    this.logger.info('export_performer_contracts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { created_after: request?.filter?.created_after },
    };
    const reply1 = await runInContext(request.context, () => call((done) => this.clients.performersPerformerQueryClient.suggestPerformers(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      context: request?.context,
      details: { format: (request?.format ?? '') },
    };
    const reply2 = await call((done) => this.clients.performersPerformerQueryClient.summarizePerformers(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.destination_uri ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        result: {
          reference_ids: (reply2?.result?.reference_ids ?? []),
          status: (reply2?.result?.status ?? ''),
        },
      },
      {
        performer_contract: { billing_address: { region: (request?.filter?.region ?? '') } },
      },
    );
  }

  /**
   * Lists performer contracts a page at a time.
   */
  findPerformerContracts(call, callback) {
    this.executeFindPerformerContracts(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeFindPerformerContracts(request) {
    validateContext(request.context);
    const pageSize = request.page?.page_size ?? 0;
    if (pageSize < 0 || pageSize > 500) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'page_size must be between 0 and 500');
    }
    await this.store.record(request.context.tenant_id, 'FindPerformerContracts', request);
    this.logger.info('find_performer_contracts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      sponsor: { primary_address: { region: (request?.filter?.region ?? '') } },
    };
    const reply1 = await new Promise((resolve, reject) => {
      this.clients.sponsorshipsSponsorValidationClient.validateSponsor(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    this.logger.info('local_audit', { value: (request?.order_by ?? '') });
    return {
      performer_contracts: [{
        billing_address: { recipient: (reply1?.sponsor?.primary_address?.recipient ?? '') },
        number: ((reply1?.sponsor?.identity_document === 'national_id' ? reply1?.sponsor.national_id : undefined)?.number ?? ''),
      }],
    };
  }

  /**
   * Returns a performer contract.
   */
  lookupPerformerContract(call, callback) {
    this.executeLookupPerformerContract(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeLookupPerformerContract(request) {
    validateContext(request.context);
    if (!request.performer_contract_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'performer_contract_id is required');
    }
    await this.store.record(request.context.tenant_id, 'LookupPerformerContract', request);
    this.logger.info('lookup_performer_contract', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return {
      performer_contract: { performer_contract_id: (request?.performer_contract_id ?? '') },
    };
  }

  /**
   * Returns several performer contracts at once.
   */
  resolvePerformerContracts(call, callback) {
    this.executeResolvePerformerContracts(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeResolvePerformerContracts(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'ResolvePerformerContracts', request);
    this.logger.info('resolve_performer_contracts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      details: { locale: (request?.context?.locale ?? '') },
      rider: {
        owner: {
          email: (request?.context?.actor?.email ?? ''),
          user_id: (request?.context?.actor?.user_id ?? ''),
        },
        scopes: [...(request?.performer_contract_ids ?? [])],
      },
    };
    const previewRider1 = promisify(this.clients.performersRiderValidationClient.previewRider.bind(this.clients.performersRiderValidationClient));
    const reply1 = await previewRider1(request1, metadataFor(request1.context), { deadline: deadline() });
    const request2 = {
      context: request?.context,
      filter: { created_after: reply1?.rider?.audit?.updated_at },
    };
    const reply2 = await call((done) => this.clients.performersPerformerQueryClient.suggestPerformers(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    return assemble(
      {
        performer_contracts: [{
          lines: [{
            amount: (reply1?.rider?.action === 'discount' ? reply1?.rider.discount : undefined)?.amount,
          }],
        }],
        result: { status: (reply1?.outcome?.status ?? '') },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          reference_ids: (reply2?.result?.reference_ids ?? []),
        },
      },
    );
  }

  /**
   * Summarizes a performer contract.
   */
  summarizePerformerContracts(call, callback) {
    this.executeSummarizePerformerContracts(call.request).then(
      (reply) => callback(null, reply),
      (error) => callback(toServiceError(error)),
    );
  }

  async executeSummarizePerformerContracts(request) {
    validateContext(request.context);
    await this.store.record(request.context.tenant_id, 'SummarizePerformerContracts', request);
    this.logger.info('summarize_performer_contracts', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.context?.deadline,
        labels: { ...(request?.details?.filters ?? {}) },
        owner_id: (request?.performer_contract_id ?? ''),
      },
    };
    const method1 = 'listVenuePolicyVersions';
    const reply1 = await call((done) => this.clients.venuesVenuePolicyHistoryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      venue_policy: {
        block: {
          notify_email: (request?.context?.actor?.email ?? ''),
          reason: (request?.reason ?? ''),
        },
        owner: { user_id: (request?.context?.actor?.user_id ?? '') },
      },
    };
    const client2 = this.clients.registry.get('acme.venues.v1.VenuePolicyHistoryService');
    const reply2 = await call((done) => client2.recordVenuePolicyVersion(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      venue: {
        audit: {
          created_at: reply2?.venue_policy?.audit?.created_at,
          updated_by: (reply2?.venue_policy?.audit?.updated_by ?? ''),
        },
        components: { primary: { description: (reply2?.venue_policy?.description ?? '') } },
      },
    };
    const send3 = this.clients.venuesVenueClient.updateVenue.bind(this.clients.venuesVenueClient);
    const reply3 = await call((done) => send3(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    let selected4_1;
    switch (reply3?.venue?.locator) {
      case 'address':
        selected4_1 = ((reply3?.venue?.locator === 'address' ? reply3?.venue.address : undefined)?.region ?? '');
        break;
      default:
        selected4_1 = '';
    }
    const request4 = {
      context: request?.context,
      filter: { region: selected4_1 },
    };
    const reply4 = await call((done) => this.clients.performersPerformerQueryClient.findPerformers(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    this.logger.info('local_audit', { value: (request?.details?.cron ?? '') });
    return assemble(
      { result: { message: (reply1?.result?.message ?? '') } },
      { result: { status: (reply2?.result?.status ?? '') } },
      { result: { message: (reply3?.result?.message ?? '') } },
      {
        result: {
          message: (reply4?.result?.message ?? ''),
          status: (reply4?.result?.status ?? ''),
        },
      },
      { period: request?.details?.period },
    );
  }
}

module.exports = { PerformerContractQueryService };
