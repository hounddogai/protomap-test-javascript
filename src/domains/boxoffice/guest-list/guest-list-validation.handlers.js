'use strict';

/**
 * Checks guest lists against the rules before anything acts on them.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call } = require('../../../platform/rpc');
const { collect } = require('../../../platform/streams');

/**
 * The handlers of GuestListValidationService, over the dependencies the boxoffice team is given.
 */
function createGuestListValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks a guest list.
   */
  async function checkGuestListEligibility(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'CheckGuestListEligibility', request);
    logger.info('check_guest_list_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.guest_list?.lines ?? []).map(async (item) => {
      const request1 = {
        cash_drawer: {
          audit: { updated_at: request?.guest_list?.audit?.updated_at },
          bank: {
            account_holder: ((request?.guest_list?.settlement === 'bank' ? request?.guest_list.bank : undefined)?.account_holder ?? ''),
            bic: ((request?.guest_list?.settlement === 'bank' ? request?.guest_list.bank : undefined)?.bic ?? ''),
          },
          limits: [{ amount: item?.tax?.amount }],
        },
        context: request?.context,
      };
      const reply1 = await call((done) => clients.boxofficeCashDrawerValidationClient.normalizeCashDrawer(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    let selected2_1;
    switch (reply1?.cash_drawer?.funding) {
      case 'card':
        selected2_1 = ((reply1?.cash_drawer?.funding === 'card' ? reply1?.cash_drawer.card : undefined)?.holder_name ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
      tour_budget: {
        audit: { updated_at: reply1?.cash_drawer?.audit?.updated_at },
        card: { holder_name: selected2_1 },
        contact: { phone: (request?.details?.contact?.phone ?? '') },
      },
    };
    const reply2 = await call((done) => clients.toursTourBudgetValidationClient.previewTourBudget(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { outcome: { status: (reply2?.outcome?.status ?? '') } },
      {
        guest_list: {
          guest_list_id: (request?.guest_list_id ?? ''),
          notes: (request?.guest_list?.notes ?? ''),
        },
      },
    );
  }

  /**
   * Explains a guest list.
   */
  async function explainGuestListDecision(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'ExplainGuestListDecision', request);
    logger.info('explain_guest_list_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.guest_list?.audit?.created_at,
        labels: { ...(request?.annotations ?? {}) },
        owner_id: (request?.guest_list_id ?? ''),
        region: (request?.guest_list?.billing_address?.region ?? ''),
      },
    };
    const messages1 = await collect(clients.venuesVenueSectionQueryClient.exportVenueSections(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const request2 = { context: request?.context };
    const reply2 = await call((done) => clients.performersPerformerContractQueryClient.resolvePerformerContracts(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: {
        destination: { recipient: (request?.guest_list?.billing_address?.recipient ?? '') },
      },
    };
    const reply3 = await new Promise((resolve, reject) => {
      clients.boxofficeGuestListCalculationClient.simulateGuestList(request3, metadataFor(request3.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply2?.result?.message ?? '') },
        result: { message: (reply2?.result?.message ?? '') },
      },
      {
        guest_list: { audit: { etag: (reply3?.guest_list?.audit?.etag ?? '') } },
        outcome: { status: (reply3?.result?.status ?? '') },
      },
      {
        guest_list: {
          counterparty: request?.guest_list?.counterparty,
          guest_list_id: (request?.guest_list_id ?? ''),
        },
      },
    );
  }

  /**
   * Returns a guest list.
   */
  async function getGuestListRules(request) {
    validateContext(request.context);
    if (!request.guest_list_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
    }
    await store.record(request.context.tenant_id, 'GetGuestListRules', request);
    logger.info('get_guest_list_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      audit_event: { correlation_id: (request?.guest_list_id ?? '') },
      context: request?.context,
      details: {
        contact: { email: (request?.context?.actor?.email ?? '') },
        locale: (request?.context?.locale ?? ''),
      },
    };
    const explainAuditEventDecision1 = promisify(clients.auditAuditEventValidationClient.explainAuditEventDecision.bind(clients.auditAuditEventValidationClient));
    const reply1 = await explainAuditEventDecision1(request1, metadataFor(request1.context), { deadline: deadline() });
    return assemble(
      { result: { correlation_id: (reply1?.result?.correlation_id ?? '') } },
      { guest_list: { guest_list_id: (request?.guest_list_id ?? '') } },
    );
  }

  /**
   * Lints a guest list.
   */
  async function lintGuestLists(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'LintGuestLists', request);
    logger.info('lint_guest_lists', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      box_office_sale: {
        billing_address: { region: (request?.guest_list?.billing_address?.region ?? '') },
        card: {
          last4: ((request?.guest_list?.settlement === 'card' ? request?.guest_list.card : undefined)?.last4 ?? ''),
        },
        counterparty: {
          cash_drawer_id: (request?.guest_list?.counterparty?.cash_drawer_id ?? ''),
        },
      },
      context: request?.context,
    };
    const reply1 = await call((done) => clients.boxofficeBoxOfficeSaleValidationClient.scoreBoxOfficeSale(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      cash_drawer: {
        audit: {
          etag: (reply1?.box_office_sale?.audit?.etag ?? ''),
          updated_at: request?.guest_list?.audit?.updated_at,
        },
        card: {
          network: ((reply1?.box_office_sale?.settlement === 'card' ? reply1?.box_office_sale.card : undefined)?.network ?? ''),
        },
      },
      context: request?.context,
      details: {
        contact: {
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
    };
    const method2 = 'checkCashDrawerEligibility';
    const reply2 = await call((done) => clients.boxofficeCashDrawerValidationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        guest_list: { counterparty: reply1?.box_office_sale?.counterparty },
        result: { reference_ids: (reply1?.result?.reference_ids ?? []) },
      },
      {
        guest_list: { number: (reply2?.cash_drawer?.holder?.number ?? '') },
        outcome: { effective_at: reply2?.outcome?.effective_at },
      },
      {
        guest_list: {
          guest_list_id: (request?.guest_list_id ?? ''),
          metadata: (request?.guest_list?.metadata ?? {}),
        },
      },
    );
  }

  /**
   * Normalizes a guest list.
   */
  async function normalizeGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'NormalizeGuestList', request);
    logger.info('normalize_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.guest_list?.lines ?? [])) {
      const request1 = {
        cash_drawer: {
          audit: {
            etag: (request?.guest_list?.audit?.etag ?? ''),
            updated_by: (request?.guest_list?.audit?.updated_by ?? ''),
          },
          bank: {
            iban: ((request?.guest_list?.settlement === 'bank' ? request?.guest_list.bank : undefined)?.iban ?? ''),
          },
          limits: [{ amount: item?.amount }],
        },
        context: request?.context,
      };
      const reply1 = await call((done) => clients.boxofficeCashDrawerValidationClient.lintCashDrawers(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      guest_list: {
        due_at: request?.guest_list?.due_at,
        guest_list_id: (request?.guest_list_id ?? ''),
      },
    };
  }

  /**
   * Previews a guest list.
   */
  async function previewGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'PreviewGuestList', request);
    logger.info('preview_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.guest_list?.due_at,
        region: (request?.guest_list?.billing_address?.region ?? ''),
      },
    };
    const client1 = clients.registry.get('acme.featureflags.v1.FeatureFlagQueryService');
    const reply1 = await call((done) => client1.countFeatureFlags(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      details: { contact: { name: request?.details?.contact?.name } },
    };
    const send2 = clients.featureflagsFeatureFlagValidationClient.explainFeatureFlagDecision.bind(clients.featureflagsFeatureFlagValidationClient);
    const reply2 = await call((done) => send2(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      subscriber: {
        preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
      },
    };
    const messages3 = await collect(clients.documentsDocumentClient.watchDocuments(request3, metadataFor(request3.context), { deadline: deadline() }));
    const reply3 = messages3.at(-1) ?? {};
    const request4 = {
      audit_event: {
        subject: { display_name: (request?.guest_list?.counterparty?.display_name ?? '') },
      },
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.guest_list?.billing_address?.country_code ?? ''),
          postal_code: (request?.guest_list?.billing_address?.postal_code ?? ''),
        },
      },
    };
    const spread4 = {
      ...request4,
      context: request4.context ? { ...request4.context, baggage: { ...request4.context.baggage } } : null,
    };
    const reply4 = await call((done) => clients.auditAuditEventValidationClient.scoreAuditEvent(spread4, metadataFor(spread4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: {
          message: (reply1?.result?.message ?? ''),
          status: (reply1?.result?.status ?? ''),
        },
      },
      {
        guest_list: {
          audit: { updated_by: (reply2?.feature_flag?.audit?.updated_by ?? '') },
        },
        outcome: { next_steps: (reply2?.outcome?.next_steps ?? []) },
      },
      {
        guest_list: {
          counterparty: { currency_code: (reply4?.breakdown?.currency_code ?? '') },
        },
      },
      {
        guest_list: {
          billing_address: request?.guest_list?.billing_address,
          guest_list_id: (request?.guest_list_id ?? ''),
        },
      },
    );
  }

  /**
   * Scores a guest list.
   */
  async function scoreGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'ScoreGuestList', request);
    logger.info('score_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.guest_list?.attachments ?? []).map(async (item) => {
      const request1 = {
        concession_order: {
          attachments: [{
            checksum: (item?.checksum ?? ''),
            content_type: (item?.content_type ?? ''),
            size_bytes: (item?.size_bytes ?? '0'),
            uri: (item?.uri ?? ''),
          }],
        },
        context: request?.context,
      };
      const reply1 = await call((done) => clients.concessionsConcessionOrderValidationClient.explainConcessionOrderDecision(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      context: request?.context,
      filter: { region: (request?.guest_list?.billing_address?.region ?? '') },
    };
    const messages2 = await collect(clients.performersPerformerContractQueryClient.exportPerformerContracts(request2, metadataFor(request2.context), { deadline: deadline() }));
    const reply2 = messages2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      guest_list: {
        counterparty: request?.guest_list?.counterparty,
        guest_list_id: (request?.guest_list_id ?? ''),
      },
    };
  }

  /**
   * Validates a guest list.
   */
  async function validateGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'ValidateGuestList', request);
    logger.info('validate_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      box_office_sale: { audit: { updated_by: (input1?.guest_list?.audit?.updated_by ?? '') } },
      context: input1?.context,
      details: { contact: { phone: (input1?.details?.contact?.phone ?? '') } },
    };
    const reply1 = await call((done) => clients.boxofficeBoxOfficeSaleValidationClient.lintBoxOfficeSales(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      cash_drawer: {
        audit: {
          created_by: (reply1?.box_office_sale?.audit?.created_by ?? ''),
          etag: (request?.guest_list?.audit?.etag ?? ''),
        },
      },
      context: request?.context,
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.boxofficeCashDrawerValidationClient.validateCashDrawer(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      filter: {
        created_after: reply1?.box_office_sale?.audit?.updated_at,
        labels: { ...(reply2?.cash_drawer?.labels ?? {}) },
        owner_id: (reply1?.box_office_sale?.counterparty?.cash_drawer_id ?? ''),
        region: (request?.guest_list?.billing_address?.region ?? ''),
      },
    };
    const copied3 = structuredClone(request3);
    const reply3 = await call((done) => clients.toursTourQueryClient.browseTours(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
    const request4 = {
      context: request?.context,
      document: {
        audit: { etag: (request?.guest_list?.audit?.etag ?? '') },
        billing_address: {
          locality: (request?.guest_list?.billing_address?.locality ?? ''),
          postal_code: (reply1?.box_office_sale?.billing_address?.postal_code ?? ''),
        },
      },
    };
    const merged4 = Object.assign({}, request4, { context: request4.context });
    const reply4 = await call((done) => clients.documentsDocumentValidationClient.validateDocument(merged4, metadataFor(merged4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        guest_list: { audit: { created_at: reply1?.box_office_sale?.audit?.created_at } },
        result: { correlation_id: (reply1?.result?.correlation_id ?? '') },
      },
      { result: { details: (reply2?.result?.details ?? {}) } },
      { outcome: { status: (reply3?.result?.status ?? '') } },
      {
        guest_list: { audit: { updated_by: (reply4?.document?.audit?.updated_by ?? '') } },
      },
      {
        guest_list: {
          billing_address: request?.guest_list?.billing_address,
          guest_list_id: (request?.guest_list_id ?? ''),
        },
      },
    );
  }

  return {
    checkGuestListEligibility: unary(checkGuestListEligibility),
    explainGuestListDecision: unary(explainGuestListDecision),
    getGuestListRules: unary(getGuestListRules),
    lintGuestLists: unary(lintGuestLists),
    normalizeGuestList: unary(normalizeGuestList),
    previewGuestList: unary(previewGuestList),
    scoreGuestList: unary(scoreGuestList),
    validateGuestList: unary(validateGuestList),
  };
}

module.exports = { createGuestListValidationHandlers };
