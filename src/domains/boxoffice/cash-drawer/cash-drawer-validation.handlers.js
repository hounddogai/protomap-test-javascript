'use strict';

/**
 * Checks cash drawers against the rules before anything acts on them.
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
 * The handlers of CashDrawerValidationService, over the dependencies the boxoffice team is given.
 */
function createCashDrawerValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks a cash drawer.
   */
  async function checkCashDrawerEligibility(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await store.record(request.context.tenant_id, 'CheckCashDrawerEligibility', request);
    logger.info('check_cash_drawer_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { labels: { ...(request?.cash_drawer?.labels ?? {}) } },
    };
    const messages1 = await collect(clients.ledgerLedgerAccountQueryClient.exportLedgerAccounts(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    let selected2_1;
    switch (request?.cash_drawer?.funding) {
      case 'bank':
        selected2_1 = ((request?.cash_drawer?.funding === 'bank' ? request?.cash_drawer.bank : undefined)?.iban ?? '');
        break;
      default:
        selected2_1 = '';
    }
    const request2 = {
      box_office_sale: {
        audit: { updated_by: (request?.cash_drawer?.audit?.updated_by ?? '') },
        bank: { iban: selected2_1 },
        box_office_sale_id: (request?.cash_drawer?.holder?.box_office_sale_id ?? ''),
      },
      context: request?.context,
    };
    const reply2 = await call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.compareBoxOfficeSaleOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      details: {
        destination: {
          country_code: (reply2?.box_office_sale?.billing_address?.country_code ?? ''),
          locality: (reply2?.box_office_sale?.billing_address?.locality ?? ''),
        },
      },
      stream_key: {
        audit: { created_by: (request?.cash_drawer?.audit?.created_by ?? '') },
        bank: {
          account_holder: ((request?.cash_drawer?.funding === 'bank' ? request?.cash_drawer.bank : undefined)?.account_holder ?? ''),
        },
      },
    };
    const recalculateStreamKeys3 = promisify(clients.broadcastsStreamKeyCalculationClient.recalculateStreamKeys.bind(clients.broadcastsStreamKeyCalculationClient));
    const reply3 = await recalculateStreamKeys3(request3, metadataFor(request3.context), { deadline: deadline() });
    const request4 = {
      context: request?.context,
      sponsor: { audit: { updated_at: reply2?.box_office_sale?.audit?.updated_at } },
    };
    const reply4 = await call((done) => clients.sponsorshipsSponsorCalculationClient.quoteSponsor(request4, metadataFor(request4.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { status: (reply2?.result?.status ?? '') },
        result: { message: (reply2?.result?.message ?? '') },
      },
      {
        cash_drawer: { audit: { updated_at: reply3?.stream_key?.audit?.updated_at } },
        outcome: { message: (reply3?.result?.message ?? '') },
      },
      {
        cash_drawer: { audit: { updated_by: (reply4?.sponsor?.audit?.updated_by ?? '') } },
        result: { correlation_id: (reply4?.result?.correlation_id ?? '') },
      },
      {
        cash_drawer: {
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
          contact: request?.details?.contact,
        },
      },
    );
  }

  /**
   * Returns a cash drawer.
   */
  async function getCashDrawerRules(request) {
    validateContext(request.context);
    if (!request.cash_drawer_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'cash_drawer_id is required');
    }
    await store.record(request.context.tenant_id, 'GetCashDrawerRules', request);
    logger.info('get_cash_drawer_rules', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { cash_drawer: { cash_drawer_id: (request?.cash_drawer_id ?? '') } };
  }

  /**
   * Lints a cash drawer.
   */
  async function lintCashDrawers(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await store.record(request.context.tenant_id, 'LintCashDrawers', request);
    logger.info('lint_cash_drawers', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      annotations: { ...(request?.annotations ?? {}) },
      context: request?.context,
      details: { filters: { ...(request?.cash_drawer?.labels ?? {}) } },
      reason: (request?.reason ?? ''),
    };
    const method1 = 'summarizeStreamKeys';
    const reply1 = await call((done) => clients.broadcastsStreamKeyQueryClient[method1](request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.cash_drawer?.audit?.updated_at,
        labels: { ...(request?.cash_drawer?.labels ?? {}) },
        owner_id: (request?.cash_drawer?.holder?.box_office_sale_id ?? ''),
      },
    };
    const client2 = clients.registry.get('acme.concessions.v1.ConcessionOrderQueryService');
    const reply2 = await call((done) => client2.countConcessionOrders(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.contact?.preferred_channel ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.result?.correlation_id ?? ''),
          details: (reply1?.result?.details ?? {}),
        },
      },
      {
        result: {
          details: (reply2?.result?.details ?? {}),
          message: (reply2?.result?.message ?? ''),
        },
      },
      {
        cash_drawer: {
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
          opened_at: request?.cash_drawer?.opened_at,
        },
      },
    );
  }

  /**
   * Normalizes a cash drawer.
   */
  async function normalizeCashDrawer(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await store.record(request.context.tenant_id, 'NormalizeCashDrawer', request);
    logger.info('normalize_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: { labels: { ...(request?.cash_drawer?.labels ?? {}) } },
    };
    const send1 = clients.sponsorshipsSponsorshipDealQueryClient.suggestSponsorshipDeals.bind(clients.sponsorshipsSponsorshipDealQueryClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const results2 = await Promise.all((request?.cash_drawer?.limits ?? []).map(async (item) => {
      const request2 = {
        context: request?.context,
        venue_policy: { name: (item?.name ?? '') },
      };
      const reply2 = await call((done) => clients.venuesVenuePolicyValidationClient.validateVenuePolicy(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { message: (reply1?.result?.message ?? '') },
      },
      {
        cash_drawer: {
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
          opened_at: request?.cash_drawer?.opened_at,
        },
      },
    );
  }

  /**
   * Previews a cash drawer.
   */
  async function previewCashDrawer(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await store.record(request.context.tenant_id, 'PreviewCashDrawer', request);
    logger.info('preview_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_1;
    switch (request?.cash_drawer?.funding) {
      case 'card':
        selected1_1 = ((request?.cash_drawer?.funding === 'card' ? request?.cash_drawer.card : undefined)?.last4 ?? '');
        break;
      default:
        selected1_1 = '';
    }
    const request1 = {
      box_office_sale: {
        audit: { created_by: (request?.cash_drawer?.audit?.created_by ?? '') },
        card: { last4: selected1_1 },
      },
      context: request?.context,
    };
    const reply1 = await call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.quoteBoxOfficeSale(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const settled2 = await Promise.allSettled((request?.cash_drawer?.limits ?? []).map(async (item) => {
      const request2 = {
        box_office_sale: {
          audit: {
            created_by: (request?.cash_drawer?.audit?.created_by ?? ''),
            updated_at: reply1?.box_office_sale?.audit?.updated_at,
          },
          lines: [{ tax: { amount: item?.amount } }],
        },
        context: request?.context,
        details: {
          destination: {
            postal_code: (reply1?.box_office_sale?.billing_address?.postal_code ?? ''),
          },
        },
      };
      const reply2 = await call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.recalculateBoxOfficeSales(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        cash_drawer: { audit: { etag: (reply1?.box_office_sale?.audit?.etag ?? '') } },
        result: { status: (reply1?.result?.status ?? '') },
      },
      {
        cash_drawer: {
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
          labels: (request?.cash_drawer?.labels ?? {}),
        },
      },
    );
  }

  /**
   * Scores a cash drawer.
   */
  async function scoreCashDrawer(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await store.record(request.context.tenant_id, 'ScoreCashDrawer', request);
    logger.info('score_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = [];
    for (const item of (request?.cash_drawer?.limits ?? [])) {
      const request1 = {
        box_office_sale: { lines: [{ tax: { amount: item?.amount } }] },
        context: request?.context,
      };
      const reply1 = await call((done) => clients.boxofficeBoxOfficeSaleValidationClient.validateBoxOfficeSale(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      results1.push(reply1);
    }
    const reply1 = results1.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      cash_drawer: {
        cash_drawer_id: (request?.cash_drawer_id ?? ''),
        contact: request?.cash_drawer?.contact,
      },
    };
  }

  /**
   * Validates a cash drawer.
   */
  async function validateCashDrawer(request) {
    validateContext(request.context);
    if (request.cash_drawer) {
      await store.save(store.toCashDrawerRecord(request.context.tenant_id, request.cash_drawer));
    }
    await store.record(request.context.tenant_id, 'ValidateCashDrawer', request);
    logger.info('validate_cash_drawer', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.cash_drawer?.audit?.updated_at,
        labels: { ...(request?.cash_drawer?.labels ?? {}) },
      },
    };
    const messages1 = await collect(clients.toursTourStopQueryClient.exportTourStops(request1, metadataFor(request1.context), { deadline: deadline() }));
    const reply1 = messages1.at(-1) ?? {};
    const results2 = await Promise.all((request?.cash_drawer?.limits ?? []).map(async (item) => {
      const request2 = {
        box_office_sale: {
          audit: { created_by: (request?.cash_drawer?.audit?.created_by ?? '') },
          card: {
            holder_name: ((request?.cash_drawer?.funding === 'card' ? request?.cash_drawer.card : undefined)?.holder_name ?? ''),
            network: ((request?.cash_drawer?.funding === 'card' ? request?.cash_drawer.card : undefined)?.network ?? ''),
          },
          lines: [{ tax: { amount: item?.amount } }],
        },
        context: request?.context,
      };
      const reply2 = await call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.compareBoxOfficeSaleOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const reply2 = results2.at(-1) ?? {};
    const { context: context3, ...body3 } = request;
    const input3 = { ...body3, context: context3 };
    const request3 = {
      context: input3?.context,
      details: {
        destination: {
          recipient: (reply2?.box_office_sale?.billing_address?.recipient ?? ''),
        },
      },
      guest_list: {
        billing_address: { region: (reply2?.box_office_sale?.billing_address?.region ?? '') },
        counterparty: {
          currency_code: (reply2?.box_office_sale?.counterparty?.currency_code ?? ''),
        },
      },
    };
    const reply3 = await call((done) => clients.boxofficeGuestListCalculationClient.simulateGuestList(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        cash_drawer: {
          audit: { etag: (reply3?.guest_list?.audit?.etag ?? '') },
          currency_code: (reply3?.guest_list?.counterparty?.currency_code ?? ''),
        },
      },
      {
        cash_drawer: {
          cash_drawer_id: (request?.cash_drawer_id ?? ''),
          contact: request?.cash_drawer?.contact,
        },
      },
    );
  }

  return {
    checkCashDrawerEligibility: unary(checkCashDrawerEligibility),
    getCashDrawerRules: unary(getCashDrawerRules),
    lintCashDrawers: unary(lintCashDrawers),
    normalizeCashDrawer: unary(normalizeCashDrawer),
    previewCashDrawer: unary(previewCashDrawer),
    scoreCashDrawer: unary(scoreCashDrawer),
    validateCashDrawer: unary(validateCashDrawer),
  };
}

module.exports = { createCashDrawerValidationHandlers };
