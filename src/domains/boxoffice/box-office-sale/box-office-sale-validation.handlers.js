'use strict';

/**
 * Checks box office sales against the rules before anything acts on them.
 */

const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of BoxOfficeSaleValidationService, over the dependencies the boxoffice team is given.
 */
function createBoxOfficeSaleValidationHandlers({ store, clients, bus, logger }) {
  /**
   * Checks a box office sale.
   */
  async function checkBoxOfficeSaleEligibility(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'CheckBoxOfficeSaleEligibility', request);
    logger.info('check_box_office_sale_eligibility', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      cash_drawer: {
        contact: {
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
      context: request?.context,
    };
    const reply1 = await withRetry(() => call((done) => clients.boxofficeCashDrawerCalculationClient.recalculateCashDrawers(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      cash_drawer: {
        audit: {
          created_by: (reply1?.cash_drawer?.audit?.created_by ?? ''),
          etag: (request?.box_office_sale?.audit?.etag ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: { lines: [...(request?.box_office_sale?.billing_address?.lines ?? [])] },
      },
    };
    const task2 = deferred(() => call((done) => clients.boxofficeCashDrawerCalculationClient.compareCashDrawerOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    const request3 = {
      box_office_sale: {
        audit: {
          created_by: (reply2?.cash_drawer?.audit?.created_by ?? ''),
          updated_at: reply1?.cash_drawer?.audit?.updated_at,
        },
      },
      context: request?.context,
    };
    const reply3 = await bus.request('acme.boxoffice.v1.BoxOfficeSaleValidationService/CheckBoxOfficeSaleEligibility:2', request3);
    const settled4 = await Promise.allSettled((request?.box_office_sale?.lines ?? []).map(async (item) => {
      const request4 = {
        cash_drawer: {
          card: {
            network: ((reply3?.box_office_sale?.settlement === 'card' ? reply3?.box_office_sale.card : undefined)?.network ?? ''),
          },
          contact: {
            preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
          },
          limits: [{ amount: item?.amount }],
        },
        context: request?.context,
      };
      const reply4 = await call((done) => clients.boxofficeCashDrawerCalculationClient.estimateCashDrawer(request4, metadataFor(request4.context), { deadline: deadline() }, done));
      return reply4;
    }));
    const results4 = settled4.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply4 = results4.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        box_office_sale: { audit: { created_by: (reply1?.cash_drawer?.audit?.created_by ?? '') } },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        box_office_sale: { audit: { updated_at: reply2?.cash_drawer?.audit?.updated_at } },
      },
      {
        box_office_sale: {
          billing_address: {
            country_code: (reply3?.box_office_sale?.billing_address?.country_code ?? ''),
            locality: (reply3?.box_office_sale?.billing_address?.locality ?? ''),
          },
        },
      },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          notes: (request?.box_office_sale?.notes ?? ''),
        },
      },
    );
  }

  /**
   * Explains a box office sale.
   */
  async function explainBoxOfficeSaleDecision(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'ExplainBoxOfficeSaleDecision', request);
    logger.info('explain_box_office_sale_decision', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      concession_stand: { audit: { created_at: request?.box_office_sale?.audit?.created_at } },
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.box_office_sale?.billing_address?.country_code ?? ''),
        },
      },
    };
    const reply1 = await runInContext(request.context, () => call((done) => clients.concessionsConcessionStandCalculationClient.compareConcessionStandOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      admission: {
        measurements: [{ unit: (reply1?.concession_stand?.dimensions?.unit ?? '') }],
      },
      context: request?.context,
    };
    const reply2 = await call((done) => clients.admissionsAdmissionValidationClient.scoreAdmission(request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const results3 = [];
    for (const item of (request?.box_office_sale?.lines ?? [])) {
      const request3 = {
        concession_stand: {
          components: { primary: {
            description: (item?.description ?? ''),
            quantity: (item?.quantity ?? '0'),
          } },
        },
        context: request?.context,
      };
      const reply3 = await call((done) => clients.concessionsConcessionStandCalculationClient.explainConcessionStandCalculation(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      results3.push(reply3);
    }
    const reply3 = results3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        box_office_sale: {
          billing_address: {
            recipient: ((reply1?.concession_stand?.locator === 'address' ? reply1?.concession_stand.address : undefined)?.recipient ?? ''),
          },
        },
        outcome: { status: (reply1?.result?.status ?? '') },
      },
      { outcome: { message: (reply2?.result?.message ?? '') } },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          metadata: (request?.box_office_sale?.metadata ?? {}),
        },
      },
    );
  }

  /**
   * Lints a box office sale.
   */
  async function lintBoxOfficeSales(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'LintBoxOfficeSales', request);
    logger.info('lint_box_office_sales', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const results1 = await Promise.all((request?.box_office_sale?.lines ?? []).map(async (item) => {
      const request1 = {
        box_office_sale: {
          lines: [{
            amount: item?.tax?.amount,
            tax: {
              jurisdiction: (item?.tax?.jurisdiction ?? ''),
              rate: (item?.tax?.rate ?? 0),
            },
          }],
        },
        context: request?.context,
      };
      const reply1 = await call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.compareBoxOfficeSaleOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const reply1 = results1.at(-1) ?? {};
    const settled2 = await Promise.allSettled((request?.box_office_sale?.lines ?? []).map(async (item) => {
      const request2 = {
        cash_drawer: { limits: [{ amount: item?.amount }] },
        context: request?.context,
      };
      const reply2 = await call((done) => clients.boxofficeCashDrawerCalculationClient.compareCashDrawerOptions(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      return reply2;
    }));
    const results2 = settled2.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply2 = results2.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return {
      box_office_sale: {
        box_office_sale_id: (request?.box_office_sale_id ?? ''),
        number: (request?.box_office_sale?.number ?? ''),
      },
    };
  }

  /**
   * Normalizes a box office sale.
   */
  async function normalizeBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'NormalizeBoxOfficeSale', request);
    logger.info('normalize_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      admission: {
        subject: {
          display_name: (request?.box_office_sale?.counterparty?.display_name ?? ''),
        },
      },
      context: request?.context,
      details: { contact: { phone: (request?.details?.contact?.phone ?? '') } },
    };
    const reply1 = await new Promise((resolve, reject) => {
      clients.admissionsAdmissionValidationClient.validateAdmission(request1, metadataFor(request1.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request2 = {
      context: request?.context,
      details: {
        destination: {
          lines: [...(request?.box_office_sale?.billing_address?.lines ?? [])],
          locality: (request?.box_office_sale?.billing_address?.locality ?? ''),
        },
      },
    };
    const recalculateCashDrawers2 = promisify(clients.boxofficeCashDrawerCalculationClient.recalculateCashDrawers.bind(clients.boxofficeCashDrawerCalculationClient));
    const reply2 = await recalculateCashDrawers2(request2, metadataFor(request2.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          correlation_id: (reply1?.admission?.correlation_id ?? ''),
          details: (reply1?.result?.details ?? {}),
        },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          total: request?.box_office_sale?.total,
        },
      },
    );
  }

  /**
   * Previews a box office sale.
   */
  async function previewBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'PreviewBoxOfficeSale', request);
    logger.info('preview_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.box_office_sale?.due_at,
        region: (request?.box_office_sale?.billing_address?.region ?? ''),
      },
    };
    const reply1 = await call((done) => clients.venuesVenueSectionSyncClient.listVenueSectionConflicts(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      cash_drawer: {
        bank: {
          bic: ((request?.box_office_sale?.settlement === 'bank' ? request?.box_office_sale.bank : undefined)?.bic ?? ''),
          iban: ((request?.box_office_sale?.settlement === 'bank' ? request?.box_office_sale.bank : undefined)?.iban ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.box_office_sale?.billing_address?.country_code ?? ''),
          region: (request?.box_office_sale?.billing_address?.region ?? ''),
        },
      },
    };
    const method2 = 'compareCashDrawerOptions';
    const reply2 = await call((done) => clients.boxofficeCashDrawerCalculationClient[method2](request2, metadataFor(request2.context), { deadline: deadline() }, done));
    const request3 = {
      context: request?.context,
      rider: {
        audit: { created_at: request?.box_office_sale?.audit?.created_at },
        owner: {
          display_name: (request?.box_office_sale?.counterparty?.display_name ?? ''),
        },
      },
    };
    const client3 = clients.registry.get('acme.performers.v1.RiderValidationService');
    const reply3 = await call((done) => client3.normalizeRider(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          message: (reply1?.result?.message ?? ''),
        },
      },
      {
        box_office_sale: {
          audit: { updated_by: (reply2?.cash_drawer?.audit?.updated_by ?? '') },
          counterparty: { currency_code: (reply2?.breakdown?.currency_code ?? '') },
        },
      },
      {
        box_office_sale: { audit: { etag: (reply3?.rider?.audit?.etag ?? '') } },
      },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          number: (request?.box_office_sale?.number ?? ''),
        },
      },
    );
  }

  /**
   * Scores a box office sale.
   */
  async function scoreBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'ScoreBoxOfficeSale', request);
    logger.info('score_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    let selected1_3;
    switch (request?.box_office_sale?.settlement) {
      case 'card':
        selected1_3 = ((request?.box_office_sale?.settlement === 'card' ? request?.box_office_sale.card : undefined)?.token ?? '');
        break;
      default:
        selected1_3 = '';
    }
    const request1 = {
      cash_drawer: {
        card: { token: selected1_3 },
        holder: {
          display_name: (request?.box_office_sale?.counterparty?.display_name ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: {
          country_code: (request?.box_office_sale?.billing_address?.country_code ?? ''),
        },
      },
    };
    const reply1 = await call((done) => clients.boxofficeCashDrawerCalculationClient.compareCashDrawerOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      cash_drawer: {
        audit: {
          created_at: reply1?.cash_drawer?.audit?.created_at,
          etag: (reply1?.cash_drawer?.audit?.etag ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: {
          postal_code: (request?.box_office_sale?.billing_address?.postal_code ?? ''),
          recipient: (request?.details?.destination?.recipient ?? ''),
        },
      },
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.boxofficeCashDrawerCalculationClient.estimateCashDrawer(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.result?.status ?? '') } },
      {
        breakdown: { currency_code: (reply2?.breakdown?.currency_code ?? '') },
      },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          issued_at: request?.box_office_sale?.issued_at,
        },
      },
    );
  }

  /**
   * Validates a box office sale.
   */
  async function validateBoxOfficeSale(request) {
    validateContext(request.context);
    if (request.box_office_sale) {
      await store.save(store.toBoxOfficeSaleRecord(request.context.tenant_id, request.box_office_sale));
    }
    await store.record(request.context.tenant_id, 'ValidateBoxOfficeSale', request);
    logger.info('validate_box_office_sale', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      annotations: { ...(input1?.annotations ?? {}) },
      context: input1?.context,
      details: { filters: { ...(input1?.box_office_sale?.metadata ?? {}) } },
      reason: (input1?.reason ?? ''),
    };
    const reply1 = await call((done) => clients.documentsDocumentQueryClient.summarizeDocuments(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      sponsor: {
        audit: {
          created_by: (request?.box_office_sale?.audit?.created_by ?? ''),
          etag: (request?.box_office_sale?.audit?.etag ?? ''),
        },
        primary_address: {
          lines: [...(request?.box_office_sale?.billing_address?.lines ?? [])],
          postal_code: (request?.box_office_sale?.billing_address?.postal_code ?? ''),
        },
      },
    };
    const cloned2 = JSON.parse(JSON.stringify(request2));
    const reply2 = await call((done) => clients.sponsorshipsSponsorCalculationClient.recalculateSponsors(cloned2, metadataFor(cloned2.context), { deadline: deadline() }, done));
    const request3 = {
      cash_drawer: {
        card: {
          token: ((request?.box_office_sale?.settlement === 'card' ? request?.box_office_sale.card : undefined)?.token ?? ''),
        },
        contact: {
          preferred_channel: (request?.details?.contact?.preferred_channel ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: {
          recipient: (request?.box_office_sale?.billing_address?.recipient ?? ''),
        },
      },
    };
    const copied3 = structuredClone(request3);
    const reply3 = await call((done) => clients.boxofficeCashDrawerCalculationClient.compareCashDrawerOptions(copied3, metadataFor(copied3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.details?.contact?.phone ?? '') });
    return assemble(
      {
        outcome: { message: (reply1?.result?.message ?? '') },
        result: { details: (reply1?.result?.details ?? {}) },
      },
      {
        box_office_sale: {
          billing_address: { locality: (reply2?.sponsor?.primary_address?.locality ?? '') },
          counterparty: { currency_code: (reply2?.breakdown?.currency_code ?? '') },
        },
      },
      {
        box_office_sale: { audit: { updated_by: (reply3?.cash_drawer?.audit?.updated_by ?? '') } },
      },
      {
        box_office_sale: {
          box_office_sale_id: (request?.box_office_sale_id ?? ''),
          status: (request?.box_office_sale?.status ?? ''),
        },
      },
    );
  }

  return {
    checkBoxOfficeSaleEligibility: unary(checkBoxOfficeSaleEligibility),
    explainBoxOfficeSaleDecision: unary(explainBoxOfficeSaleDecision),
    lintBoxOfficeSales: unary(lintBoxOfficeSales),
    normalizeBoxOfficeSale: unary(normalizeBoxOfficeSale),
    previewBoxOfficeSale: unary(previewBoxOfficeSale),
    scoreBoxOfficeSale: unary(scoreBoxOfficeSale),
    validateBoxOfficeSale: unary(validateBoxOfficeSale),
  };
}

module.exports = { createBoxOfficeSaleValidationHandlers };
