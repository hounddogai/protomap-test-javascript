'use strict';

/**
 * Computes the figures of a guest list, such as totals, taxes, and estimates.
 */

const grpc = require('@grpc/grpc-js');
const { promisify } = require('node:util');
const { runInContext, validateContext } = require('../../../platform/context');
const { deadline, metadataFor } = require('../../../platform/credentials');
const { RpcError } = require('../../../platform/errors');
const { unary } = require('../../../platform/handlers');
const { assemble, call, deferred, withRetry } = require('../../../platform/rpc');

/**
 * The handlers of GuestListCalculationService, over the dependencies the boxoffice team is given.
 */
function createGuestListCalculationHandlers({ store, clients, bus, logger }) {
  /**
   * Calculates a guest list.
   */
  async function calculateGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'CalculateGuestList', request);
    logger.info('calculate_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      venue_section: {
        children: [{
          children: [{
            location: { locality: (request?.guest_list?.billing_address?.locality ?? '') },
          }],
        }],
      },
    };
    const send1 = clients.venuesVenueSectionSyncClient.syncVenueSections.bind(clients.venuesVenueSectionSyncClient);
    const reply1 = await call((done) => send1(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        guest_list: {
          billing_address: { country_code: (reply1?.venue_section?.location?.country_code ?? '') },
        },
      },
      {
        guest_list: {
          guest_list_id: (request?.guest_list_id ?? ''),
          issued_at: request?.guest_list?.issued_at,
        },
      },
    );
  }

  /**
   * Compares a guest list.
   */
  async function compareGuestListOptions(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'CompareGuestListOptions', request);
    logger.info('compare_guest_list_options', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const settled1 = await Promise.allSettled((request?.guest_list?.lines ?? []).map(async (item) => {
      const request1 = {
        context: request?.context,
        stream_key: {
          audit: {
            created_at: request?.guest_list?.audit?.created_at,
            updated_by: (request?.guest_list?.audit?.updated_by ?? ''),
          },
          limits: [{ amount: item?.tax?.amount }],
        },
      };
      const reply1 = await call((done) => clients.broadcastsStreamKeyCalculationClient.compareStreamKeyOptions(request1, metadataFor(request1.context), { deadline: deadline() }, done));
      return reply1;
    }));
    const results1 = settled1.map((outcome) => {
      if (outcome.status === 'rejected') throw outcome.reason;
      return outcome.value;
    });
    const reply1 = results1.at(-1) ?? {};
    const request2 = {
      box_office_sale: {
        audit: { etag: (request?.guest_list?.audit?.etag ?? '') },
        billing_address: { country_code: (request?.details?.destination?.country_code ?? '') },
        card: {
          holder_name: ((request?.guest_list?.settlement === 'card' ? request?.guest_list.card : undefined)?.holder_name ?? ''),
        },
        counterparty: { display_name: (request?.guest_list?.counterparty?.display_name ?? '') },
      },
      context: request?.context,
    };
    const spread2 = {
      ...request2,
      context: request2.context ? { ...request2.context, baggage: { ...request2.context.baggage } } : null,
    };
    const reply2 = await call((done) => clients.boxofficeBoxOfficeSaleValidationClient.validateBoxOfficeSale(spread2, metadataFor(spread2.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { message: (reply2?.result?.message ?? '') } },
      {
        guest_list: {
          counterparty: request?.guest_list?.counterparty,
          guest_list_id: (request?.guest_list_id ?? ''),
        },
      },
    );
  }

  /**
   * Estimates a guest list.
   */
  async function estimateGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'EstimateGuestList', request);
    logger.info('estimate_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const { context: context1, ...body1 } = request;
    const input1 = { ...body1, context: context1 };
    const request1 = {
      context: input1?.context,
      wristband: {
        address: {
          country_code: (input1?.guest_list?.billing_address?.country_code ?? ''),
        },
        audit: { updated_by: (input1?.guest_list?.audit?.updated_by ?? '') },
      },
    };
    const reply1 = await call((done) => clients.admissionsWristbandValidationClient.validateWristband(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const results2 = [];
    for (const item of (request?.guest_list?.lines ?? [])) {
      const request2 = {
        box_office_sale: {
          lines: [{
            tax: {
              exemptions: [...(item?.tax?.exemptions ?? [])],
              jurisdiction: (item?.tax?.jurisdiction ?? ''),
            },
          }],
        },
        context: request?.context,
      };
      const reply2 = await call((done) => clients.boxofficeBoxOfficeSaleValidationClient.explainBoxOfficeSaleDecision(request2, metadataFor(request2.context), { deadline: deadline() }, done));
      results2.push(reply2);
    }
    const reply2 = results2.at(-1) ?? {};
    const results3 = await Promise.all((request?.guest_list?.attachments ?? []).map(async (item) => {
      const request3 = {
        box_office_sale: { attachments: [{ content_type: (item?.content_type ?? '') }] },
        context: request?.context,
      };
      const reply3 = await call((done) => clients.boxofficeBoxOfficeSaleValidationClient.checkBoxOfficeSaleEligibility(request3, metadataFor(request3.context), { deadline: deadline() }, done));
      return reply3;
    }));
    const reply3 = results3.at(-1) ?? {};
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        guest_list: { audit: { created_at: reply1?.wristband?.audit?.created_at } },
      },
      {
        guest_list: {
          guest_list_id: (request?.guest_list_id ?? ''),
          total: request?.guest_list?.total,
        },
      },
    );
  }

  /**
   * Explains a guest list.
   */
  async function explainGuestListCalculation(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'ExplainGuestListCalculation', request);
    logger.info('explain_guest_list_calculation', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      wristband: {
        address: {
          country_code: (request?.guest_list?.billing_address?.country_code ?? ''),
        },
      },
    };
    const cloned1 = JSON.parse(JSON.stringify(request1));
    const reply1 = await call((done) => clients.admissionsWristbandCalculationClient.calculateWristband(cloned1, metadataFor(cloned1.context), { deadline: deadline() }, done));
    const request2 = {
      context: request?.context,
      filter: {
        created_after: request?.guest_list?.audit?.created_at,
        owner_id: (request?.guest_list?.counterparty?.cash_drawer_id ?? ''),
        region: ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.region ?? ''),
      },
    };
    const copied2 = structuredClone(request2);
    const reply2 = await call((done) => clients.broadcastsBroadcastClient.searchBroadcasts(copied2, metadataFor(copied2.context), { deadline: deadline() }, done));
    let selected3_1;
    switch (reply1?.wristband?.locator) {
      case 'address':
        selected3_1 = ((reply1?.wristband?.locator === 'address' ? reply1?.wristband.address : undefined)?.region ?? '');
        break;
      default:
        selected3_1 = '';
    }
    const request3 = {
      context: request?.context,
      filter: {
        created_after: request?.guest_list?.audit?.updated_at,
        labels: { ...(request?.guest_list?.metadata ?? {}) },
        owner_id: (request?.guest_list?.counterparty?.cash_drawer_id ?? ''),
        region: selected3_1,
      },
    };
    const reply3 = await call((done) => clients.featureflagsFeatureFlagQueryClient.suggestFeatureFlags(request3, metadataFor(request3.context), { deadline: deadline() }, done));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        guest_list: { audit: { etag: (reply1?.wristband?.audit?.etag ?? '') } },
      },
      { result: { correlation_id: (reply2?.result?.correlation_id ?? '') } },
      { result: { message: (reply3?.result?.message ?? '') } },
      {
        guest_list: {
          guest_list_id: (request?.guest_list_id ?? ''),
          status: (request?.guest_list?.status ?? ''),
        },
      },
    );
  }

  /**
   * Returns a guest list.
   */
  async function getGuestListRates(request) {
    validateContext(request.context);
    if (!request.guest_list_id?.trim()) {
      throw new RpcError(grpc.status.INVALID_ARGUMENT, 'guest_list_id is required');
    }
    await store.record(request.context.tenant_id, 'GetGuestListRates', request);
    logger.info('get_guest_list_rates', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    return { guest_list: { guest_list_id: (request?.guest_list_id ?? '') } };
  }

  /**
   * Quotes a guest list.
   */
  async function quoteGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'QuoteGuestList', request);
    logger.info('quote_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      box_office_sale: {
        billing_address: { postal_code: (request?.details?.destination?.postal_code ?? '') },
        counterparty: {
          currency_code: (request?.guest_list?.counterparty?.currency_code ?? ''),
        },
      },
      context: request?.context,
    };
    const reply1 = await withRetry(() => call((done) => clients.boxofficeBoxOfficeSaleValidationClient.checkBoxOfficeSaleEligibility(request1, metadataFor(request1.context), { deadline: deadline() }, done)));
    const request2 = {
      box_office_sale: {
        audit: {
          created_by: (reply1?.box_office_sale?.audit?.created_by ?? ''),
          etag: (request?.guest_list?.audit?.etag ?? ''),
        },
        billing_address: {
          postal_code: (reply1?.box_office_sale?.billing_address?.postal_code ?? ''),
        },
      },
      context: request?.context,
      details: {
        destination: { region: (reply1?.box_office_sale?.billing_address?.region ?? '') },
      },
    };
    const task2 = deferred(() => call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.quoteBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    const reply2 = await task2();
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        breakdown: { total: reply1?.box_office_sale?.total },
        guest_list: { due_at: reply1?.box_office_sale?.due_at },
      },
      {
        guest_list: {
          audit: { updated_by: (reply2?.box_office_sale?.audit?.updated_by ?? '') },
        },
        result: { correlation_id: (reply2?.result?.correlation_id ?? '') },
      },
      {
        breakdown: { currency_code: (request?.details?.currency_code ?? '') },
        guest_list: { guest_list_id: (request?.guest_list_id ?? '') },
      },
    );
  }

  /**
   * Recalculates a guest list.
   */
  async function recalculateGuestLists(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'RecalculateGuestLists', request);
    logger.info('recalculate_guest_lists', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      context: request?.context,
      filter: {
        created_after: request?.guest_list?.issued_at,
        labels: { ...(request?.guest_list?.metadata ?? {}) },
        owner_id: (request?.guest_list?.counterparty?.cash_drawer_id ?? ''),
        region: (request?.guest_list?.billing_address?.region ?? ''),
      },
    };
    const reply1 = await bus.request('acme.boxoffice.v1.GuestListCalculationService/RecalculateGuestLists:0', request1);
    const request2 = {
      box_office_sale: {
        bank: {
          iban: ((request?.guest_list?.settlement === 'bank' ? request?.guest_list.bank : undefined)?.iban ?? ''),
        },
        billing_address: {
          country_code: (request?.details?.destination?.country_code ?? ''),
          recipient: (request?.details?.destination?.recipient ?? ''),
        },
        counterparty: {
          cash_drawer_id: (request?.guest_list?.counterparty?.cash_drawer_id ?? ''),
        },
      },
      context: request?.context,
    };
    const reply2 = await runInContext(request.context, () => call((done) => clients.boxofficeBoxOfficeSaleValidationClient.validateBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, done)));
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      {
        result: {
          details: (reply1?.result?.details ?? {}),
          reference_ids: (reply1?.result?.reference_ids ?? []),
        },
      },
      {
        breakdown: { total: reply2?.box_office_sale?.total },
        guest_list: {
          counterparty: {
            cash_drawer_id: (reply2?.box_office_sale?.counterparty?.cash_drawer_id ?? ''),
          },
        },
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
   * Simulates a guest list.
   */
  async function simulateGuestList(request) {
    validateContext(request.context);
    if (request.guest_list) {
      await store.save(store.toGuestListRecord(request.context.tenant_id, request.guest_list));
    }
    await store.record(request.context.tenant_id, 'SimulateGuestList', request);
    logger.info('simulate_guest_list', { tenant: request.context.tenant_id, trace: request.context.trace_id });
    const request1 = {
      box_office_sale: {
        audit: { etag: (request?.guest_list?.audit?.etag ?? '') },
        billing_address: {
          country_code: (request?.details?.destination?.country_code ?? ''),
          region: (request?.details?.destination?.region ?? ''),
        },
        card: {
          network: ((request?.guest_list?.settlement === 'card' ? request?.guest_list.card : undefined)?.network ?? ''),
        },
      },
      context: request?.context,
    };
    const reply1 = await call((done) => clients.boxofficeBoxOfficeSaleCalculationClient.explainBoxOfficeSaleCalculation(request1, metadataFor(request1.context), { deadline: deadline() }, done));
    const request2 = {
      box_office_sale: {
        audit: { created_by: (request?.guest_list?.audit?.created_by ?? '') },
        billing_address: {
          country_code: (request?.guest_list?.billing_address?.country_code ?? ''),
          locality: (request?.details?.destination?.locality ?? ''),
        },
      },
      context: request?.context,
    };
    const reply2 = await new Promise((resolve, reject) => {
      clients.boxofficeBoxOfficeSaleValidationClient.validateBoxOfficeSale(request2, metadataFor(request2.context), { deadline: deadline() }, (error, response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
    const request3 = {
      context: request?.context,
      details: {
        destination: { lines: [...(request?.guest_list?.billing_address?.lines ?? [])] },
      },
      notification: {
        audit: { etag: (request?.guest_list?.audit?.etag ?? '') },
        customer: {
          display_name: (reply2?.box_office_sale?.counterparty?.display_name ?? ''),
        },
      },
    };
    const scoreNotification3 = promisify(clients.notificationsNotificationValidationClient.scoreNotification.bind(clients.notificationsNotificationValidationClient));
    const reply3 = await scoreNotification3(request3, metadataFor(request3.context), { deadline: deadline() });
    logger.info('local_audit', { value: (request?.reason ?? '') });
    return assemble(
      { result: { status: (reply1?.outcome?.status ?? '') } },
      {
        guest_list: {
          billing_address: {
            recipient: (reply2?.box_office_sale?.billing_address?.recipient ?? ''),
          },
          metadata: (reply2?.box_office_sale?.metadata ?? {}),
        },
      },
      {
        guest_list: {
          counterparty: { display_name: (reply3?.notification?.assignee?.display_name ?? '') },
        },
      },
      {
        guest_list: {
          due_at: request?.guest_list?.due_at,
          guest_list_id: (request?.guest_list_id ?? ''),
        },
      },
    );
  }

  return {
    calculateGuestList: unary(calculateGuestList),
    compareGuestListOptions: unary(compareGuestListOptions),
    estimateGuestList: unary(estimateGuestList),
    explainGuestListCalculation: unary(explainGuestListCalculation),
    getGuestListRates: unary(getGuestListRates),
    quoteGuestList: unary(quoteGuestList),
    recalculateGuestLists: unary(recalculateGuestLists),
    simulateGuestList: unary(simulateGuestList),
  };
}

module.exports = { createGuestListCalculationHandlers };
