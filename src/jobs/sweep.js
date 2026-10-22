'use strict';

/**
 * The sweep job refreshes downstream views from paged records.
 */

const grpc = require('@grpc/grpc-js');
const clients = require('../platform/clients');
const { call } = require('../platform/rpc');

const tenant = process.env.ACME_TENANT ?? 'acme';
const metadata = new grpc.Metadata();
metadata.set('authorization', `Bearer ${process.env.ACME_SERVICE_TOKEN ?? ''}`);
metadata.set('x-tenant-id', tenant);
// The program acts with the server's credential, as its service account, which the servers downstream require.
const actor = { user_id: 'live-sweep', service_account: true };

async function run() {
  let pageToken0 = '';
  do {
    const page = await call((done) => clients.admissionsAdmissionClient.listAdmissions(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken0 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.admissions) {
      await call((done) => clients.admissionsAdmissionClient.getAdmission(
        { context: { tenant_id: tenant, actor }, admission_id: record.admission_id },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
    }
    pageToken0 = page.page_info?.next_page_token ?? '';
  } while (pageToken0);
  let pageToken1 = '';
  do {
    const page = await call((done) => clients.admissionsAdmissionClient.searchAdmissions(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken1 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.admissions) {
      await call((done) => clients.admissionsAdmissionClient.getAdmission(
        { context: { tenant_id: tenant, actor }, admission_id: record.admission_id },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
    }
    pageToken1 = page.page_info?.next_page_token ?? '';
  } while (pageToken1);
  let pageToken2 = '';
  do {
    const page = await call((done) => clients.boxofficeBoxOfficeSaleClient.listBoxOfficeSales(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken2 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.box_office_sales) {
      await call((done) => clients.boxofficeBoxOfficeSaleClient.getBoxOfficeSale(
        { context: { tenant_id: tenant, actor }, box_office_sale_id: record.box_office_sale_id },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
    }
    pageToken2 = page.page_info?.next_page_token ?? '';
  } while (pageToken2);
  let pageToken3 = '';
  do {
    const page = await call((done) => clients.boxofficeBoxOfficeSaleClient.searchBoxOfficeSales(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken3 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.box_office_sales) {
      await call((done) => clients.boxofficeBoxOfficeSaleClient.getBoxOfficeSale(
        { context: { tenant_id: tenant, actor }, box_office_sale_id: record.box_office_sale_id },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
    }
    pageToken3 = page.page_info?.next_page_token ?? '';
  } while (pageToken3);
  let pageToken4 = '';
  do {
    const page = await call((done) => clients.broadcastsBroadcastClient.listBroadcasts(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken4 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.broadcasts) {
      await call((done) => clients.broadcastsBroadcastClient.getBroadcast(
        { context: { tenant_id: tenant, actor }, broadcast_id: record.broadcast_id },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
    }
    pageToken4 = page.page_info?.next_page_token ?? '';
  } while (pageToken4);
  let pageToken5 = '';
  do {
    const page = await call((done) => clients.broadcastsBroadcastClient.searchBroadcasts(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken5 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.broadcasts) {
      await call((done) => clients.broadcastsBroadcastClient.getBroadcast(
        { context: { tenant_id: tenant, actor }, broadcast_id: record.broadcast_id },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
    }
    pageToken5 = page.page_info?.next_page_token ?? '';
  } while (pageToken5);
}

run().then(() => clients.close(), (error) => {
  clients.close();
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});
