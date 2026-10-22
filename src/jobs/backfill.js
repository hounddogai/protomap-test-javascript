'use strict';

/**
 * The backfill job refreshes downstream views from paged records.
 */

const grpc = require('@grpc/grpc-js');
const clients = require('../platform/clients');
const { call } = require('../platform/rpc');

const tenant = process.env.ACME_TENANT ?? 'acme';
const metadata = new grpc.Metadata();
metadata.set('authorization', `Bearer ${process.env.ACME_SERVICE_TOKEN ?? ''}`);
metadata.set('x-tenant-id', tenant);
// The program acts with the server's credential, as its service account, which the servers downstream require.
const actor = { user_id: 'live-backfill', service_account: true };

async function run() {
  let pageToken0 = '';
  do {
    const page = await call((done) => clients.concessionsConcessionStandClient.searchConcessionStands(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken0 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.concession_stands) {
      await call((done) => clients.concessionsConcessionStandClient.getConcessionStand(
        { context: { tenant_id: tenant, actor }, concession_stand_id: record.concession_stand_id },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
    }
    pageToken0 = page.page_info?.next_page_token ?? '';
  } while (pageToken0);
  let pageToken1 = '';
  do {
    const page = await call((done) => clients.fanportalEventPageClient.searchEventPages(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken1 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.event_pages) {
      await call((done) => clients.fanportalEventPageClient.getEventPage(
        { context: { tenant_id: tenant, actor }, event_page_id: record.event_page_id },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
    }
    pageToken1 = page.page_info?.next_page_token ?? '';
  } while (pageToken1);
  let pageToken2 = '';
  do {
    const page = await call((done) => clients.fanportalFanSessionClient.listFanSessions(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken2 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.fan_sessions) {
      await call((done) => clients.fanportalFanSessionClient.getFanSession(
        { context: { tenant_id: tenant, actor }, fan_session_id: record.fan_session_id },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
    }
    pageToken2 = page.page_info?.next_page_token ?? '';
  } while (pageToken2);
  let pageToken3 = '';
  do {
    const page = await call((done) => clients.fanportalFanSessionClient.searchFanSessions(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken3 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.fan_sessions) {
      await call((done) => clients.fanportalFanSessionClient.getFanSession(
        { context: { tenant_id: tenant, actor }, fan_session_id: record.fan_session_id },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
    }
    pageToken3 = page.page_info?.next_page_token ?? '';
  } while (pageToken3);
  let pageToken4 = '';
  do {
    const page = await call((done) => clients.performersPerformerClient.listPerformers(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken4 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.performers) {
      await call((done) => clients.performersPerformerClient.getPerformer(
        { context: { tenant_id: tenant, actor }, performer_id: record.performer_id },
        metadata, { deadline: Date.now() + 15_000 }, done,
      ));
    }
    pageToken4 = page.page_info?.next_page_token ?? '';
  } while (pageToken4);
  let pageToken5 = '';
  do {
    const page = await call((done) => clients.performersPerformerClient.searchPerformers(
      { context: { tenant_id: tenant, actor }, page: { page_size: 100, page_token: pageToken5 } },
      metadata, { deadline: Date.now() + 30_000 }, done,
    ));
    for (const record of page.performers) {
      await call((done) => clients.performersPerformerClient.getPerformer(
        { context: { tenant_id: tenant, actor }, performer_id: record.performer_id },
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
