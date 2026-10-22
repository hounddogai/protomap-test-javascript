'use strict';

/** The one PostgreSQL pool of the process, opened on first use, and the tables the domains write. */
const { Pool } = require('pg');

let pool = null;

function database() {
  if (!pool) {
    if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required');
    pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 20, connectionTimeoutMillis: 5_000 });
  }
  return pool;
}

async function save(record) {
  await database().query(
    `INSERT INTO live_records (tenant_id, kind, record_id, payload) VALUES ($1, $2, $3, $4)
     ON CONFLICT (tenant_id, kind, record_id) DO UPDATE SET payload = EXCLUDED.payload`,
    [record.tenant_id, record.kind, record.record_id, JSON.stringify(record.document)],
  );
}

async function record(tenantId, domain, action, payload) {
  await database().query(
    'INSERT INTO live_actions (tenant_id, domain, action, payload) VALUES ($1, $2, $3, $4)',
    [tenantId, domain, action, JSON.stringify(payload)],
  );
}

/** Deployment runs this before the server takes traffic; handlers never create tables. */
async function migrate() {
  await database().query(`CREATE TABLE IF NOT EXISTS live_records (
    tenant_id text NOT NULL, kind text NOT NULL, record_id text NOT NULL, payload jsonb NOT NULL,
    PRIMARY KEY (tenant_id, kind, record_id))`);
  await database().query(`CREATE TABLE IF NOT EXISTS live_actions (
    id bigserial PRIMARY KEY, tenant_id text NOT NULL, domain text NOT NULL, action text NOT NULL,
    payload jsonb NOT NULL)`);
}

async function close() {
  if (pool) {
    const closing = pool;
    pool = null;
    await closing.end();
  }
}

module.exports = { save, record, migrate, close };
