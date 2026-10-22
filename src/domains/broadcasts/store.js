'use strict';

/**
 * The broadcasts team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map broadcast API values to the database document boundary.
 */
function toBroadcastRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'broadcast', record_id: value.broadcast_id, document: value };
}

/**
 * Map replay clip API values to the database document boundary.
 */
function toReplayClipRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'replay_clip', record_id: value.replay_clip_id, document: value };
}

/**
 * Map stream key API values to the database document boundary.
 */
function toStreamKeyRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'stream_key', record_id: value.stream_key_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'broadcasts', action, payload);
}

module.exports = { toBroadcastRecord, toReplayClipRecord, toStreamKeyRecord, save, record };
