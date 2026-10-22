'use strict';

/**
 * The concessions team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map concession order API values to the database document boundary.
 */
function toConcessionOrderRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'concession_order', record_id: value.concession_order_id, document: value };
}

/**
 * Map concession stand API values to the database document boundary.
 */
function toConcessionStandRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'concession_stand', record_id: value.concession_stand_id, document: value };
}

/**
 * Map menu item API values to the database document boundary.
 */
function toMenuItemRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'menu_item', record_id: value.menu_item_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'concessions', action, payload);
}

module.exports = { toConcessionOrderRecord, toConcessionStandRecord, toMenuItemRecord, save, record };
