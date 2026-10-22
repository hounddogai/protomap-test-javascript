'use strict';

/**
 * The boxoffice team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map box office sale API values to the database document boundary.
 */
function toBoxOfficeSaleRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'box_office_sale', record_id: value.box_office_sale_id, document: value };
}

/**
 * Map cash drawer API values to the database document boundary.
 */
function toCashDrawerRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'cash_drawer', record_id: value.cash_drawer_id, document: value };
}

/**
 * Map guest list API values to the database document boundary.
 */
function toGuestListRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'guest_list', record_id: value.guest_list_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'boxoffice', action, payload);
}

module.exports = { toBoxOfficeSaleRecord, toCashDrawerRecord, toGuestListRecord, save, record };
