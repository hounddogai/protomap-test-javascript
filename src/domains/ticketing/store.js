'use strict';

/**
 * The ticketing team's records, kept in the platform's database.
 */

const database = require('../../platform/database');

/**
 * Map ticket API values to the database document boundary.
 */
function toTicketRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'ticket', record_id: value.ticket_id, document: value };
}

/**
 * Map ticket tier API values to the database document boundary.
 */
function toTicketTierRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'ticket_tier', record_id: value.ticket_tier_id, document: value };
}

/**
 * Map ticket transfer API values to the database document boundary.
 */
function toTicketTransferRecord(tenantId, value) {
  return { tenant_id: tenantId, kind: 'ticket_transfer', record_id: value.ticket_transfer_id, document: value };
}

async function save(record) {
  await database.save(record);
}

async function record(tenantId, action, payload) {
  await database.record(tenantId, 'ticketing', action, payload);
}

module.exports = { toTicketRecord, toTicketTierRecord, toTicketTransferRecord, save, record };
