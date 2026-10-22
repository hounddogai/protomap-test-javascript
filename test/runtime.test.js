'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const grpc = require('@grpc/grpc-js');
const { WorkBus } = require('../src/platform/bus');
const { toServiceError } = require('../src/platform/errors');
const { assemble } = require('../src/platform/rpc');
const { validateContext } = require('../src/platform/context');

test('a response keeps earlier nonempty fields and every contributed list item', () => {
  const first = { result: { status: '', message: 'created' }, records: ['one'] };
  const second = { result: { status: 'ok', message: '' }, records: ['two'] };
  assert.deepEqual(assemble(first, second), { result: { status: 'ok', message: 'created' }, records: ['one', 'two'] });
  assert.deepEqual(first.records, ['one']);
});

test('workers answer the requests of their topic and fail promptly without one', async () => {
  const bus = new WorkBus();
  bus.consume('echo', async (request) => ({ ...request, answered: true }));
  try {
    const replies = await Promise.all(['alpha', 'beta'].map((tenant_id) => bus.request('echo', { tenant_id })));
    assert.deepEqual(replies, [{ tenant_id: 'alpha', answered: true }, { tenant_id: 'beta', answered: true }]);
    await assert.rejects(bus.request('missing', {}), { code: grpc.status.UNAVAILABLE });
    bus.consume('blocked', () => new Promise(() => {}));
    const pending = bus.request('blocked', {});
    bus.close();
    await assert.rejects(pending, { code: grpc.status.CANCELLED });
  } finally {
    bus.close();
  }
});

test('a request names its tenant', () => {
  assert.throws(() => validateContext({ tenant_id: ' ' }), { code: grpc.status.INVALID_ARGUMENT });
  validateContext({ tenant_id: 'acme' });
});

test('unexpected failures disclose nothing of their cause', () => {
  const error = toServiceError(new Error('the database password was rejected'));
  assert.equal(error.code, grpc.status.INTERNAL);
  assert.equal(error.details, 'The operation could not be completed');
});
