'use strict';

/** Every request names its tenant; adapters may read the context of the request they serve without passing it. */
const { AsyncLocalStorage } = require('node:async_hooks');
const grpc = require('@grpc/grpc-js');
const { RpcError } = require('./errors');

const scope = new AsyncLocalStorage();

function validateContext(context) {
  if (!context?.tenant_id?.trim()) throw new RpcError(grpc.status.INVALID_ARGUMENT, 'context.tenant_id is required');
  if (context.tenant_id.length > 128) throw new RpcError(grpc.status.INVALID_ARGUMENT, 'tenant_id is too long');
}

function runInContext(context, work) {
  return scope.run(context, work);
}

function currentContext() {
  return scope.getStore();
}

module.exports = { validateContext, runInContext, currentContext };
