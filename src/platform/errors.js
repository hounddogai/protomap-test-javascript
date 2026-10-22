'use strict';

/** Failures whose public message is safe to return to a caller, and the translation of every other failure. */
const grpc = require('@grpc/grpc-js');

class RpcError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

function toServiceError(error) {
  if (error instanceof Error && typeof error.code === 'number') {
    return Object.assign(error, { details: error.message, metadata: new grpc.Metadata() });
  }
  const failure = new Error('The operation could not be completed');
  return Object.assign(failure, {
    code: grpc.status.INTERNAL, details: failure.message, metadata: new grpc.Metadata(),
  });
}

module.exports = { RpcError, toServiceError };
