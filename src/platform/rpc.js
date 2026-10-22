'use strict';

/** Promise ports over grpc-js's callback clients, which the domains' operations make their calls through. */
const grpc = require('@grpc/grpc-js');
const { RpcError } = require('./errors');

function call(start) {
  return new Promise((resolve, reject) => {
    start((error, reply) => {
      if (error) reject(error);
      else if (reply === undefined) reject(new RpcError(grpc.status.INTERNAL, 'The upstream service returned no message'));
      else resolve(reply);
    });
  });
}

async function withRetry(work) {
  try {
    return await work();
  } catch (error) {
    if (error?.code !== grpc.status.UNAVAILABLE) throw error;
    return work();
  }
}

function deferred(work) {
  return () => work();
}

/** Fill a response from the parts its calls contributed: later nonempty fields win, lists keep every item. */
function assemble(...parts) {
  function merge(previous, next) {
    if (next === undefined || next === null || next === '' || next === 0 || next === false) return previous ?? next;
    if (Array.isArray(next)) return [...(Array.isArray(previous) ? previous : []), ...next];
    if (typeof next === 'object' && !Buffer.isBuffer(next)) {
      const base = previous !== null && typeof previous === 'object' ? { ...previous } : {};
      for (const [key, value] of Object.entries(next)) base[key] = merge(base[key], value);
      return base;
    }
    return next;
  }
  return parts.reduce((previous, next) => merge(previous, next), {});
}

module.exports = { call, withRetry, deferred, assemble };
