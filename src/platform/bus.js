'use strict';

/** Request/reply work that workers installed at startup take on the next turn of the event loop. */
const { EventEmitter } = require('node:events');
const grpc = require('@grpc/grpc-js');
const { RpcError } = require('./errors');

class WorkBus {
  constructor() {
    this.events = new EventEmitter();
    this.pending = new Set();
    this.stopped = false;
  }

  consume(topic, work) {
    if (this.events.listenerCount(topic)) throw new Error(`A worker already takes ${topic}`);
    this.events.on(topic, (item) => {
      setImmediate(() => {
        if (this.stopped) {
          item.reject(new RpcError(grpc.status.CANCELLED, 'The worker stopped'));
          return;
        }
        work(item.request).then(item.resolve, item.reject);
      });
    });
  }

  request(topic, request) {
    return new Promise((resolve, reject) => {
      if (this.stopped || !this.events.listenerCount(topic)) {
        reject(new RpcError(grpc.status.UNAVAILABLE, `No worker takes ${topic}`));
        return;
      }
      const fail = (error) => {
        clearTimeout(timer);
        this.pending.delete(fail);
        reject(error);
      };
      const timer = setTimeout(() => fail(new RpcError(grpc.status.DEADLINE_EXCEEDED, 'The worker timed out')), 30_000);
      this.pending.add(fail);
      this.events.emit(topic, {
        request,
        reject: fail,
        resolve: (reply) => {
          clearTimeout(timer);
          this.pending.delete(fail);
          resolve(reply);
        },
      });
    });
  }

  close() {
    this.stopped = true;
    for (const reject of this.pending) reject(new RpcError(grpc.status.CANCELLED, 'The worker stopped'));
    this.events.removeAllListeners();
  }
}

module.exports = { WorkBus, bus: new WorkBus() };
