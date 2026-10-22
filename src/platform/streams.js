'use strict';

/** Streams of the platform's clients: read every message of a reply stream, or send a bounded group of requests. */

function collect(stream) {
  return new Promise((resolve, reject) => {
    const messages = [];
    stream.on('data', (message) => messages.push(message));
    stream.on('error', reject);
    stream.on('end', () => resolve(messages));
  });
}

function upload(open, requests) {
  return new Promise((resolve, reject) => {
    const stream = open((error, reply) => (error ? reject(error) : resolve(reply)));
    stream.on('error', reject);
    for (const request of requests) stream.write(request);
    stream.end();
  });
}

function exchange(stream, requests) {
  const reading = collect(stream);
  for (const request of requests) stream.write(request);
  stream.end();
  return reading;
}

module.exports = { collect, upload, exchange };
