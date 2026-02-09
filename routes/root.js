/// <reference path="../global.d.ts" />
'use strict'

const { join } = require('node:path')

/** @param {import('fastify').FastifyInstance} fastify */
module.exports = async function (fastify, opts) {
  // Serve static files from public directory
  fastify.register(require('@fastify/static'), {
    root: join(__dirname, '../public'),
    prefix: '/'
  })
}
