/// <reference path="../global.d.ts" />
'use strict'

const fastifyStatic = require('@fastify/static')

/** @param {import('fastify').FastifyInstance} fastify */
module.exports = async function (fastify, opts) {
  fastify.register(fastifyStatic, {
    root: `${__dirname}/../public`,
    prefix: '/'
  })
}
