'use strict'

const envSchema = require('env-schema')
const { schema } = require('../lib/config')

/** @param {import('fastify').FastifyInstance} fastify */
module.exports = async function (fastify, opts) {
  // Parse environment variables using schema
  const config = envSchema({
    schema,
    data: process.env,
    dotenv: true // Read from .env file if present
  })

  // Decorate fastify with config
  fastify.decorate('config', config)

  fastify.log.debug({ config }, 'Configuration loaded')
}
