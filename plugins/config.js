'use strict'

const envSchema = require('env-schema')
const { schema } = require('../lib/config')

/** @param {import('fastify').FastifyInstance} fastify */
module.exports = async function (fastify, opts) {
  // Parse environment variables using schema
  // Note: dotenv is NOT used here - we rely on process.env being populated.
  // For local dev, load .env before starting: `source .env && npm start`
  // For tests, set env vars directly.
  const data = process.env
  const config = envSchema({ schema, data })

  // Decorate fastify with config
  fastify.decorate('config', config)
}

// Make plugin non-encapsulated so decorators are shared
module.exports[Symbol.for('skip-override')] = true
