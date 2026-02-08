'use strict'

const { Database } = require('../lib/db')

/** @param {import('fastify').FastifyInstance} fastify */
module.exports = async function (fastify, opts) {
  // Get config from parent or use opts
  const config = fastify.config || opts

  if (!config.NODEJS_DOWNLOAD_STATS_DB) {
    throw new Error('NODEJS_DOWNLOAD_STATS_DB is required in config')
  }

  // Create database instance
  const db = new Database(config.NODEJS_DOWNLOAD_STATS_DB)
  db.initSchema()

  // Decorate fastify with database
  fastify.decorate('db', db)

  // Close database on shutdown
  fastify.addHook('onClose', async () => {
    fastify.log.info('Closing database connection')
    db.closeDb()
  })
}
