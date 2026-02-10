/// <reference path="../global.d.ts" />
'use strict'

const { DataIngester } = require('../lib/ingest')

/** @param {import('fastify').FastifyInstance} fastify */
module.exports = async function (fastify, opts) {
  // Only register admin routes if explicitly enabled via env var
  const adminAuthToken = process.env.NODEJS_DOWNLOAD_STATS_ADMIN_AUTH
  if (!adminAuthToken) {
    return // Skip registration entirely if admin API is not enabled
  }

  // Get database from fastify decorator
  const db = fastify.db
  if (!db) {
    throw new Error('Database not initialized - ensure database plugin is registered before admin routes')
  }

  // Store reference to ingester for stats
  let ingester = null

  // Register bearer auth plugin for protected routes
  await fastify.register(require('@fastify/bearer-auth'), {
    keys: new Set([adminAuthToken]),
    // Only apply auth to specific routes
    addHook: false // We'll manually specify which routes need auth
  })

  // Expose ingestion stats (read-only, no auth needed)
  fastify.get('/admin/ingestion-stats', async (request, reply) => {
    const versionRows = db.getDailyVersionDownloads()
    const monthlyVersionRows = db.getMonthlyVersionDownloads()

    // Calculate unique versions found
    const versions = new Set()
    for (const { major_version } of monthlyVersionRows) {
      versions.add(major_version)
    }

    const stats = {
      dailyRecords: versionRows.length,
      monthlyRecords: monthlyVersionRows.length,
      uniqueVersions: [...versions].sort((a, b) => a - b),
      totalDailyDownloads: versionRows.reduce((sum, row) => sum + row.downloads, 0),
      lastUpdate: db.getLastUpdate(),
      mostRecentDate: db.getMostRecentDate(),
      ingesterStats: ingester ? ingester.getStats() : null
    }

    return stats
  })

  // Trigger ingestion (requires auth)
  fastify.post('/admin/retrigger-ingestion', {
    onRequest: fastify.verifyBearerAuth,
    schema: {
      body: {
        type: 'object',
        properties: {
          clearData: { type: 'boolean', default: false },
          resetOnly: { type: 'boolean', default: false }
        },
        additionalProperties: false
      }
    }
  }, async (request, reply) => {
    const { clearData, resetOnly } = request.body

    if (resetOnly) {
      if (ingester) {
        ingester.reset()
      }
      return { message: 'Ingestion state reset' }
    }

    // Clear data if requested
    if (clearData) {
      fastify.log.warn('Clearing all download data as requested')
      db.clearData()
    }

    // Create new ingester and trigger ingestion
    ingester = new DataIngester(fastify.log, db)

    // Start ingestion in background
    ingester.ingest().catch(err => {
      fastify.log.error({ err }, 'Manual ingestion failed')
    })

    return {
      message: 'Ingestion triggered',
      clearData,
      status: ingester.getStats()
    }
  })

  // Get raw data for a specific date (for debugging)
  fastify.get('/admin/raw-data/:date', {
    onRequest: fastify.verifyBearerAuth
  }, async (request, reply) => {
    const { date } = request.params

    // Validate date format
    if (!date.match(/^\d{4}-\d{2}-\d{2}$/)) {
      reply.code(400)
      return { error: 'Invalid date format, expected YYYY-MM-DD' }
    }

    // Fetch from GCS
    try {
      const undici = require('undici')
      const url = `https://storage.googleapis.com/access-logs-summaries-nodejs/nodejs.org-access.log.${date.replace(/-/g, '')}.json`
      const response = await undici.request(url)
      const data = await response.body.json()

      return {
        date,
        url,
        versions: Object.keys(data.version || {}).sort(),
        os: Object.keys(data.os || {}).sort(),
        sampleVersionData: Object.entries(data.version || {}).slice(0, 5).map(([k, v]) => ({ version: k, downloads: v }))
      }
    } catch (err) {
      reply.code(500)
      return { error: 'Failed to fetch raw data', details: err.message }
    }
  })

  // Health check
  fastify.get('/admin/health', async (request, reply) => {
    const versionRows = db.getDailyVersionDownloads()
    const monthlyVersionRows = db.getMonthlyVersionDownloads()

    return {
      healthy: true,
      database: {
        dailyRecords: versionRows.length,
        monthlyRecords: monthlyVersionRows.length,
        uniqueVersions: new Set(monthlyVersionRows.map(r => r.major_version)).size
      }
    }
  })
}
