/// <reference path="../global.d.ts" />
'use strict'

const { DataIngester } = require('../lib/ingest')

/** @param {import('fastify').FastifyInstance} fastify */
module.exports = async function (fastify, opts) {
  // Initialize data ingester with Fastify's logger
  const ingester = new DataIngester(fastify.log)

  // Flag to track ingestion status
  let isReady = false

  // Start ingestion in the background (don't block server startup)
  ingester.ingest().then(() => {
    isReady = true
    fastify.log.info('Data ingestion completed, server is ready')
  }).catch(err => {
    fastify.log.error({ err }, 'Initial data ingestion failed')
  })

  // Schedule periodic refresh every 24 hours
  setInterval(() => {
    ingester.ingest().catch(err => fastify.log.error({ err }, 'Periodic ingestion failed'))
  }, 24 * 60 * 60 * 1000)

  fastify.get('/metrics', async (request, reply) => {
    const db = require('../lib/db')

    // Check if data is available
    const versionRows = db.getDailyVersionDownloads()

    // If no data yet and ingestion is still running, return a temporary error
    if (versionRows.length === 0 && !isReady) {
      reply.code(503)
      reply.header('Retry-After', '30')
      return { error: 'Data is still loading, please retry in a few minutes' }
    }

    // Fetch monthly aggregated data from SQLite
    const monthlyVersionRows = db.getMonthlyVersionDownloads()
    const monthlyOsRows = db.getMonthlyOsDownloads()

    // Fetch daily data for backward compatibility with frontend
    const dailyOsRows = db.getDailyOsDownloads()

    // Transform version data into the expected format
    // Format: { "4": [{ date: "2024-01-01", downloads: 123 }, ...] }
    const versions = {}
    for (const { major_version, date, downloads } of versionRows) {
      const key = String(major_version)
      if (!versions[key]) {
        versions[key] = []
      }
      versions[key].push({ date, downloads })
    }

    // Transform OS data into the expected format
    // Format: { "linux": [{ date: "2024-01-01", downloads: 123 }, ...] }
    const operatingSystems = {}
    for (const { os, date, downloads } of dailyOsRows) {
      if (!operatingSystems[os]) {
        operatingSystems[os] = []
      }
      operatingSystems[os].push({ date, downloads })
    }

    // Sort arrays by date
    for (const key in versions) {
      versions[key].sort((a, b) => a.date.localeCompare(b.date))
    }
    for (const key in operatingSystems) {
      operatingSystems[key].sort((a, b) => a.date.localeCompare(b.date))
    }

    // Also include monthly aggregated format for the byVersion/byOs response
    const byVersion = {}
    for (const { major_version, month, total_downloads } of monthlyVersionRows) {
      const key = `v${major_version}`
      if (!byVersion[key]) {
        byVersion[key] = {}
      }
      byVersion[key][month] = total_downloads
    }

    const byOs = {}
    for (const { os, month, total_downloads } of monthlyOsRows) {
      if (!byOs[os]) {
        byOs[os] = {}
      }
      byOs[os][month] = total_downloads
    }

    const res = {
      // Original format for backward compatibility
      versions,
      operatingSystems,
      // New aggregated format
      byVersion,
      byOs
    }

    // Cache headers - content is stable for 1 hour
    const oneHour = 60 * 60
    reply.header('Cache-Control', `max-age=${oneHour}, s-maxage=${oneHour}, stale-while-revalidate=${oneHour}, stale-if-error=${oneHour}`)

    return res
  })
}
