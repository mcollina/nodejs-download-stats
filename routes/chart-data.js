/// <reference path="../global.d.ts" />
'use strict'

/** @param {import('fastify').FastifyInstance} fastify */
module.exports = async function (fastify, opts) {
  const db = fastify.db
  if (!db) {
    throw new Error('Database not initialized - ensure database plugin is registered before chart-data routes')
  }

  /**
   * Compute chart data from monthly aggregated database data
   * Returns pre-computed data ready for Chart.js
   */
  function computeChartData () {
    // Get monthly aggregated data from database
    const monthlyVersionRows = db.getMonthlyVersionDownloads()
    const monthlyOsRows = db.getMonthlyOsDownloads()

    // Collect all unique months from both datasets
    const allMonths = new Set()

    // Add months from version data
    for (const { month } of monthlyVersionRows) {
      allMonths.add(month)
    }

    // Add months from OS data
    for (const { month } of monthlyOsRows) {
      allMonths.add(month)
    }

    // Sort months chronologically
    const labels = [...allMonths].sort()

    // Build version chart datasets
    // Group by major version first
    const versionData = {}
    for (const { major_version, month, total_downloads } of monthlyVersionRows) {
      const key = String(major_version)
      if (!versionData[key]) {
        versionData[key] = {}
      }
      versionData[key][month] = total_downloads
    }

    // Create datasets array for Chart.js
    const versionDatasets = []
    const versionTotals = labels.map(() => 0)

    for (const version of Object.keys(versionData).sort((a, b) => Number(a) - Number(b))) {
      const data = labels.map((month, index) => {
        const downloads = versionData[version][month] || 0
        versionTotals[index] += downloads
        return downloads
      })

      versionDatasets.push({
        label: version,
        data,
        fill: true,
        showLine: true
      })
    }

    // Add 'All' dataset (sum of all versions)
    versionDatasets.push({
      label: 'All',
      data: versionTotals,
      fill: false,
      showLine: true
    })

    // Build OS chart datasets (stacked)
    const osData = {}
    for (const { os, month, total_downloads } of monthlyOsRows) {
      if (!osData[os]) {
        osData[os] = {}
      }
      osData[os][month] = total_downloads
    }

    const osDatasets = []
    for (const os of Object.keys(osData).sort()) {
      const data = labels.map((month) => osData[os][month] || 0)

      osDatasets.push({
        label: os,
        data,
        fill: true,
        showLine: true
      })
    }

    // Generate CSV data
    const csv = generateCSV(labels, versionData, osData)

    return {
      labels,
      versionChart: {
        datasets: versionDatasets
      },
      osChart: {
        datasets: osDatasets
      },
      csv
    }
  }

  /**
   * Generate CSV data from aggregated monthly data
   */
  function generateCSV (labels, versionData, osData) {
    const csv = []
    csv.push('Month,Version,Operating System,Downloads')

    // Add version data rows
    for (const version of Object.keys(versionData).sort((a, b) => Number(a) - Number(b))) {
      for (const month of labels) {
        const downloads = versionData[version][month] || 0
        if (downloads > 0) {
          csv.push(`${month},${version},,${downloads}`)
        }
      }
    }

    // Add OS data rows
    for (const os of Object.keys(osData).sort()) {
      for (const month of labels) {
        const downloads = osData[os][month] || 0
        if (downloads > 0) {
          csv.push(`${month},,${os},${downloads}`)
        }
      }
    }

    return csv.join('\n')
  }

  fastify.get('/chart-data', async (request, reply) => {
    // Check if data is available
    const versionRows = db.getMonthlyVersionDownloads()

    if (versionRows.length === 0) {
      reply.code(503)
      reply.header('Retry-After', '30')
      return {
        error: 'Data is still loading',
        message: 'Initial data load in progress - chart data not available yet'
      }
    }

    const chartData = computeChartData()

    // Cache headers - content is stable for 1 hour
    const oneHour = 60 * 60
    reply.header('Cache-Control', `max-age=${oneHour}, s-maxage=${oneHour}, stale-while-revalidate=${oneHour}, stale-if-error=${oneHour}`)

    return chartData
  })
}
