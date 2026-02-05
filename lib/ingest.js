'use strict'

const undici = require('undici')
const semver = require('semver')
const { XMLParser } = require('fast-xml-parser')
const {
  initSchema,
  insertVersionDownload,
  insertOsDownload,
  setLastUpdate,
  getLastUpdate,
  clearData
} = require('./db')

const BASE_URL = 'https://storage.googleapis.com/access-logs-summaries-nodejs/'
const INGESTION_INTERVAL_MS = 1000 * 60 * 60 * 24 // 24 hours

class DataIngester {
  constructor (logger) {
    this.logger = logger || console
    this.agent = new undici.Agent({ connections: 10 })
    this.isIngesting = false
    initSchema()
  }

  async ingest () {
    if (this.isIngesting) {
      this.logger.debug('Ingestion already in progress, skipping')
      return
    }

    this.isIngesting = true
    this.logger.info('Starting data ingestion from GCS')

    try {
      // Check if we need to refresh data
      const lastUpdate = getLastUpdate()
      const now = Date.now()

      if (lastUpdate && (now - lastUpdate.updatedAt) < INGESTION_INTERVAL_MS) {
        this.logger.debug({ lastUpdate }, 'Data is fresh, skipping ingestion')
        this.isIngesting = false
        return
      }

      // Start fresh - clear old data
      clearData()

      // Fetch available files from GCS
      const availableData = await this.listAvailableFiles()

      // Skip current month as it doesn't have all the data yet
      const today = new Date()
      const currentMonth = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}`

      const toDownload = availableData.filter(({ date }) => !date.startsWith(currentMonth))

      this.logger.info({ count: toDownload.length }, 'Downloading daily stats files')

      // Download and process each file
      await Promise.all(toDownload.map(async ({ date, url }) => {
        try {
          const response = await undici.request(url, { dispatcher: this.agent })
          const result = await response.body.json()

          // Process version data
          const versionKeys = Object.keys(result.version || {})
          for (const key of versionKeys) {
            const version = semver.parse(key)
            if (!version) continue
            if (version.major < 4) continue

            insertVersionDownload(date, version.major, result.version[key])
          }

          // Process OS data
          const osKeys = Object.keys(result.os || {})
          for (const os of osKeys) {
            insertOsDownload(date, os, result.os[os])
          }

          this.logger.debug({ date }, 'Processed daily stats')
        } catch (err) {
          this.logger.warn({ err, date, url }, 'Failed to process daily stats')
        }
      }))

      // Update last update timestamp
      setLastUpdate(new Date().toISOString())

      this.logger.info('Data ingestion completed successfully')
    } catch (err) {
      this.logger.error({ err }, 'Data ingestion failed')
      throw err
    } finally {
      this.isIngesting = false
    }
  }

  async listAvailableFiles () {
    const availableData = []
    let nextMarker = ''

    do {
      let url = BASE_URL + '?max-keys=100'
      if (nextMarker) {
        url += '&marker=' + nextMarker
      }

      this.logger.debug({ url }, 'Fetching bucket listing')
      const response = await undici.request(url, { dispatcher: this.agent })
      const parser = new XMLParser({ isArray: (tagName) => tagName === 'Contents' })
      const obj = parser.parse(await response.body.text())

      for (const key of obj.ListBucketResult.Contents || []) {
        const match = key.Key.match(/nodejs\.org-access\.log\.(\d{4})(\d{2})(\d{2})\.json/)
        if (!match) continue

        const year = match[1]
        const month = match[2]
        const day = match[3]
        availableData.push({
          date: `${year}-${month}-${day}`,
          url: `${BASE_URL}nodejs.org-access.log.${year}${month}${day}.json`
        })
      }

      nextMarker = obj.ListBucketResult.NextMarker
    } while (nextMarker)

    // Sort by date
    availableData.sort((a, b) => a.date.localeCompare(b.date))

    return availableData
  }

  startPeriodicIngestion (intervalMs = INGESTION_INTERVAL_MS) {
    // Initial ingestion
    this.ingest().catch(err => this.logger.error({ err }, 'Initial ingestion failed'))

    // Schedule periodic ingestion
    const interval = setInterval(() => {
      this.ingest().catch(err => this.logger.error({ err }, 'Periodic ingestion failed'))
    }, intervalMs)

    return interval
  }
}

module.exports = { DataIngester }
