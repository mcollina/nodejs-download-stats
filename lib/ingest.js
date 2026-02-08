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
  getExistingDates,
  getMostRecentDate
} = require('./db')

const BASE_URL = 'https://storage.googleapis.com/access-logs-summaries-nodejs/'
const INGESTION_INTERVAL_MS = 1000 * 60 * 60 * 24 // 24 hours

class DataIngester {
  constructor (logger) {
    if (!logger) {
      throw new Error('Logger is required')
    }
    this.logger = logger
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

      // Get the most recent date in the database to resume efficiently
      const mostRecentDate = getMostRecentDate()
      if (mostRecentDate) {
        this.logger.info({ mostRecentDate }, 'Resuming ingestion from last known date')
      }

      // Fetch available files from GCS (only new data since mostRecentDate)
      const availableData = await this.listAvailableFiles(mostRecentDate)

      // Skip current month as it doesn't have all the data yet
      const today = new Date()
      const currentMonth = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}`

      // Get dates already in the database to avoid re-ingesting
      const existingDates = new Set(getExistingDates())
      this.logger.info({ count: existingDates.size }, 'Found existing dates in database')

      // Filter out:
      // 1. Dates already in the database (from previous runs/restarts)
      // 2. Current incomplete month
      const toDownload = availableData.filter(({ date }) => {
        if (existingDates.has(date)) {
          return false
        }
        if (date.startsWith(currentMonth)) {
          return false
        }
        return true
      })

      this.logger.info({ count: toDownload.length }, 'Downloading daily stats files')

      // Process downloads sequentially to avoid blocking event loop
      // SQLite is synchronous, so we need to yield control between files
      let processed = 0
      for (const { date, url } of toDownload) {
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

          processed++
          if (processed % 10 === 0) {
            this.logger.info({ processed, total: toDownload.length }, 'Ingestion progress')
          } else {
            this.logger.debug({ date }, 'Processed daily stats')
          }

        } catch (err) {
          this.logger.warn({ err, date, url }, 'Failed to process daily stats')
        }
      }

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

  async listAvailableFiles (sinceDate = null) {
    const availableData = []
    let nextMarker = ''

    // If we have a sinceDate, we can start from that point instead of fetching everything
    // GCS listing is sorted lexicographically by key, and our keys contain dates
    // nodejs.org-access.log.YYYYMMDD.json sorts correctly by date
    const prefix = 'nodejs.org-access.log.'

    // Calculate a marker if we know the last date we have
    // This avoids fetching everything from the beginning every time
    if (sinceDate) {
      const [year, month, day] = sinceDate.split('-')
      // Start listing from next day after our most recent data
      nextMarker = prefix + `${year}${month}${day}.json`
      this.logger.debug({ sinceDate, marker: nextMarker }, 'Resuming listing from marker')
    }

    do {
      // max-keys=1000 is the max allowed, reduces pagination
      let url = BASE_URL + '?max-keys=1000'
      if (nextMarker) {
        url += '&marker=' + encodeURIComponent(nextMarker)
      }

      this.logger.debug({ url }, 'Fetching bucket listing')
      const response = await undici.request(url, { dispatcher: this.agent })
      const parser = new XMLParser({ isArray: (tagName) => tagName === 'Contents' })
      const obj = parser.parse(await response.body.text())

      const contents = (obj.ListBucketResult && obj.ListBucketResult.Contents) || []

      for (const key of contents) {
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

      nextMarker = obj.ListBucketResult && obj.ListBucketResult.NextMarker

      // Safety break: if we hit 21 and have no new data, stop
      // The current incomplete month is handled by the caller anyway
      if (nextMarker && contents.length > 0) {
        // Check if we've reached data that is too recent (current month + 1)
        const lastContent = contents[contents.length - 1]
        if (lastContent && lastContent.Key) {
          const match = lastContent.Key.match(/nodejs\.org-access\.log\.(\d{4})(\d{2})(\d{2})\.json/)
          if (match) {
            const [year, month] = [match[1], match[2]]
            const fileMonth = `${year}-${month}`
            const today = new Date()
            const currentMonth = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}`
            // If we're in the current month or beyond, we can stop
            if (fileMonth >= currentMonth) {
              this.logger.debug({ fileMonth }, 'Reached current month, stopping listing')
              break
            }
          }
        }
      }
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

  async ingestWithProgress (onProgress) {
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

      // Get the most recent date in the database
      const mostRecentDate = getMostRecentDate()
      if (mostRecentDate) {
        this.logger.info({ mostRecentDate }, 'Resuming ingestion from last known date')
      }

      // Fetch available files from GCS
      const availableData = await this.listAvailableFiles(mostRecentDate)

      // Skip current month
      const today = new Date()
      const currentMonth = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}`

      // Get existing dates
      const existingDates = new Set(getExistingDates())
      this.logger.info({ count: existingDates.size }, 'Found existing dates in database')

      // Filter out existing and current month
      const toDownload = availableData.filter(({ date }) => {
        if (existingDates.has(date)) return false
        if (date.startsWith(currentMonth)) return false
        return true
      })

      this.logger.info({ count: toDownload.length }, 'Downloading daily stats files')

      if (onProgress) {
        onProgress({ processed: 0, total: toDownload.length })
      }

      // Process downloads sequentially with yields
      let processed = 0
      for (const { date, url } of toDownload) {
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

          processed++
          if (onProgress && processed % 5 === 0) {
            onProgress({ processed, total: toDownload.length })
          }
        } catch (err) {
          this.logger.warn({ err, date, url }, 'Failed to process daily stats')
        }
      }

      if (onProgress) {
        onProgress({ processed, total: toDownload.length })
      }

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
}

module.exports = { DataIngester }
