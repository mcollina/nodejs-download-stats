'use strict'

const undici = require('undici')
const semver = require('semver')
const { XMLParser } = require('fast-xml-parser')

const BASE_URL = 'https://storage.googleapis.com/access-logs-summaries-nodejs/'
const INGESTION_INTERVAL_MS = 1000 * 60 * 60 * 24 // 24 hours

class DataIngester {
  constructor (logger, db, agent) {
    if (!logger) {
      throw new Error('Logger is required')
    }
    if (!db) {
      throw new Error('Database is required')
    }
    this.logger = logger
    this.db = db
    // Allow passing custom agent for testing (e.g., MockAgent)
    this.agent = agent || new undici.Agent({ connections: 10 })
    this.isIngesting = false
    this.lastError = null
    this.stats = {
      totalFiles: 0,
      processedFiles: 0,
      versionRecords: 0,
      osRecords: 0,
      errors: 0,
      byVersion: {}
    }
  }

  async ingest () {
    if (this.isIngesting) {
      this.logger.debug('Ingestion already in progress, skipping')
      return
    }

    this.isIngesting = true
    this.lastError = null
    this.stats = {
      totalFiles: 0,
      processedFiles: 0,
      versionRecords: 0,
      osRecords: 0,
      errors: 0,
      byVersion: {}
    }
    this.logger.info('Starting data ingestion from GCS')

    try {
      // Check if we need to refresh data
      const lastUpdate = this.db.getLastUpdate()
      const now = Date.now()

      if (lastUpdate && (now - lastUpdate.updatedAt) < INGESTION_INTERVAL_MS) {
        this.logger.debug({ lastUpdate }, 'Data is fresh, skipping ingestion')
        this.isIngesting = false
        return
      }

      // Get the most recent date in the database to resume efficiently
      const mostRecentDate = this.db.getMostRecentDate()
      if (mostRecentDate) {
        this.logger.info({ mostRecentDate }, 'Resuming ingestion from last known date')
      }

      // Fetch available files from GCS (only new data since mostRecentDate)
      const availableData = await this.listAvailableFiles(mostRecentDate)
      this.stats.totalFiles = availableData.length
      this.logger.info({ totalFiles: availableData.length }, 'Found files to process')

      // Skip current month as it doesn't have all the data yet
      const today = new Date()
      const currentMonth = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}`

      // Get dates already in the database to avoid re-ingesting
      const existingDates = new Set(this.db.getExistingDates())
      this.logger.info({ existingDates: existingDates.size }, 'Found existing dates in database')

      // Filter out:
      // 1. Dates already in the database (from previous runs/restarts)
      // 2. Current incomplete month
      const toDownload = availableData.filter(({ date }) => {
        if (existingDates.has(date)) {
          this.logger.debug({ date }, 'Skipping - already in database')
          return false
        }
        if (date.startsWith(currentMonth)) {
          this.logger.debug({ date, currentMonth }, 'Skipping - current month')
          return false
        }
        return true
      })

      this.logger.info({ count: toDownload.length }, 'Downloading daily stats files')

      // Accumulate data for batch inserts - much faster than individual inserts
      // and reduces event loop blocking
      let processed = 0
      const versionBatch = []
      const osBatch = []
      const BATCH_SIZE = 500 // Flush batch after this many entries

      for (const { date, url } of toDownload) {
        try {
          this.logger.debug({ date, url }, 'Fetching daily stats file')
          const response = await undici.request(url, { dispatcher: this.agent })
          const result = await response.body.json()

          // Debug: log all version keys found
          const versionKeys = Object.keys(result.version || {})
          this.logger.debug({ 
            date, 
            versionCount: versionKeys.length,
            sampleVersions: versionKeys.slice(0, 5)
          }, 'Found versions in file')
          
          let versionsInserted = 0
          for (const key of versionKeys) {
            this.logger.trace({ key, date }, 'Parsing version')
            const version = semver.parse(key)
            if (!version) {
              this.logger.warn({ key, date }, 'Failed to parse version - skipping')
              continue
            }
            if (version.major < 4) {
              this.logger.trace({ key, major: version.major, date }, 'Skipping version < 4')
              continue
            }

            this.logger.trace({ date, version: key, major: version.major, downloads: result.version[key] }, 'Adding to version batch')
            versionBatch.push({
              date,
              majorVersion: version.major,
              downloads: result.version[key]
            })
            versionsInserted++
            this.stats.versionRecords++
            
            // Track by version for debugging
            if (!this.stats.byVersion[version.major]) {
              this.stats.byVersion[version.major] = 0
            }
            this.stats.byVersion[version.major]++

            // Flush batch when it reaches threshold
            if (versionBatch.length >= BATCH_SIZE) {
              this.logger.debug({ batchSize: versionBatch.length }, 'Flushing version batch')
              this.db.insertVersionDownloadsBatch(versionBatch.splice(0, BATCH_SIZE))
            }
          }
          this.logger.debug({ date, versionsInserted }, 'Added versions for date')

          // Accumulate OS data
          const osKeys = Object.keys(result.os || {})
          let osInserted = 0
          for (const os of osKeys) {
            this.logger.trace({ date, os, downloads: result.os[os] }, 'Adding to OS batch')
            osBatch.push({
              date,
              os,
              downloads: result.os[os]
            })
            osInserted++
            this.stats.osRecords++

            // Flush batch when it reaches threshold
            if (osBatch.length >= BATCH_SIZE) {
              this.logger.debug({ batchSize: osBatch.length }, 'Flushing OS batch')
              this.db.insertOsDownloadsBatch(osBatch.splice(0, BATCH_SIZE))
            }
          }
          this.logger.debug({ date, osInserted }, 'Added OS records for date')

          processed++
          this.stats.processedFiles = processed
          if (processed % 10 === 0) {
            this.logger.info({ 
              processed, 
              total: toDownload.length,
              versionRecords: this.stats.versionRecords,
              osRecords: this.stats.osRecords,
              versionQueue: versionBatch.length,
              osQueue: osBatch.length,
              byVersion: this.stats.byVersion
            }, 'Ingestion progress')
            await new Promise(resolve => setImmediate(resolve))
          } else {
            this.logger.debug({ date, versions: versionKeys.length }, 'Processed daily stats')
          }
        } catch (err) {
          this.stats.errors++
          this.lastError = err
          this.logger.error({ err, date, url }, 'Failed to process daily stats')
        }
      }

      // Flush any remaining batches
      if (versionBatch.length > 0) {
        this.logger.debug({ batchSize: versionBatch.length }, 'Flushing final version batch')
        this.db.insertVersionDownloadsBatch(versionBatch)
      }
      if (osBatch.length > 0) {
        this.logger.debug({ batchSize: osBatch.length }, 'Flushing final OS batch')
        this.db.insertOsDownloadsBatch(osBatch)
      }

      // Update last update timestamp
      this.db.setLastUpdate(new Date().toISOString())

      this.logger.info({
        stats: this.stats
      }, 'Data ingestion completed successfully')
    } catch (err) {
      this.lastError = err
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
      this.logger.debug({ pageSize: contents.length }, 'Got bucket listing page')

      for (const key of contents) {
        const match = key.Key.match(/nodejs\.org-access\.log\.(\d{4})(\d{2})(\d{2})\.json/)
        if (!match) {
          this.logger.trace({ key: key.Key }, 'Skipping non-matching key')
          continue
        }

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
    this.logger.info({ 
      totalFiles: availableData.length,
      dateRange: availableData.length > 0 ? `${availableData[0].date} to ${availableData[availableData.length - 1].date}` : 'none'
    }, 'Listed available files')

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
    this.lastError = null
    this.stats = {
      totalFiles: 0,
      processedFiles: 0,
      versionRecords: 0,
      osRecords: 0,
      errors: 0,
      byVersion: {}
    }
    this.logger.info('Starting data ingestion from GCS')

    try {
      // Check if we need to refresh data
      const lastUpdate = this.db.getLastUpdate()
      const now = Date.now()

      if (lastUpdate && (now - lastUpdate.updatedAt) < INGESTION_INTERVAL_MS) {
        this.logger.debug({ lastUpdate }, 'Data is fresh, skipping ingestion')
        this.isIngesting = false
        return
      }

      // Get the most recent date in the database
      const mostRecentDate = this.db.getMostRecentDate()
      if (mostRecentDate) {
        this.logger.info({ mostRecentDate }, 'Resuming ingestion from last known date')
      }

      // Fetch available files from GCS
      const availableData = await this.listAvailableFiles(mostRecentDate)
      this.stats.totalFiles = availableData.length

      // Skip current month
      const today = new Date()
      const currentMonth = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}`

      // Get existing dates
      const existingDates = new Set(this.db.getExistingDates())
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

      // Process downloads sequentially with batches
      let processed = 0
      const versionBatch = []
      const osBatch = []
      const BATCH_SIZE = 500

      for (const { date, url } of toDownload) {
        try {
          this.logger.debug({ date, url }, 'Fetching daily stats file')
          const response = await undici.request(url, { dispatcher: this.agent })
          const result = await response.body.json()

          // Debug: log all version keys found
          const versionKeys = Object.keys(result.version || {})
          this.logger.debug({ 
            date, 
            versionCount: versionKeys.length,
            sampleVersions: versionKeys.slice(0, 5)
          }, 'Found versions in file')
          
          let versionsInserted = 0
          for (const key of versionKeys) {
            this.logger.trace({ key, date }, 'Parsing version')
            const version = semver.parse(key)
            if (!version) {
              this.logger.warn({ key, date }, 'Failed to parse version - skipping')
              continue
            }
            if (version.major < 4) {
              this.logger.trace({ key, major: version.major, date }, 'Skipping version < 4')
              continue
            }

            this.logger.trace({ date, version: key, major: version.major }, 'Adding to version batch')
            versionBatch.push({
              date,
              majorVersion: version.major,
              downloads: result.version[key]
            })
            versionsInserted++
            this.stats.versionRecords++
            
            // Track by version for debugging
            if (!this.stats.byVersion[version.major]) {
              this.stats.byVersion[version.major] = 0
            }
            this.stats.byVersion[version.major]++

            // Flush batch when it reaches threshold
            if (versionBatch.length >= BATCH_SIZE) {
              this.db.insertVersionDownloadsBatch(versionBatch.splice(0, BATCH_SIZE))
            }
          }
          this.logger.debug({ date, versionsInserted }, 'Added versions for date')

          // Process OS data
          const osKeys = Object.keys(result.os || {})
          let osInserted = 0
          for (const os of osKeys) {
            this.logger.trace({ date, os }, 'Adding to OS batch')
            osBatch.push({
              date,
              os,
              downloads: result.os[os]
            })
            osInserted++
            this.stats.osRecords++

            // Flush batch when it reaches threshold
            if (osBatch.length >= BATCH_SIZE) {
              this.db.insertOsDownloadsBatch(osBatch.splice(0, BATCH_SIZE))
            }
          }
          this.logger.debug({ date, osInserted }, 'Added OS records for date')

          processed++
          this.stats.processedFiles = processed
          if (onProgress && processed % 5 === 0) {
            onProgress({ processed, total: toDownload.length })
          }

          // Yield to event loop every 10 files to prevent blocking
          if (processed % 10 === 0) {
            await new Promise(resolve => setImmediate(resolve))
          }
        } catch (err) {
          this.stats.errors++
          this.lastError = err
          this.logger.error({ err, date, url }, 'Failed to process daily stats')
        }
      }

      // Flush remaining batches
      if (versionBatch.length > 0) {
        this.db.insertVersionDownloadsBatch(versionBatch)
      }
      if (osBatch.length > 0) {
        this.db.insertOsDownloadsBatch(osBatch)
      }

      if (onProgress) {
        onProgress({ processed, total: toDownload.length })
      }

      // Update last update timestamp
      this.db.setLastUpdate(new Date().toISOString())

      this.logger.info({
        stats: this.stats
      }, 'Data ingestion completed successfully')
    } catch (err) {
      this.lastError = err
      this.logger.error({ err }, 'Data ingestion failed')
      throw err
    } finally {
      this.isIngesting = false
    }
  }

  getStats () {
    return {
      isIngesting: this.isIngesting,
      lastError: this.lastError ? this.lastError.message : null,
      stats: this.stats
    }
  }

  reset () {
    this.logger.info('Resetting ingestion state')
    this.isIngesting = false
    this.lastError = null
    this.stats = {
      totalFiles: 0,
      processedFiles: 0,
      versionRecords: 0,
      osRecords: 0,
      errors: 0,
      byVersion: {}
    }
  }
}

module.exports = { DataIngester }
