/// <reference path="../global.d.ts" />
'use strict'

const os = require('node:os')
const { join } = require('node:path')
const undici = require('undici')
const semver = require('semver')
const cacache = require('cacache')
const { createCache } = require('async-cache-dedupe')
const { XMLParser } = require('fast-xml-parser')


const BASE_URL = 'https://storage.googleapis.com/access-logs-summaries-nodejs/'
const CACHE_KEY_FORMAT = 1

/** @param {import('fastify').FastifyInstance} fastify */
module.exports = async function (fastify, opts) {
  const cache = createCache({
    ttl: 5, // seconds
    stale: 5, // number of seconds to return data after ttl has expired
    storage: { type: 'memory' },
  })

  const agent = new undici.Agent({
    connections: 10
  })

  const cachePath = join(os.tmpdir(), 'downloads-cache')

  cache.define('computeMetrics', async () => {
    const cacheKey = '/metrics#' + CACHE_KEY_FORMAT
    const indexInfo = await cacache.get.info(cachePath, cacheKey)
    if (indexInfo && indexInfo.time + 1000 * 60 * 60 * 24 < Date.now()) {
      try {
        const blob = await cacache.get(cachePath, cacheKey)
        return JSON.parse(blob)
      } catch (err) {
        await cacache.rm.entry(cachePath, cacheKey)
        fastify.log.warn({ err }, 'unable to retrieve cache for main data')
        // ignore
      }
    }

    const availableData = []
    let nextMarker = ''
    do {
      let url = BASE_URL + '?max-keys=100'
      if (nextMarker) {
        url += '&marker=' + nextMarker
      }
      fastify.log.debug({ url }, 'fetching data')
      const response = await undici.request(url, {
        dispatcher: agent
      })
      const parser = new XMLParser({
        isArray: (tagName) => tagName === "Contents",
      })
      const obj = parser.parse(await response.body.text())

      for (const key of obj.ListBucketResult.Contents) {
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

    // Sort available data by date
    availableData.sort((a, b) => {
      const [yearA, monthA, dateA] = a.date.split('-')
      const [yearB, monthB, dateB] = b.date.split('-')
      return yearA - yearB || monthA - monthB || dateA - dateB
    })

    const monthsToSkip = [] // this month has botched data

    // Skip current month as it doesn't have all the data yet
    const today = new Date()
    monthsToSkip.push(String(today.getFullYear()) + '-' + String(today.getMonth() + 1).padStart(2, '0'))

    const toDownload = availableData.filter(({ date }) => !monthsToSkip.includes(date.slice(0, -3)))

    const versions = {}
    const operatingSystems = {}

    // Make those files cached on disk
    await Promise.all(toDownload.map(async ({ date, url }) => {
      const info = await cacache.get.hasContent(cachePath, url)
      let result
      if (info) {
        const res = await cacache.get(cachePath, url)
        result = JSON.parse(res.data.toString())
      } else {
        const response = await undici.request(url, {
          dispatcher: agent
        })
        result = await response.body.json()
        await cacache.put(cachePath, url, JSON.stringify(result))
      }
      const keys = Object.keys(result.version)
      for (const key of keys) {
        const version = semver.parse(key)
        if (!version) continue
        if (version.major < 4) continue
        versions[version.major] ||= []
        versions[version.major].push({
          date,
          downloads: result.version[key]
        })
      }

      const oses = Object.keys(result.os)
      for (const os of oses) {
        operatingSystems[os] ||= []
        operatingSystems[os].push({
          date,
          downloads: result.os[os]
        })
      }
    }))

    for (const key in versions) {
      versions[key].sort((a, b) => a.date.localeCompare(b.date))
    }

    for (const key in operatingSystems) {
      operatingSystems[key].sort((a, b) => a.date.localeCompare(b.date))
    }

    const res = { versions, operatingSystems }

    await cacache.put(cachePath, cacheKey, JSON.stringify(res))

    return res
  })

  fastify.get('/metrics', async (request, reply) => {
    const res = await cache.computeMetrics()
    // The content stays stable for 1 hour

    const onehour = 60 * 60
    reply.header('Cache-Control', `max-age=${onehour}, s-maxage=${onehour}, stale-while-revalidate=${onehour}, stale-if-error=${onehour}`)
    return res
  })
}
