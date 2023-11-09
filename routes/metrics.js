/// <reference path="../global.d.ts" />
'use strict'

const undici = require('undici')
const saxophonist = require('saxophonist')
const { pipeline } = require('stream/promises')
const semver = require('semver')
const cacache = require('cacache')
const { join } = require('path')
const { createCache } = require('async-cache-dedupe')

const BASE_URL = 'https://storage.googleapis.com/access-logs-summaries-nodejs/'

/** @param {import('fastify').FastifyInstance} fastify */
module.exports = async function (fastify, opts) {
  const cache = createCache({
    ttl: 5, // seconds
    stale: 5, // number of seconds to return data after ttl has expired
    storage: { type: 'memory' },
  })

  cache.define('computeMetrics', async () => {
    const cacheKey = '/metrics'
    const indexInfo = await cacache.get.info(cachePath, cacheKey)
    if (indexInfo && indexInfo.time + 1000 * 60 * 60 * 24 < Date.now()) {
      return JSON.parse(await cacache.get(cachePath, cacheKey))
    }
    const response = await undici.request(BASE_URL)
    const parser = saxophonist('Key')
    const toDownload = []

    await pipeline(response.body, parser, async function * (stream) {
      for await (const chunk of stream) {
        // write a regexp that parses the date out of nodejs.org-access.log.20231107.json
        const match = chunk.text.toString().match(/nodejs\.org-access\.log\.(\d{4})(\d{2})(\d{2})\.json/)
        if (!match) continue
        const year = match[1]
        const month = match[2]
        const day = match[3]
        toDownload.push({
          date: `${year}-${month}-${day}`,
          url: `${BASE_URL}nodejs.org-access.log.${year}${month}${day}.json`
        })
      }
    })

    const versions = {}

    // Make those files cached on disk
    await Promise.all(toDownload.map(async ({ date, url }) => {
      const info = await cacache.get.hasContent(cachePath, url)
      let result
      if (info) {
        const res = await cacache.get(cachePath, url)
        result = JSON.parse(res.data.toString())
      } else {
        const response = await undici.request(url)
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
    }))

    for (const key in versions) {
      versions[key].sort((a, b) => a.date.localeCompare(b.date))
    }

    const res = { versions }

    await cacache.put(cachePath, cacheKey, JSON.stringify(res))

    return res
  })

  const cachePath = join(__dirname, '..', 'cache')
  fastify.get('/metrics', async (request, reply) => {
    const res = await cache.computeMetrics()
    // The content stays stable for 1 hour

    const onehour = 60 * 60
    reply.header('Cache-Control', `max-age=${onehour}, s-maxage=${onehour}, stale-while-revalidate=${onehour}, stale-if-error=${onehour}`)
    return res
  })
}
