'use strict'

const { join } = require('node:path')
const os = require('node:os')

const schema = {
  type: 'object',
  required: [],
  properties: {
    NODEJS_DOWNLOAD_STATS_DB: {
      type: 'string',
      default: join(os.tmpdir(), 'nodejs-download-stats.db'),
      description: 'Path to SQLite database file'
    }
  }
}

module.exports = { schema }
