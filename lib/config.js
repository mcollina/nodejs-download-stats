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
    },
    PLT_SERVER_HOSTNAME: {
      type: 'string',
      default: '0.0.0.0'
    },
    PORT: {
      type: 'string',
      default: '3000'
    },
    PLT_SERVER_LOGGER_LEVEL: {
      type: 'string',
      default: 'info',
      enum: ['fatal', 'error', 'warn', 'info', 'debug', 'trace']
    }
  }
}

module.exports = { schema }
