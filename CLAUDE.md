# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Node.js download statistics application built with Platformatic Service. It fetches and caches Node.js download data from Google Cloud Storage, processes the data by version and operating system, and serves it through a web API with a frontend dashboard showing charts.

## Key Commands

- `npm start` - Start the Platformatic service (runs on port from ENV or 3042)
- `npm test` - Run tests using Node.js built-in test runner
- `npm install` - Install dependencies

## Architecture

### Core Components

- **Platformatic Service**: Framework providing the HTTP server, OpenAPI documentation, and plugin system
- **Routes**: `/metrics` endpoint in `routes/metrics.js` that fetches, caches, and serves download statistics
- **Plugins**: Static file serving in `plugins/static.js` for the frontend
- **Frontend**: Single-page application in `public/` with Chart.js visualizations

### Data Flow

1. `/metrics` endpoint fetches XML data from Google Cloud Storage bucket listing
2. Downloads daily JSON files containing Node.js download statistics  
3. Processes data by major version (v4+) and operating system
4. Uses two-layer caching: in-memory (5s TTL) + disk cache (24h TTL) via cacache
5. Frontend fetches processed data and renders charts using Chart.js

### Key Files

- `routes/metrics.js` - Main data processing logic with caching
- `platformatic.service.json` - Service configuration
- `public/index.html` - Frontend dashboard
- `public/count.js` - Chart rendering logic
- `test/helper.js` - Test utilities for Platformatic service setup

### Dependencies

- **@platformatic/service**: Web service framework
- **cacache**: Disk-based caching
- **async-cache-dedupe**: In-memory caching with deduplication
- **undici**: HTTP client for fetching data
- **fast-xml-parser**: XML parsing for Google Cloud Storage responses
- **semver**: Version parsing and filtering
- **Chart.js**: Frontend charting (CDN)

## Development Notes

- Cache directory: `os.tmpdir()/downloads-cache`
- Data source: `https://storage.googleapis.com/access-logs-summaries-nodejs/`
- Supports Node.js ^18.8.0 || >=20.6.0
- OpenAPI documentation available at `/documentation/`
- Current month data is excluded as incomplete