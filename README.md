# Node.js Download Statistics

A web application that tracks and visualizes Node.js download statistics by version and operating system. Built with Platformatic Service, this app fetches real-time data from Google Cloud Storage and presents it through interactive charts with CSV export functionality.

Powered by [Watt](https://platformatic.dev), the Node.js application server by Platformatic.

## Features

- **Real-time Data**: Fetches Node.js download statistics from Google Cloud Storage
- **Interactive Charts**: Visualizes downloads by version and operating system using Chart.js
- **CSV Export**: Download monthly statistics in CSV format
- **SQLite Storage**: Pre-processed data stored in SQLite for fast queries
- **REST API**: OpenAPI documented endpoints for programmatic access
- **Responsive Design**: Clean, mobile-friendly interface

## Requirements

- Node.js ^22.16.0 (for `node:sqlite` built-in module support)
- macOS, Linux, or Windows (WSL recommended)

## Installation

```bash
npm install
```

## Usage

Start the application:

```bash
npm start
```

The application will be available at:
- **Dashboard**: http://localhost:3042/
- **API Documentation**: http://localhost:3042/documentation/
- **Metrics Endpoint**: http://localhost:3042/metrics

## API Endpoints

### GET /metrics

Returns Node.js download statistics processed by major version (v4+) and operating system.

**Response Format:**
```json
{
  "byVersion": {
    "v18": { "2024-01": 12345678, "2024-02": 23456789 }
  },
  "byOs": {
    "linux": { "2024-01": 45678901, "2024-02": 56789012 }
  }
}
```

## Architecture

### Data Flow
1. Fetches XML bucket listing from Google Cloud Storage
2. Downloads daily JSON files containing download statistics
3. Processes and stores data in SQLite database (by version and OS)
4. Automatically refreshes data every 24 hours
5. Serves pre-aggregated data via REST API and web dashboard

### Database Schema

The SQLite database stores:

**version_downloads table:**
- `date` (TEXT): The date of the stats (YYYY-MM-DD)
- `major_version` (INTEGER): Node.js major version number
- `downloads` (INTEGER): Download count for that version on that date

**os_downloads table:**
- `date` (TEXT): The date of the stats (YYYY-MM-DD)
- `os` (TEXT): Operating system name (linux, win32, darwin, aix, sunos)
- `downloads` (INTEGER): Download count for that OS on that date

### Key Components
- **Backend**: Platformatic Service with custom routes and plugins
- **Database**: SQLite using Node.js built-in `node:sqlite` module
- **Frontend**: Single-page application with Chart.js visualizations
- **Data Processing**: Filters versions v4+, excludes current incomplete month

## Development

Run tests:
```bash
npm test
```

### Environment Variables

- `NODEJS_DOWNLOAD_STATS_DB`: Path to SQLite database file (default: temp directory)

### Project Structure
```
├── lib/
│   ├── db.js               # SQLite database module
│   └── ingest.js           # Data ingestion service
├── routes/
│   └── metrics.js          # API endpoint with data queries
├── plugins/
│   └── static.js           # Static file serving
├── public/
│   ├── index.html          # Frontend dashboard
│   ├── count.js            # Chart rendering logic
│   └── mvp.css             # Styling
└── test/                   # Test suite
```

## Data Source

Statistics are sourced from Google Cloud Storage bucket: `access-logs-summaries-nodejs`

The application processes daily download summaries and aggregates them by:
- Major Node.js versions (v4, v6, v8, v10, v12, v14, v16, v18, v20, v22+)
- Operating systems (linux, win32, darwin, aix, sunos)

## License

See [LICENSE](LICENSE) file for details.
