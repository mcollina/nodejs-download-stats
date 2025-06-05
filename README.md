# Node.js Download Statistics

A web application that tracks and visualizes Node.js download statistics by version and operating system. Built with Platformatic Service, this app fetches real-time data from Google Cloud Storage and presents it through interactive charts with CSV export functionality.

Powered by [Watt](https://platformatic.dev), the Node.js application server by Platformatic.

## Features

- **Real-time Data**: Fetches Node.js download statistics from Google Cloud Storage
- **Interactive Charts**: Visualizes downloads by version and operating system using Chart.js
- **CSV Export**: Download monthly statistics in CSV format
- **Smart Caching**: Two-layer caching system (in-memory + disk) for optimal performance
- **REST API**: OpenAPI documented endpoints for programmatic access
- **Responsive Design**: Clean, mobile-friendly interface

## Requirements

- Node.js ^22.16.0
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
3. Processes data by major Node.js version and operating system
4. Caches results using two-layer system (5s in-memory, 24h disk cache)
5. Serves processed data via REST API and web dashboard

### Key Components
- **Backend**: Platformatic Service with custom routes and plugins
- **Frontend**: Single-page application with Chart.js visualizations
- **Caching**: cacache for disk storage, async-cache-dedupe for memory
- **Data Processing**: Filters versions v4+, excludes current incomplete month

## Development

Run tests:
```bash
npm test
```

### Project Structure
```
├── routes/
│   └── metrics.js          # Main API endpoint with data processing
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