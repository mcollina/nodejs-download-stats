let downloadsChart
let byOperatingSystems

function getBorderColor () {
  return getComputedStyle(document.body).getPropertyValue('--color-bg-secondary')
}

function getTextColor () {
  return getComputedStyle(document.body).getPropertyValue('--color-text')
}

function downloadCSV(csv) {
  const blob = new Blob([csv], { type: 'text/csv' })
  const url = window.URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'nodejs-downloads.csv'
  a.click()
  window.URL.revokeObjectURL(url)
}

async function updateChart () {
  const response = await fetch('/chart-data')

  const json = await response.json()

  // Update ingestion status in footer
  const statusEl = document.getElementById('ingestion-status')
  if (statusEl && json.ingestionStatus) {
    statusEl.textContent = json.ingestionStatus.message
  }

  if (json.error) {
    console.error('Error fetching chart data:', json.error)
    return
  }

  // Hide skeleton loading elements
  document.querySelectorAll('.skeleton').forEach((el) => el.classList.add('hidden'))

  const { labels, versionChart, osChart, csv } = json

  const borderColor = getBorderColor()
  const textColor = getTextColor()

  // Configuration options
  const options = {
    responsive: true,
    aspectRatio: 1.5,
    maintainAspectRatio: true,
    resizeDelay: 200,
    color: textColor,
    scales: {
      x: {
        grid: {
          color: borderColor,
        },
        ticks: {
          color: textColor,
        },
      },
      y: {
        grid: {
          color: borderColor,
        },
        ticks: {
          color: textColor,
        },
      },
    },
  }

  // Create the version chart
  const ctx = document.getElementById('downloadsChart').getContext('2d')
  downloadsChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: versionChart.datasets
    },
    options: options
  })

  // Create the OS chart (stacked)
  const ctx2 = document.getElementById('byOperatingSystem').getContext('2d')
  byOperatingSystems = new Chart(ctx2, {
    type: 'line',
    data: {
      labels,
      datasets: osChart.datasets
    },
    options: {
      ...options,
      scales: {
        ...options.scales,
        y: {
          ...options.scales.y,
          stacked: true
        }
      }
    }
  })

  // Setup CSV download
  document.getElementById('csvDownload').addEventListener('click', (e) => {
    e.preventDefault()
    downloadCSV(csv)
  })
}

window.matchMedia('(prefers-color-scheme: dark)')
  .addEventListener('change', event => {
    const borderColor = getBorderColor()
    const textColor = getTextColor()

    for (const chart of [downloadsChart, byOperatingSystems]) {
      if (chart) {
        chart.options.color = textColor
        chart.options.scales.x.grid.color = borderColor
        chart.options.scales.y.grid.color = borderColor
        chart.options.scales.x.ticks.color = textColor
        chart.options.scales.y.ticks.color = textColor
        chart.update()
      }
    }
  })
