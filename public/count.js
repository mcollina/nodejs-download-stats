let downloadsChart
let byOperatingSystems

function getBorderColor () {
  return getComputedStyle(document.body).getPropertyValue('--color-bg-secondary')
}

function getTextColor () {
  return getComputedStyle(document.body).getPropertyValue('--color-text')
}

function computeDataSet (labels, versions, includeAll = true) {
  const all = labels.map((label) => 0)

  const datasets = Object.keys(versions).map((version) => {
    const downloadsCounts = {}

    for (const { date, downloads } of versions[version]) {
      const month = date.substring(0, 7) // Extract YYYY-MM from YYYY-MM-DD
      downloadsCounts[month] = (downloadsCounts[month] || 0) + downloads
    }

    const data = labels.map((label) => downloadsCounts[label] || 0)

    for (let i = 0; i < data.length; i++) {
      all[i] += data[i]
    }

    return {
      label: version,
      data,
      fill: true,
      showLine: true
    }
  })

  if (includeAll) {
    datasets.push({
      label: 'All',
      data: all,
      fill: false,
      showLine: true
    })
  }

  return datasets
}

function generateCSV(json) {
  const csv = []
  csv.push('Month,Version,Operating System,Downloads')
  
  // Aggregate version data by month
  Object.keys(json.versions).forEach(version => {
    const monthlyData = {}
    json.versions[version].forEach(({ date, downloads }) => {
      const month = date.substring(0, 7) // Extract YYYY-MM
      monthlyData[month] = (monthlyData[month] || 0) + downloads
    })
    
    Object.keys(monthlyData).forEach(month => {
      csv.push(`${month},${version},,${monthlyData[month]}`)
    })
  })
  
  // Aggregate operating system data by month
  Object.keys(json.operatingSystems).forEach(os => {
    const monthlyData = {}
    json.operatingSystems[os].forEach(({ date, downloads }) => {
      const month = date.substring(0, 7) // Extract YYYY-MM
      monthlyData[month] = (monthlyData[month] || 0) + downloads
    })
    
    Object.keys(monthlyData).forEach(month => {
      csv.push(`${month},,${os},${monthlyData[month]}`)
    })
  })
  
  return csv.join('\n')
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
  const response = await fetch('/metrics')

  const json = await response.json()

  if (json.error) {
    console.error('Error fetching metrics:', json.error)
    return;
  } else {
    document.querySelectorAll('.skeleton').forEach((el) => el.classList.add('hidden'))
  }

  const versions = json.versions

  // Get all unique months from both versions and operating systems
  const allMonths = new Set()
  
  // Add months from version data
  for (const versionData of Object.values(versions)) {
    for (const { date } of versionData) {
      allMonths.add(date.substring(0, 7)) // Extract YYYY-MM
    }
  }
  
  // Add months from OS data
  if (json.operatingSystems) {
    for (const osData of Object.values(json.operatingSystems)) {
      for (const { date } of osData) {
        allMonths.add(date.substring(0, 7)) // Extract YYYY-MM
      }
    }
  }
  
  const labels = [...allMonths].sort()

  // Your chart data
  var data = {
    labels,
    datasets: computeDataSet(labels, versions)
  };

  // Your chart data
  var data2 = {
    labels,
    datasets: computeDataSet(labels, json.operatingSystems, false)
  };

  var ctx = document.getElementById('downloadsChart').getContext('2d');
  var ctx2 = document.getElementById('byOperatingSystem').getContext('2d');

  var borderColor = getBorderColor();
  var textColor = getTextColor();

  // Configuration options
  var options = {
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
  };


  // Create the line chart
  downloadsChart = new Chart(ctx, {
    type: 'line',
    data: data,
    options: options
  });

  // Create the line chart
  byOperatingSystems = new Chart(ctx2, {
    type: 'line',
    data: data2,
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
  });
  
  // Setup CSV download
  document.getElementById('csvDownload').addEventListener('click', (e) => {
    e.preventDefault()
    const csv = generateCSV(json)
    downloadCSV(csv)
  })
}

window.matchMedia('(prefers-color-scheme: dark)')
  .addEventListener('change', event => {
    var borderColor = getBorderColor();
    var textColor = getTextColor();

    for (const chart of [downloadsChart, byOperatingSystems]) {
      chart.options.color = textColor
      chart.options.scales.x.grid.color = borderColor
      chart.options.scales.y.grid.color = borderColor
      chart.options.scales.x.ticks.color = textColor
      chart.options.scales.y.ticks.color = textColor
      chart.update()
    }
  })
