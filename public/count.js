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
    versions[version].map(({ date, downloads }) => downloads)

    const downloadsCounts = {}

    for (const { date, downloads } of versions[version]) {
      const toCheck = date.replace(/-\d{2}$/g, '')
      downloadsCounts[toCheck] ||= 0
      downloadsCounts[toCheck] += downloads
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

async function updateChart () {
  const response = await fetch('/metrics')

  const json = await response.json()

  if (json.error) {
    return
  } else {
    document.querySelectorAll('.skeleton').forEach((el) => el.classList.add('hidden')
    )
  }

  const versions = json.versions

  const labels = [...new Set(versions['4'].map(({ date }) => date.replace(/-\d{2}$/g, '')))]

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
    aspectRatio: 1.2,
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
