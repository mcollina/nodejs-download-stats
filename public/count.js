 
async function updateChart () {
  const response = await fetch('/metrics')
  const json = await response.json()

  const versions = json.versions

  const labels = [...new Set(versions['4'].map(({ date }) => date.replace(/-\d{2}$/g, '')))]

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
      fill: false,
      showLine: true
    }
  })

  // Your chart data
  var data = {
    labels,
    datasets: [
      ...datasets,
      {
        label: 'All',
        data: all,
        fill: false,
        showLine: true
      }
    ]
  };

  var ctx = document.getElementById('myLineChart').getContext('2d');

  // Configuration options
  var options = {
    responsive: false,
    maintainAspectRatio: false
  };

  // Create the line chart
  myLineChart = new Chart(ctx, {
    type: 'line',
    data: data,
    options: options
  });
}
