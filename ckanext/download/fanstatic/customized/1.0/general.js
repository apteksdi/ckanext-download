/*
* desc : make table sortable
*/
function sortDwnTable() {
  if(window.jQuery && $("#resDwnSum").length > 0) {
    $("#resDwnSum").tablesorter();
  }
}

/*
* desc : plot trend chart
*/
var downloadTrendChartData = [];
var downloadTrendChartLayout= {
        title: "",
        margin: { l: 100, r: 20, b: 30, t: 20, pad: 4 },
        showlegend: false,
        xaxis: { showgrid: false, zeroline: false, autotick: false },
        yaxis: { showline: false }
};
function plotTrendChart() {
  if(window.jQuery && $("#downloadTrendChart").length > 0) {
    Plotly.d3.json('download_date/peroid', function(rawData){
      downloadTrendChartData = [{
        type: 'bar',
        x: [rawData["data"]["0-30"], rawData["data"]["31-90"], rawData["data"]["90-"]],
        y: ['< 31 days', '31-90 days', '> 90 days'],
        orientation: 'h',
        marker: {
          color: 'rgba(55,128,191,1)',
          width: 1
        }
      }];
      Plotly.newPlot('downloadTrendChart', downloadTrendChartData, downloadTrendChartLayout);
    });
  }
}

/*
* desc : auto resize charts (RWD)
*/
function rendering() {
  if(window.jQuery && $("#downloadTrendChartContainer").length > 0 && $("#downloadTrendChart").length > 0) {
    Plotly.d3.select('#downloadTrendChart svg').html('');
    // set layout options
    downloadTrendChartLayout["height"] = parseInt(Plotly.d3.select("#downloadTrendChartContainer").style("height"), 10);
    downloadTrendChartLayout["width"] = parseInt(Plotly.d3.select("#downloadTrendChartContainer").style("width"), 10);
    downloadTrendChartLayout["autosize"] = false;

    // try to plot a new one image
    Plotly.newPlot('downloadTrendChart', downloadTrendChartData, downloadTrendChartLayout);
  }
}

/*
* desc : jquery
*/
$(document).ready(function() {
  sortDwnTable();
  plotTrendChart();
  
  // listen to the resize event
  Plotly.d3.select(window).on('resize', rendering);
});







