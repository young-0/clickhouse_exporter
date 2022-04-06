package main

import (
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	// "os"

	"github.com/ClickHouse/clickhouse_exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/log"
)

var (
	listeningAddress    = flag.String("telemetry.address", ":9116", "Address on which to expose metrics.")
	metricsEndpoint     = flag.String("telemetry.endpoint", "/metrics", "Path under which to expose metrics.")
	clickhouseScrapeURI = flag.String("scrape_uri", "http://clickhouse-domain-com:8123/", "URI to clickhouse http endpoint")
	clickhouseOnly      = flag.Bool("clickhouse_only", true, "Expose only Clickhouse metrics, not metrics from the exporter itself")
	insecure            = flag.Bool("insecure", true, "Ignore server certificate if using https")
	user                = os.Getenv("CLICKHOUSE_USER")
	password            = os.Getenv("CLICKHOUSE_PASSWORD")
	cacheTime		= flag.Int("cache_time",300, "cache time")
)

func main() {
	flag.Parse()

	uri, err := url.Parse(*clickhouseScrapeURI)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Scraping %s", *clickhouseScrapeURI)

	registerer := prometheus.DefaultRegisterer
	gatherer := prometheus.DefaultGatherer
	if *clickhouseOnly {
		reg := prometheus.NewRegistry()
		registerer = reg
		gatherer = reg
	}

	e := exporter.NewExporter(*uri, *insecure, user, password)
	registerer.MustRegister(e)

	http.Handle(*metricsEndpoint, promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Clickhouse Exporter</title></head>
			<body>
			<h1>Clickhouse Exporter</h1>
			<p><a href="` + *metricsEndpoint + `">Metrics</a></p>
			</body>
			</html>`))
	})

	fmt.Println("init iniTickerFunc, cacheTime ", *cacheTime)
	var duration int = *cacheTime
    ticker1 := time.NewTicker(time.Duration(duration) * time.Second)
    defer ticker1.Stop()
    go func(t *time.Ticker) {
        for {
            <-t.C
			exporter.CacheResult = nil
            fmt.Println("Ticker: clear cache", time.Now().Format("2006-01-02 15:04:05"))
        }
    }(ticker1)

	log.Fatal(http.ListenAndServe(*listeningAddress, nil))
}
