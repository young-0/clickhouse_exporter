package exporter

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

const (
	namespace = "clickhouse" // For Prometheus metrics.
)



var CacheResult []cdpEventsResult


// Exporter collects clickhouse stats from the given URI and exports them using
// the prometheus metrics package.
type Exporter struct {
	// metricsURI      string
	// asyncMetricsURI string
	// eventsURI       string

	partsURI        string
	client          *http.Client

	ckDomain url.URL
	cdpEventsURI string
	cdpEventsTableList []string

	scrapeFailures prometheus.Counter

	user     string
	password string

}

// NewExporter returns an initialized Exporter.
func NewExporter(uri url.URL, insecure bool, user, password string) *Exporter {
	q := uri.Query()

	partsURI := uri
	q.Set("query", "select database, table, sum(bytes) as bytes, count() as parts, sum(rows) as rows from system.parts where active = 1 group by database, table")
	partsURI.RawQuery = q.Encode()

	return &Exporter{
		ckDomain: uri,
		partsURI:   partsURI.String(),
		scrapeFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrape_failures_total",
			Help:      "Number of errors while scraping clickhouse.",
		}),
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
			},
			Timeout: 30 * time.Second,
		},
		user:     user,
		password: password,
	}
}

// Describe describes all the metrics ever exported by the clickhouse exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// from clickhouse. So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh
}

func (e *Exporter) getCdpEventsTableList() error {
	uri := e.ckDomain
	q := uri.Query()

	getTableListURI := uri
	q.Set("query", "select name from system.tables where multiMatchAny(name, ['cdp_events_[0-9].+'])")
	getTableListURI.RawQuery = q.Encode()

	data, err := e.handleResponse(getTableListURI.String())
	if err != nil {
		return err
	}
	// fmt.Printf("getCdpEventsTableList data: %s",data)
	// Parsing results
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		// fmt.Printf("%d:%s\n",i,line)
	}
	e.cdpEventsTableList = lines

	return nil
	// if err != nil {
	// 	fmt.Errorf("Error scraping clickhouse url %v: %v", e.cdpTableNameURI, err)
	// }

	// return &events_table_list
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) error {
	events, err := e.parseCdpEventsResponse()
	if err != nil {
		return fmt.Errorf("Error scraping parseCdpEventsResponse: %v", err)
	}
	for _, event := range events {
		newRowsMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cdp_events_count",
			Help:      "cdp events table count",
		}, []string{"table"}).WithLabelValues( event.table)
		newRowsMetric.Set(float64(event.count))
		newRowsMetric.Collect(ch)
	}

	//
	// parts, err := e.parsePartsResponse(e.partsURI)
	// if err != nil {
	// 	return fmt.Errorf("Error scraping clickhouse url %v: %v", e.partsURI, err)
	// }
	// for _, part := range parts {
	// 	newRowsMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
	// 		Namespace: namespace,
	// 		Name:      "table_parts_rows",
	// 		Help:      "Number of rows in the table",
	// 	}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
	// 	newRowsMetric.Set(float64(part.rows))
	// 	newRowsMetric.Collect(ch)
	// }

	return nil
}

func (e *Exporter) handleResponse(uri string) ([]byte, error) {
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}
	if e.user != "" && e.password != "" {
		req.Header.Set("X-ClickHouse-User", e.user)
		req.Header.Set("X-ClickHouse-Key", e.password)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Error scraping clickhouse: %v", err)
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		if err != nil {
			data = []byte(err.Error())
		}
		return nil, fmt.Errorf("Status %s (%d): %s", resp.Status, resp.StatusCode, data)
	}

	return data, nil
}

type lineResult struct {
	key   string
	value float64
}

func parseNumber(s string) (float64, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}

	return v, nil
}

type partsResult struct {
	database string
	table    string
	bytes    int
	parts    int
	rows     int
}
type cdpEventsResult struct {
	database string
	table    string
	count     int
}
func (e *Exporter) parseCdpEventsResponse() ([]cdpEventsResult, error) {
	if nil != CacheResult {
		fmt.Println("use cache", time.Now().Format("2006-01-02 15:04:05"))
		return CacheResult, nil
	}

	err := e.getCdpEventsTableList()
	if err != nil {
		return nil,fmt.Errorf("Error scraping getCdpEventsTableList %s", err)
	}
	var results []cdpEventsResult = make([]cdpEventsResult, 0)
	for _, tableName := range e.cdpEventsTableList {
		if tableName == "" {
			continue
		}

		uri := e.ckDomain
		q := uri.Query()

		cdpEventsURI := uri
		sql := fmt.Sprintf("select count(*) as counts_events from xsy_dataplatform.%s where created_at > toDateTime('%s 00:00:00')",tableName,  time.Now().Format("2006-01-02"))
		// fmt.Printf("%s\n",sql)
		q.Set("query", sql)
		cdpEventsURI.RawQuery = q.Encode()

		data, err := e.handleResponse(cdpEventsURI.String())
		if err != nil {
			fmt.Printf("%s\n",err)
			return nil, err
		}
		count,err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			return nil, err
		}

		results = append(results, cdpEventsResult{"xsy_dataplatform", tableName,  count})
	}
	CacheResult = make([]cdpEventsResult, len(results))
	copy(CacheResult, results)
	// fmt.Printf("%s\n",results)

	return results, nil
}
func (e *Exporter) parsePartsResponse(uri string) ([]partsResult, error) {
	data, err := e.handleResponse(uri)
	if err != nil {
		return nil, err
	}

	// Parsing results
	lines := strings.Split(string(data), "\n")
	var results []partsResult = make([]partsResult, 0)

	for i, line := range lines {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		if len(parts) != 5 {
			return nil, fmt.Errorf("parsePartsResponse: unexpected %d line: %s", i, line)
		}
		database := strings.TrimSpace(parts[0])
		table := strings.TrimSpace(parts[1])

		bytes, err := strconv.Atoi(strings.TrimSpace(parts[2]))
		if err != nil {
			return nil, err
		}

		count, err := strconv.Atoi(strings.TrimSpace(parts[3]))
		if err != nil {
			return nil, err
		}

		rows, err := strconv.Atoi(strings.TrimSpace(parts[4]))
		if err != nil {
			return nil, err
		}

		results = append(results, partsResult{database, table, bytes, count, rows})
	}

	return results, nil
}
func (e *Exporter) c(uri string) ([]partsResult, error) {
	data, err := e.handleResponse(uri)
	if err != nil {
		return nil, err
	}

	// Parsing results
	lines := strings.Split(string(data), "\n")
	var results []partsResult = make([]partsResult, 0)

	for i, line := range lines {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		if len(parts) != 5 {
			return nil, fmt.Errorf("parsePartsResponse: unexpected %d line: %s", i, line)
		}
		database := strings.TrimSpace(parts[0])
		table := strings.TrimSpace(parts[1])

		bytes, err := strconv.Atoi(strings.TrimSpace(parts[2]))
		if err != nil {
			return nil, err
		}

		count, err := strconv.Atoi(strings.TrimSpace(parts[3]))
		if err != nil {
			return nil, err
		}

		rows, err := strconv.Atoi(strings.TrimSpace(parts[4]))
		if err != nil {
			return nil, err
		}

		results = append(results, partsResult{database, table, bytes, count, rows})
	}

	return results, nil
}

// Collect fetches the stats from configured clickhouse location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	upValue := 1

	if err := e.collect(ch); err != nil {
		log.Printf("Error scraping clickhouse: %s", err)
		e.scrapeFailures.Inc()
		e.scrapeFailures.Collect(ch)

		upValue = 0
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "up"),
			"Was the last query of ClickHouse successful.",
			nil, nil,
		),
		prometheus.GaugeValue, float64(upValue),
	)

}

func metricName(in string) string {
	out := toSnake(in)
	return strings.Replace(out, ".", "_", -1)
}

// toSnake convert the given string to snake case following the Golang format:
// acronyms are converted to lower-case and preceded by an underscore.
func toSnake(in string) string {
	runes := []rune(in)
	length := len(runes)

	var out []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) && ((i+1 < length && unicode.IsLower(runes[i+1])) || unicode.IsLower(runes[i-1])) {
			out = append(out, '_')
		}
		out = append(out, unicode.ToLower(runes[i]))
	}

	return string(out)
}

// check interface
var _ prometheus.Collector = (*Exporter)(nil)
