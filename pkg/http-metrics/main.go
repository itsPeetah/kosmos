package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/asecurityteam/rolling"
	"github.com/lterrac/system-autoscaler/pkg/http-metrics/dag"
	"github.com/lterrac/system-autoscaler/pkg/metrics-exposer/pkg/metrics"
)

var target = &url.URL{}
var window = &rolling.TimePolicy{}
var reverseProxy = &httputil.ReverseProxy{}
var dagWindows = &dag.DAGWindows{}
var dispatcherTarget = &url.URL{}
var dispatcherProxy = &httputil.ReverseProxy{}

// Environment
var address string
var port string
var windowSize time.Duration
var windowGranularity time.Duration

func main() {
	mux := http.NewServeMux()

	mux.Handle("/metric/response_time", http.HandlerFunc(ResponseTime))
	mux.Handle("/metric/request_count", http.HandlerFunc(RequestCount))
	mux.Handle("/metric/throughput", http.HandlerFunc(Throughput))
	mux.Handle("/metrics/", http.HandlerFunc(AllMetrics))
	mux.Handle("/function/", http.HandlerFunc(ForwardFunctionRequest))
	mux.Handle("/", http.HandlerFunc(ForwardRequest))

	address = os.Getenv("ADDRESS")
	port = os.Getenv("APP_PORT")
	windowSizeString := os.Getenv("WINDOW_SIZE")
	windowGranularityString := os.Getenv("WINDOW_GRANULARITY")

	var err error
	log.Println("Reading environment variables")

	srv := &http.Server{
		Addr:    ":8000",
		Handler: mux,
	}
	// Initialize dispatcher reverse proxy
	dispatcherTarget, _ = url.Parse("http://dispatcher.default.svc.cluster.local:80")
	dispatcherProxy = httputil.NewSingleHostReverseProxy(dispatcherTarget)
	log.Println("Forwarding all /function requests to:", dispatcherTarget)

	target, _ = url.Parse("http://" + address + ":" + port)
	reverseProxy = httputil.NewSingleHostReverseProxy(target)
	log.Println("Forwarding all requests to:", target)

	windowSize, err = time.ParseDuration(windowSizeString)

	if err != nil {
		log.Fatalf("Failed to parse windows size. Error: %v", err)
	}

	windowGranularity, err = time.ParseDuration(windowGranularityString)

	if err != nil {
		log.Fatalf("Failed to parse windows granularity. Error: %v", err)
	}

	window = rolling.NewTimePolicy(rolling.NewWindow(int(windowSize.Nanoseconds()/windowGranularity.Nanoseconds())), time.Millisecond)
	log.Println("Time window initialized with size:", windowSizeString, " and granularity:", windowGranularityString)

	dag.InitDag()
	dag.GetDagJson()
	options := dag.DAGWindowsOptions{
		WinSize: windowSize.Nanoseconds(),
		WinGran: windowGranularity.Nanoseconds(),
		BuckDur: time.Millisecond,
	}
	klog.Infof("Time windows will be initialized initialized with size: %d and granularity: %d", options.WinSize, options.WinGran)

	dagWindows = dag.NewDAGWindows(&options)

	// output error and quit if ListenAndServe fails
	log.Fatal(srv.ListenAndServe())

}

// ForwardRequest send all the request the the pod except for the ones having metrics/ in the path
func ForwardRequest(res http.ResponseWriter, req *http.Request) {

	klog.Info("FWDREQ")

	requestTime := time.Now()
	reverseProxy.ServeHTTP(res, req)
	responseTime := time.Now()
	delta := responseTime.Sub(requestTime)
	window.Append(float64(delta.Milliseconds()))
}

// ResponseTime return the pod average response time
func ResponseTime(res http.ResponseWriter, req *http.Request) {
	responseTime := window.Reduce(rolling.Avg)
	if math.IsNaN(responseTime) {
		responseTime = 0
	}
	_, _ = fmt.Fprintf(res, `{"%s": %f}`, metrics.ResponseTime.String(), responseTime)
}

// RequestCount return the current number of request sent to the pod
func RequestCount(res http.ResponseWriter, req *http.Request) {
	requestCount := window.Reduce(rolling.Count)
	if math.IsNaN(requestCount) {
		requestCount = 0
	}
	_, _ = fmt.Fprintf(res, `{"%s": %f}`, metrics.RequestCount.String(), requestCount)
}

// Throughput returns the pod throughput in request per second
func Throughput(res http.ResponseWriter, req *http.Request) {
	// throughput := window.Reduce(rolling.Count) / windowSize.Seconds()
	throughput := float64(dagWindows.GetExternalResponeTime())
	if math.IsNaN(throughput) {
		throughput = 0
	}

	_, _ = fmt.Fprintf(res, `{"%s": %f}`, metrics.Throughput.String(), throughput)
}

// AllMetrics returns all the metrics available for the pod
func AllMetrics(res http.ResponseWriter, req *http.Request) {

	responseTime := window.Reduce(rolling.Avg)
	if math.IsNaN(responseTime) {
		responseTime = 0
	}

	requestCount := window.Reduce(rolling.Count)
	if math.IsNaN(requestCount) {
		requestCount = 0
	}

	// throughput := window.Reduce(rolling.Count) / windowSize.Seconds()
	throughput := float64(dagWindows.GetExternalResponeTime())
	if math.IsNaN(throughput) {
		throughput = 0
	}
	// TODO: maybe we should wrap this into an helper function of metrics struct

	klog.Infof(`{"%s": %f,"%s": %f,"%s": %f}`, metrics.ResponseTime.String(), responseTime, metrics.RequestCount.String(), requestCount, metrics.Throughput.String(), throughput)
	_, _ = fmt.Fprintf(res, `{"%s": %f,"%s": %f,"%s": %f}`, metrics.ResponseTime.String(), responseTime, metrics.RequestCount.String(), requestCount, metrics.Throughput.String(), throughput)
}

func ForwardFunctionRequest(res http.ResponseWriter, req *http.Request) {

	klog.Infof("Forwarding request to: %s", req.URL.Path)

	requestTime := time.Now()
	// Forward to dispatcher first to avoid delaying the user due to metric work
	dispatcherProxy.ServeHTTP(res, req)
	responseTime := time.Now()
	delta := responseTime.Sub(requestTime)

	// Extract /function/<fns>/<fname>/...
	path := strings.TrimPrefix(req.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) >= 3 && parts[0] == "function" {
		fns := parts[1]
		fname := parts[2]
		key := fns + "/" + fname
		dagWindows.RecordResponseTime(key, delta.Milliseconds())

		w, _ := dagWindows.Windows.Load(key)
		klog.Infof("Requests made to %s: %f", req.URL.Path, w.(*rolling.TimePolicy).Reduce(rolling.Count))
	}
}
