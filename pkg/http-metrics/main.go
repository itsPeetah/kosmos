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
	"github.com/lterrac/system-autoscaler/pkg/http-metrics/dependencies"
	"github.com/lterrac/system-autoscaler/pkg/metrics-exposer/pkg/metrics"
)

var target = &url.URL{}
var window = &rolling.TimePolicy{}
var reverseProxy = &httputil.ReverseProxy{}
var dispatcherTarget = &url.URL{}
var dispatcherProxy = &httputil.ReverseProxy{}
var dependencyController = &dependencies.DependencyController{}

// Environment
var address string
var port string
var windowSize time.Duration
var windowGranularity time.Duration

func main() {
	mux := http.NewServeMux()

	mux.Handle("/metric/response_time", http.HandlerFunc(ResponseTime))
	mux.Handle("/metric/local_response_time", http.HandlerFunc(LocalResponseTime))
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

	options := dependencies.DAGWindowsOptions{
		WinSize: windowSize.Nanoseconds(),
		WinGran: windowGranularity.Nanoseconds(),
		BuckDur: time.Millisecond,
	}
	dependencyController = dependencies.NewDependencyController(
		0.8, // arbitrary scale factor for now
		&options,
	)

	// output error and quit if ListenAndServe fails
	log.Fatal(srv.ListenAndServe())

}

// ForwardRequest send all the request the the pod except for the ones having metrics/ in the path
func ForwardRequest(res http.ResponseWriter, req *http.Request) {
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

// ResponseTime return the pod average response time
func LocalResponseTime(res http.ResponseWriter, req *http.Request) {
	responseTime := getLocalResponseTime()
	_, _ = fmt.Fprintf(res, `{"%s": %f}`, metrics.LocalResponseTime.String(), responseTime)
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
	throughput := window.Reduce(rolling.Count) / windowSize.Seconds()
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

	localResponseTime := getLocalResponseTime()

	requestCount := window.Reduce(rolling.Count)
	if math.IsNaN(requestCount) {
		requestCount = 0
	}

	throughput := window.Reduce(rolling.Count) / windowSize.Seconds()
	if math.IsNaN(throughput) {
		throughput = 0
	}
	// TODO: maybe we should wrap this into an helper function of metrics struct

	json := fmt.Sprintf(`{"%s": %f,"%s": %f,"%s": %f, "%s": "%f"}`,
		metrics.ResponseTime.String(), responseTime,
		metrics.RequestCount.String(), requestCount,
		metrics.Throughput.String(), throughput,
		metrics.LocalResponseTime.String(), localResponseTime,
	)

	klog.Infof(json)
	_, _ = fmt.Fprintf(res, "%s", json)
}

func ForwardFunctionRequest(res http.ResponseWriter, req *http.Request) {
	requestTime := time.Now()
	// Forward to dispatcher first to avoid delaying the user due to metric work
	dispatcherProxy.ServeHTTP(res, req)
	responseTime := time.Now()
	delta := responseTime.Sub(requestTime)

	// Extract /function/<fns>/<fname>/...
	path := strings.TrimPrefix(req.URL.Path, "/")
	dependencyController.RecordResponseTime(path, delta.Milliseconds())
}

func getLocalResponseTime() float64 {

	responseTime := window.Reduce(rolling.Avg)
	if math.IsNaN(responseTime) {
		responseTime = 0
	}

	externalTime := dependencyController.GetExternalResponeTime()

	localTime := responseTime - externalTime
	if localTime <= 0 {
		localTime = 0
	}

	return localTime
}
