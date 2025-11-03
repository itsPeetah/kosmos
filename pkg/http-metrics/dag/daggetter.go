package dag

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	np "github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	"k8s.io/klog/v2"
)

const exposerUrl = "http://np-dag-expo-service.kube-system.svc.cluster.local/get-json"

var (
	functionName      string
	functionNamespace string
	DepDAG            *np.DependencyGraphSpec = nil
)

func InitDag() {
	functionName = os.Getenv("FUNCTION")
	functionNamespace = os.Getenv("NAMESPACE")
}

func GetDagJson() {
	url := fmt.Sprintf("%s?name=%s&namespace=%s", exposerUrl, functionName, functionNamespace)
	klog.Infof("Requesting dag at: %s", url)
	res, err := http.Get(url)
	if err != nil {
		klog.Errorf("HTTP request failed: %v", err)
		DepDAG = nil
		return
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		klog.Errorf("Unexpected status code %d while fetching DAG. Body: %s", res.StatusCode, string(body))
		DepDAG = nil
		return
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		klog.Errorf("Failed reading response body: %v", err)
		DepDAG = nil
		return
	}

	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" || strings.EqualFold(trimmed, "null") {
		klog.Infof("No DAG found for %s/%s (empty or null response)", functionNamespace, functionName)
		DepDAG = nil
		return
	}

	if klog.V(4).Enabled() {
		max := 512
		if len(body) < max {
			max = len(body)
		}
		klog.Infof("DAG response preview (%d bytes): %s", len(body), string(body[:max]))
	}

	var depGraph np.DependencyGraphSpec
	if err := json.Unmarshal(body, &depGraph); err != nil {
		preview := body
		if len(preview) > 200 {
			preview = preview[:200]
		}
		klog.Errorf("Failed to decode DAG JSON: %v. Preview: %s", err, string(preview))
		DepDAG = nil
		return
	}
	DepDAG = &depGraph

	klog.Infof("Successfully retrieved DAG: %v", DepDAG)
}
