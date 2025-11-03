package dag

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	np "github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	"k8s.io/klog/v2"
)

const exposerUrl = "http://np-dag-expo-service.kube-system.svc.cluster.local/get-json"

var (
	functionName      string
	functionNamespace string
	DepDAG            *np.DependencyGraph = nil
)

func InitDag() {
	functionName = os.Getenv("FUNCTION_NAME")
	functionNamespace = os.Getenv("FUNCTION_NAMESPACE")
}

func GetDagJson() {
	url := fmt.Sprintf("%s?name=%s&namespace=%s", exposerUrl, functionName, functionNamespace)
	klog.Infof("Requesting dag at: %s", url)
	res, err := http.Get(url)
	if err != nil {
		klog.Error("Error %v", err)
	}
	defer res.Body.Close()
	var depGraph np.DependencyGraph
	if err := json.NewDecoder(res.Body).Decode(&depGraph); err != nil {
		klog.Errorf("Failed to decode DAG JSON: %v", err)
		return
	}
	DepDAG = &depGraph

	klog.Infof("Successfully retrieved DAG: %v", DepDAG)
}
