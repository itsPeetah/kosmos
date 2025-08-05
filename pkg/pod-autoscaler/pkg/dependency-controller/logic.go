package dependencycontroller

import (
	"fmt"

	"github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	"github.com/lterrac/system-autoscaler/pkg/metrics-exposer/pkg/metrics"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog"
)

func (c *Controller) aggregateGraphTimes(nodes []v1alpha1.FunctionNode) {

	avgFunctionRTs := make(map[string]int64)

	// Get times for all services
	for _, node := range nodes {

		svcArt, err := c.getServiceAverageResponseTime(node.FunctionNamespace, node.FunctionName)
		key := MakeNamespaceNameKey(node.FunctionNamespace, node.FunctionName)

		if err != nil {
			klog.Errorf("[N+] Could not retrieve response time metrics for service %s:%s. %v", node.FunctionNamespace, node.FunctionName, err)
			avgFunctionRTs[key] = node.NominalResponseTime.MilliValue()
		} else {
			avgFunctionRTs[key] = svcArt
		}

	}

	avgEdgeRTs := make(map[int]int64)
	for _, node := range nodes {

		// Aggregate times
		for _, edge := range node.Invocations {
			currFunctionEdgeValue, ok := avgFunctionRTs[MakeNamespaceNameKey(edge.FunctionNamespace, edge.FunctionName)]
			if !ok {
				currFunctionEdgeValue = 0
			}

			multed := currFunctionEdgeValue * int64(edge.EdgeMultiplier)
			if val, ok := avgEdgeRTs[edge.EdgeId]; !ok {
				// This is either a sequential call or the first time we see a parallel call (therefore this is the slowest so far)
				avgEdgeRTs[edge.EdgeId] = multed
			} else if multed > val {
				// This means the calls are sequential so take the slowest one
				avgEdgeRTs[edge.EdgeId] = multed
			}
		}

		// Calculate average external response time for every function
		sum := int64(0)
		for _, edge := range node.Invocations {
			sum += avgEdgeRTs[edge.EdgeId]
			avgEdgeRTs[edge.EdgeId] = 0
		}

		c.Status.ExternalResponseTimesMap.Store(MakeNamespaceNameKey(node.FunctionNamespace, node.FunctionName), sum)
		klog.Infof("[N+] %s:%s - External response time for function: %d", node.FunctionNamespace, node.FunctionName, sum)
	}
}

func (c *Controller) getServiceAverageResponseTime(namespace string, name string) (int64, error) {

	svc, err := c.listers.Services(namespace).Get(name)
	if err != nil {
		return 0, err
	}

	selector := labels.SelectorFromSet(svc.Spec.Selector)
	if selector.Empty() {
		return 0, fmt.Errorf("[N+] Service %s:%s has an empty selector", namespace, name)
	}

	pods, err := c.listers.Pods(namespace).List(selector)
	if err != nil {
		return 0, fmt.Errorf("[N+] error listing pods with selector '%s': %v", selector.String(), err)
	}
	if len(pods) < 1 {
		return 0, fmt.Errorf("[N+] no pods found with selector '%s'", selector.String())
	}

	var val int64 = 0
	for _, pod := range pods {
		metrics, err := c.MetricClient.PodMetrics(pod, metrics.ResponseTime)
		if err != nil {
			return 0, err
		}
		val += metrics.Value.MilliValue()
	}
	average := float64(val) / float64(len(pods))
	return int64(average), nil
}
