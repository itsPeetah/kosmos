package dependencycontroller

import (
	"fmt"

	np "github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	"github.com/lterrac/system-autoscaler/pkg/metrics-exposer/pkg/metrics"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog"
	"k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
)

func (c *Controller) aggregateGraphTimes() {

	avgFunctionRTs := make(map[string]*resource.Quantity)

	klog.Infof("\n\n\n\n\n\n\nAggregating graph times\n\n\n\n\n\n\n")

	c.status.graphMap.Range(func(key, value interface{}) bool {

		klog.Info("\n\n There's an item in the map")
		return true
	})

	c.status.graphMap.Range(func(key, value interface{}) bool {
		klog.Infof("[N+] Aggregating graph times for dependency graph %s", key)

		nodes, ok := value.([]np.FunctionNode)
		if !ok {
			klog.Errorf("[N+] Could not parse sorted nodes for graph %s", key)
			return false
		}

		// Get times for all services
		for _, node := range nodes {

			metric, err := c.getServiceAverageResponseTime(node.FunctionNamespace, node.FunctionName)
			if err != nil {
				klog.Errorf("[N+] Could not retrieve response time metrics for service %s:%s. %v", node.FunctionNamespace, node.FunctionName, err)
			} else {
				avgFunctionRTs[fmt.Sprintf("%s:%s", node.FunctionNamespace, node.FunctionName)] = &metric.Value
			}
		}

		// Aggregate times
		avgEdgeRTs := make(map[int]int)
		for _, node := range nodes {

			for _, edge := range node.Invocations {
				currFunctionEdgeValue, ok := avgFunctionRTs[edge.FunctionNamespace+":"+edge.FunctionName]
				if !ok {
					currFunctionEdgeValue = resource.NewMilliQuantity(0, resource.BinarySI)
				}

				multed := int(currFunctionEdgeValue.MilliValue()) * edge.EdgeMultiplier
				if val, ok := avgEdgeRTs[edge.EdgeId]; ok {
					if multed > val {
						avgEdgeRTs[edge.EdgeId] = multed
					}
				} else {
					// This is either a sequential call or the first time we see a parallel call (therefore this is the slowest so far)
					avgEdgeRTs[edge.EdgeId] = multed
				}
			}

		}

		// Calculate average external response time for every function
		for _, node := range nodes {
			sum := resource.NewMilliQuantity(0, resource.BinarySI)
			for _, edge := range node.Invocations {
				sum.Add(*resource.NewMilliQuantity(int64(avgEdgeRTs[edge.EdgeId]), resource.BinarySI))
				// mark edge as already counted to avoid counting it multiple times for parallel invocations
				avgEdgeRTs[edge.EdgeId] = 0
			}
			c.SharedStatus.ExternalResponseTimesMap.Store(node.FunctionNamespace+":"+node.FunctionName, sum)
			klog.Infof("[N+] %s:%s - External response time for function: %d", node.FunctionNamespace, node.FunctionName, sum.MilliValue())
		}

		return true
	})

}

func (c *Controller) getServiceAverageResponseTime(namespace string, name string) (*v1beta2.MetricValue, error) {

	svc, err := c.listers.Services(namespace).Get(name)
	if err != nil {
		return nil, err
	}

	selector := labels.SelectorFromSet(svc.Spec.Selector)
	if selector.Empty() {
		return nil, fmt.Errorf("[N+] Service %s:%s has an empty selector", namespace, name)
	}

	pods, err := c.listers.Pods(namespace).List(selector)
	if err != nil {
		return nil, fmt.Errorf("[N+] error listing pods with selector '%s': %v", selector.String(), err)
	}
	if len(pods) < 1 {
		return nil, fmt.Errorf("[N+] no pods found with selector '%s'", selector.String())
	}

	var val int64 = 0
	for _, pod := range pods {
		metrics, err := c.MetricClient.PodMetrics(pod, metrics.ResponseTime)
		if err != nil {
			return nil, err
		}
		val += metrics.Value.MilliValue()
	}
	average := float64(val) / float64(len(pods))
	return &v1beta2.MetricValue{Value: *resource.NewMilliQuantity(int64(average), resource.DecimalSI)}, nil

}
