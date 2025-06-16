package dependencycontroller

import (
	"fmt"

	np "github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	"github.com/lterrac/system-autoscaler/pkg/metrics-exposer/pkg/metrics"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"
)

func (c *Controller) aggregateGraphTimes() {

	avgFunctionRTs := make(map[string]*resource.Quantity)

	// send service metrics to out channel
	c.status.graphMap.Range(func(key, value interface{}) bool {
		graph, ok := value.(np.DependencyGraph)
		if !ok {
			return false
		}

		// Get times for all services
		for _, node := range graph.Spec.Nodes {

			svc, err := c.listers.Services(node.FunctionNamespace).Get(node.FunctionName)
			if err != nil {
				avgFunctionRTs[fmt.Sprintf("%s:%s", svc.Namespace, svc.Name)] = resource.NewMilliQuantity(0, resource.BinarySI)
			}

			metric, err := c.MetricClient.ServiceMetrics(svc, metrics.ResponseTime)
			if err != nil {
				avgFunctionRTs[fmt.Sprintf("%s:%s", svc.Namespace, svc.Name)] = &metric.Value
			}
		}

		// Aggregate times
		avgEdgeRTs := make(map[int]int)
		for _, node := range graph.Spec.Nodes {

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
		for _, node := range graph.Spec.Nodes {
			sum := resource.NewMilliQuantity(0, resource.BinarySI)
			for _, edge := range node.Invocations {
				sum.Add(*resource.NewMilliQuantity(int64(avgEdgeRTs[edge.EdgeId]), resource.BinarySI))
				// mark edge as already counted to avoid counting it multiple times for parallel invocations
				avgEdgeRTs[edge.EdgeId] = 0
			}
			c.SharedStatus.ExternalResponseTimesMap.Store(node.FunctionNamespace+":"+node.FunctionName, sum)
			klog.Infof("[%s:%s] External response time for function: %d", node.FunctionNamespace, node.FunctionName, sum.MilliValue())
		}

		return true
	})
}
