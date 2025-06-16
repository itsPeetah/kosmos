package recommender

import (
	"fmt"

	"github.com/lterrac/system-autoscaler/pkg/apis/systemautoscaler/v1beta1"
	"k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
)

func (c *Controller) computeLocalResponseTime(podScale *v1beta1.PodScale, responseTime *v1beta2.MetricValue) *v1beta2.MetricValue {
	key := fmt.Sprintf("%s/%s", podScale.Namespace, podScale.Name)

	ert, _ := c.dependencyStatus.ExternalResponseTime(key)
	nrt, _ := c.dependencyStatus.NominalResponseTime(key)

	// subtract external response time and set lower bound as nrt
	responseTime.Value.Sub(ert)
	if responseTime.Value.Cmp(nrt) < 0 {
		responseTime.Value.Set(nrt.MilliValue())
	}

	return responseTime
}
