package recommender

import (
	"fmt"

	"github.com/lterrac/system-autoscaler/pkg/apis/systemautoscaler/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
)

func (c *Controller) computeLocalResponseTime(podScale *v1beta1.PodScale, responseTime *v1beta2.MetricValue) *v1beta2.MetricValue {
	key := fmt.Sprintf("%s/%s", podScale.Namespace, podScale.Name)

	ert, ok := c.extTimesIn.Load(key)
	if !ok {
		ert = resource.NewMilliQuantity(0, resource.BinarySI)
	}

	// TODO:itspeetah Clamp with nrt to avoid paradoxes

	responseTime.Value.Sub(ert.(resource.Quantity))
	return responseTime
}
