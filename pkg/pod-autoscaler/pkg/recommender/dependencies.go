package recommender

import (
	"github.com/lterrac/system-autoscaler/pkg/apis/systemautoscaler/v1beta1"
	dc "github.com/lterrac/system-autoscaler/pkg/pod-autoscaler/pkg/dependency-controller"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
)

func (c *Controller) computeLocalResponseTimeMilli(podScale *v1beta1.PodScale, responseTime *v1beta2.MetricValue) int64 {

	rt := responseTime.Value.MilliValue()

	// this just catches the rt == 0 case, which happens when there's no requests
	if rt <= 0 {
		return rt
	}

	// local response time
	key := dc.MakeNamespaceNameKey(podScale.Namespace, podScale.Spec.Service)
	nrt, _ := c.dependencyStatus.NominalResponseTime(key)
	ert, _ := c.dependencyStatus.ExternalResponseTime(key)
	lrt := rt - ert

	klog.Infof("[N+] Compute local response time (milli) for %s: rt=%d, nrt=%d, ert=%d, lrt=%d", podScale.Spec.Service, rt, nrt, ert, lrt)

	// avoid paradoxes
	if lrt < nrt {
		return nrt
	}

	return lrt
}
