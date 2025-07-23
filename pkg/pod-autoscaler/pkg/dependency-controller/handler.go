// Derived from pkg/podscale-controller/pkg/controller/handler.go

package dependencycontroller

import (
	"reflect"

	v1alpha1 "github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	"k8s.io/klog/v2"
)

func (c *Controller) handleDependencyGraphAdd(new interface{}) {
	c.depdagsWorkQueue.Enqueue(new)
}

func (c *Controller) handleDependencyGraphDelete(old interface{}) {
	c.depdagsWorkQueue.Enqueue(old)
}

func (c *Controller) handleDependencyGraphUpdate(old, new interface{}) {

	// Type assert the objects to your DependencyGraph type
	oldGraph, okOld := old.(*v1alpha1.DependencyGraph)
	newGraph, okNew := new.(*v1alpha1.DependencyGraph)

	if !okOld || !okNew {
		// This should ideally not happen if informers are set up correctly,
		return
	}
	if !reflect.DeepEqual(oldGraph.Spec, newGraph.Spec) {
		klog.Infof("[N+] DependencyGraph spec changed for %s:%s, enqueuing for reconciliation", newGraph.Namespace, newGraph.Name)
		c.depdagsWorkQueue.Enqueue(new)
	}
}
