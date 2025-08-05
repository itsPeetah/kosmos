// Derived from pkg/podscale-controller/pkg/controller/handler.go

package dependencycontroller

func (c *Controller) handleDependencyGraphAdd(new interface{}) {
	c.depdagsWorkQueue.Enqueue(new)
}

func (c *Controller) handleDependencyGraphDelete(old interface{}) {
	c.depdagsWorkQueue.Enqueue(old)
}

func (c *Controller) handleDependencyGraphUpdate(old, new interface{}) {
	c.depdagsWorkQueue.Enqueue(new)
}
