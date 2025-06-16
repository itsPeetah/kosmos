package dependencycontroller

import (
	"github.com/modern-go/concurrent"
	"k8s.io/apimachinery/pkg/api/resource"
)

type SharedStatus struct {
	// Key: namespace:name of the function, Value: computed external response time
	ExternalResponseTimesMap *concurrent.Map
	// Key: namespace:name of the function, Value: nominal response time as noted in the graph
	NominalResponseTimesMap *concurrent.Map
}

type Status struct {
	// Key: namespace:name of the graph, Value: nodes, sorted leaves-to-root
	graphMap concurrent.Map
}

func (ss *SharedStatus) ExternalResponseTime(key string) (resource.Quantity, bool) {
	ert, ok := ss.ExternalResponseTimesMap.Load(key)
	if !ok {
		return *resource.NewMilliQuantity(0, resource.BinarySI), false
	}
	ertq, ok := ert.(resource.Quantity)
	if !ok {
		return *resource.NewMilliQuantity(0, resource.BinarySI), false
	}
	return ertq, true
}

func (ss *SharedStatus) NominalResponseTime(key string) (resource.Quantity, bool) {
	nrt, ok := ss.ExternalResponseTimesMap.Load(key)
	if !ok {
		return *resource.NewMilliQuantity(0, resource.BinarySI), false
	}
	nrtq, ok := nrt.(resource.Quantity)
	if !ok {
		return *resource.NewMilliQuantity(0, resource.BinarySI), false
	}
	return nrtq, true
}
