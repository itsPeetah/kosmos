// Derived from pkg/podscale-controller/pkg/controller/sync.go

package dependencycontroller

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// syncServiceLevelAgreement compares the actual SLA with the desired, and attempts to
// converge the two.
func (c *Controller) syncDependencyGraph(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)

	klog.Infof("[N+] Syncing dependency graph %s:%s", namespace, name)

	if err != nil {
		utilruntime.HandleError(fmt.Errorf("[N+] invalid resource key: %s", key))
		return nil
	}

	// Get the DAG resource with this namespace/name
	dag, err := c.listers.DependencyGraphs(namespace).Get(name)

	if err != nil {
		// The DAG resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	for _, node := range dag.Spec.Nodes {
		_, err := c.listers.Services(node.FunctionNamespace).Get(node.FunctionName)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("[N+] error while getting service %s:%s tracked by Dependency Graph %s:%s", node.FunctionNamespace, node.FunctionName, namespace, name))
			return err
		}
	}

	// Store the nodes in the map (overwriting old ones if already present)
	dagKey := MakeNamespaceNameKey(namespace, name)

	nodesSorted := sortNodesByDependencies(dag.Spec.Nodes)
	c.status.graphMap.Store(dagKey, nodesSorted)

	// Store the nominal response times
	for _, node := range dag.Spec.Nodes {
		c.SharedStatus.NominalResponseTimesMap.Store(MakeNamespaceNameKey(node.FunctionNamespace, node.FunctionName), node.NominalResponseTime.MilliValue())
	}

	c.recorder.Event(dag, corev1.EventTypeNormal, "Synced", fmt.Sprintf("[N+] Dependency graph %s synced successfully", dagKey))
	return nil
}
