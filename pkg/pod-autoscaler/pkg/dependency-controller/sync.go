// Derived from pkg/podscale-controller/pkg/controller/sync.go

package dependencycontroller

import (
	"fmt"

	np "github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
)

// syncServiceLevelAgreement compares the actual SLA with the desired, and attempts to
// converge the two.
func (c *Controller) syncDependencyGraph(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)

	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
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
			utilruntime.HandleError(fmt.Errorf("error while getting service %s:%s tracked by Dependency Graph %s:%s", node.FunctionNamespace, node.FunctionName, namespace, name))
			return nil
		}
	}

	// Store the nodes in the map (overwriting old ones if already present)
	dagKey := fmt.Sprintf("%s:%s", namespace, name)
	nodesSorted := sortNodesByDependencies(dag.DeepCopy().Spec.Nodes)
	c.status.graphMap.Store(dagKey, nodesSorted)

	c.recorder.Event(dag, corev1.EventTypeNormal, "Synced", fmt.Sprintf("Dependency graph %s synced successfully", dagKey))
	return nil
}

// I don't think this is particularly optimized, but it's not running often and the code that I got Gemini to generate for me was utter trash
func sortNodesByDependencies(nodes []np.FunctionNode) []np.FunctionNode {

	// NodeName -> NodeIndex
	remainingNodeIndices := make(map[string]int)
	for index, node := range nodes {
		remainingNodeIndices[node.FunctionName] = index
	}

	calculateOutDegrees := func() map[string]int {
		outDegrees := make(map[string]int)
		// initialize at 0
		for nodeName, indexInArray := range remainingNodeIndices {
			node := nodes[indexInArray]
			outDegrees[nodeName] = 0
			// increase outdegree only if invoked node has not been "sorted" yet
			for _, edge := range node.Invocations {
				_, ok := remainingNodeIndices[edge.FunctionName]
				if ok {
					outDegrees[nodeName]++
				}
			}
		}
		return outDegrees
	}

	// Get "current" leaves (nodes with outdegree 0 in the current scenario, i.e. with already sorted nodes excluded from the pool)
	getCurrentLeaves := func(outDegreeMap map[string]int) []string {
		leaves := []string{}
		for nodeName, degree := range outDegreeMap {
			if degree == 0 {
				leaves = append(leaves, nodeName)
			}
		}
		return leaves
	}

	sortedNodes := []np.FunctionNode{}

	iterations := 0 // just to make sure that I don't run into an infinite loop: there should never be more iterations than nodes
	limit := len(nodes)
	for len(sortedNodes) < limit && iterations <= limit {
		outDegree := calculateOutDegrees()
		leaves := getCurrentLeaves(outDegree)

		for _, leafName := range leaves {
			sortedNodes = append(sortedNodes, nodes[remainingNodeIndices[leafName]])
			// remove current node from pool of nodes that still need to be sorted
			delete(remainingNodeIndices, leafName)
		}

		iterations++
	}

	if len(sortedNodes) != len(nodes) {
		return []np.FunctionNode{}
	}

	return sortedNodes
}
