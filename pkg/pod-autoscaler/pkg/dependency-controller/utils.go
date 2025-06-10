package dependencycontroller

import (
	"fmt"

	np "github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
)

func MakeNamespaceNameKey(namespace string, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func (ss *SharedStatus) ExternalResponseTime(key string) (int64, bool) {
	ert, ok := ss.ExternalResponseTimesMap.Load(key)
	if !ok {
		return 0, false
	}
	ertq, ok := ert.(int64)
	if !ok {
		return 0, false
	}
	return ertq, true
}

func (ss *SharedStatus) NominalResponseTime(key string) (int64, bool) {
	nrt, ok := ss.NominalResponseTimesMap.Load(key)
	if !ok {
		return 0, false
	}
	nrtq, ok := nrt.(int64)
	if !ok {
		return 0, false
	}
	return nrtq, true
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
