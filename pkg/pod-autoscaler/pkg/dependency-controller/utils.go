package dependencycontroller

import (
	"fmt"

	np "github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
)

func MakeNamespaceNameKey(namespace string, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
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

func (s *Status) ExternalResponseTime(key string) (int64, bool) {
	ert, ok := s.ExternalResponseTimesMap.Load(key)
	if !ok {
		return 0, false
	}
	ertq, ok := ert.(int64)
	if !ok {
		return 0, false
	}
	return ertq, true
}

func (s *Status) NominalResponseTime(key string) (int64, bool) {
	nrt, ok := s.NominalResponseTimesMap.Load(key)
	if !ok {
		return 0, false
	}
	nrtq, ok := nrt.(int64)
	if !ok {
		return 0, false
	}
	return nrtq, true
}

func (s *Status) GetLocalResponseTimeMilli(functionNamespace string, functionName string, responseTime *v1beta2.MetricValue) int64 {
	rt := responseTime.Value.MilliValue()

	// this just catches the rt == 0 case, which happens when there's no requests
	if rt <= 0 {
		return rt
	}

	// local response time
	key := MakeNamespaceNameKey(functionNamespace, functionName)
	nrt, _ := s.NominalResponseTime(key)
	ert, _ := s.ExternalResponseTime(key)
	lrt := rt - ert

	klog.Infof("[N+] Compute local response time (milli) for %s: rt=%d, nrt=%d, ert=%d, lrt=%d", functionName, rt, nrt, ert, lrt)

	// avoid paradoxes
	if lrt < nrt {
		return nrt
	}

	return lrt
}
