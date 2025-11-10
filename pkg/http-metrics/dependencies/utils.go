package dependencies

import (
	"encoding/json"
	"os"

	np "github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
)

func getNodeString() (nodeString string, depsDefined bool) {
	depString := os.Getenv("DEPDAG_NODE")
	if len(depString) > 0 {
		nodeString = depString
		depsDefined = true
	} else {
		nodeString = ""
		depsDefined = false
	}
	return
}

func parseNodeString(nodeString string, dependenciesAreDefined bool) *np.FunctionNode {
	dagNode := &np.FunctionNode{}
	if dependenciesAreDefined {
		err := json.Unmarshal([]byte(nodeString), dagNode)
		if err != nil {
			dagNode = nil
		}
	} else {
		dagNode = nil
	}
	return dagNode
}
