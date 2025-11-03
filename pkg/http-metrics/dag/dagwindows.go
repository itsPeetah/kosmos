package dag

import (
	"fmt"
	"time"

	"github.com/asecurityteam/rolling"
)

type DAGWindows struct {
	Windows       map[string]*rolling.TimePolicy
	InvocationIDs map[string]int
}

func NewDAGWindows(window rolling.Window, bucketDuration time.Duration) DAGWindows {

	dagWindows := make(map[string]*rolling.TimePolicy)
	invocationIDs := make(map[string]int)

	for _, node := range DepDAG.Nodes {
		if node.FunctionName == functionName && node.FunctionNamespace == functionNamespace {
			for _, invocation := range node.Invocations {
				key := fmt.Sprintf("%s/%s", invocation.FunctionNamespace, invocation.FunctionName)
				dagWindows[key] = rolling.NewTimePolicy(window, bucketDuration)
				invocationIDs[key] = invocation.EdgeId
			}
			break
		}
	}

	return DAGWindows{Windows: dagWindows, InvocationIDs: invocationIDs}
}
