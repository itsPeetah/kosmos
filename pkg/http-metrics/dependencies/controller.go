package dependencies

import (
	"math"
	"strings"

	"k8s.io/klog"
)

type DependencyController struct {
	scaleFactor float64
	windows     *DAGWindows
	nlrt        int64
}

func NewDependencyController(scaleFactor float64, windowsOptions *DAGWindowsOptions) *DependencyController {

	nodeString, dependenciesAreDefined := getNodeString()
	dagNode := parseNodeString(nodeString, dependenciesAreDefined)

	return &DependencyController{
		scaleFactor: math.Min(1, math.Max(0, scaleFactor)),
		windows:     NewDAGWindows(dagNode, windowsOptions),
		nlrt:        dagNode.NominalLocalResponseTime.MilliValue(),
	}
}

func (c *DependencyController) RecordResponseTime(requestPath string, milliseconds int64) {
	parts := strings.Split(requestPath, "/")
	if len(parts) >= 3 && parts[0] == "function" {
		fns := parts[1]
		fname := parts[2]
		key := fns + "/" + fname
		err := c.windows.recordResponseTime(key, milliseconds)
		if err != nil {
			klog.Errorf("Could not append time for key: %s", key)
		}
	}
}

func (c *DependencyController) GetExternalResponeTime() float64 {
	ert := c.windows.getExternalResponeTime()
	if math.IsNaN(ert) {
		ert = 0
	}
	return ert
}
