package dag

import (
	"fmt"
	"time"

	"github.com/asecurityteam/rolling"
	"github.com/modern-go/concurrent"
	"k8s.io/klog/v2"
)

type DAGWindowsOptions struct {
	WinSize int64
	WinGran int64
	BuckDur time.Duration
}

type DAGWindows struct {
	Windows       *concurrent.Map
	InvocationIDs *concurrent.Map
}

func NewDAGWindows(options *DAGWindowsOptions) *DAGWindows {

	dw := &DAGWindows{
		Windows:       concurrent.NewMap(),
		InvocationIDs: concurrent.NewMap(),
	}

	if DepDAG == nil {
		klog.Infof("DAG is nil")
		return dw
	}

	for _, node := range DepDAG.Nodes {
		if node.FunctionName == functionName && node.FunctionNamespace == functionNamespace {
			for _, invocation := range node.Invocations {
				key := fmt.Sprintf("%s/%s", invocation.FunctionNamespace, invocation.FunctionName)
				klog.Infof("Registering new dependency: %s", key)
				window := rolling.NewWindow(int(options.WinSize / options.WinGran))
				policy := rolling.NewTimePolicy(window, options.BuckDur)
				dw.Windows.Store(key, policy)
				dw.InvocationIDs.Store(key, invocation.EdgeId)
			}
			break
		}
	}

	return dw
}

func (dw *DAGWindows) RecordResponseTime(key string, ms int64) {
	m, ok := dw.Windows.Load(key)
	if !ok {
		klog.Errorf("Could not append time for key: %s", key)
	}
	m.(*rolling.TimePolicy).Append(float64(ms))
}
