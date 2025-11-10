package dependencies

import (
	"fmt"
	"time"

	"github.com/asecurityteam/rolling"
	"github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	"github.com/modern-go/concurrent"
	"k8s.io/klog"
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

func NewDAGWindows(node *v1alpha1.FunctionNode, options *DAGWindowsOptions) *DAGWindows {
	dw := &DAGWindows{
		Windows:       concurrent.NewMap(),
		InvocationIDs: concurrent.NewMap(),
	}

	if node != nil {
		for _, invocation := range node.Invocations {
			key := fmt.Sprintf("%s/%s", invocation.FunctionNamespace, invocation.FunctionName)
			klog.Infof("Registering new dependency: %s", key)
			window := rolling.NewWindow(int(options.WinSize / options.WinGran))
			policy := rolling.NewTimePolicy(window, options.BuckDur)
			dw.Windows.Store(key, policy)
			dw.InvocationIDs.Store(key, invocation.EdgeId)
		}
	}

	return dw
}

func (dw *DAGWindows) recordResponseTime(key string, ms int64) error {
	m, ok := dw.Windows.Load(key)
	if !ok {
		return fmt.Errorf("could not append time for key: %s", key)
	}
	m.(*rolling.TimePolicy).Append(float64(ms))
	return nil
}

func (dw *DAGWindows) getExternalResponeTime() float64 {

	avgEdgeRTs := make(map[int]float64)

	dw.InvocationIDs.Range(func(key, value interface{}) (ret bool) {

		ret = true
		fn := key.(string)
		id := value.(int)

		window, ok := dw.Windows.Load(fn)
		if !ok {
			klog.Infof("no window for function %s", key)
			avgEdgeRTs[id] = 0
			return
		}

		w := window.(*rolling.TimePolicy)
		avg := w.Reduce(rolling.Avg)
		if val, ok := avgEdgeRTs[id]; ok {
			if avg > val {
				avgEdgeRTs[id] = avg
			}
		} else {
			avgEdgeRTs[id] = avg
		}

		return
	})

	sum := float64(0)
	for _, v := range avgEdgeRTs {
		sum += v
	}
	return sum
}
