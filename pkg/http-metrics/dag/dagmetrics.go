package dag

import (
	"github.com/asecurityteam/rolling"
	"k8s.io/klog/v2"
)

func (dw *DAGWindows) GetExternalResponeTime() float64 {

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
