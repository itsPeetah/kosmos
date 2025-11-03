package dag

import (
	"github.com/asecurityteam/rolling"
)

func GetExternalResponeTime(dw *DAGWindows) int64 {

	avgEdgeRTs := make(map[int]int64)
	for key, invocationID := range dw.InvocationIDs {
		avg := dw.Windows[key].Reduce(rolling.Avg)
		if val, ok := avgEdgeRTs[invocationID]; ok {
			if int64(avg) > val {
				avgEdgeRTs[invocationID] = int64(avg)
			}
		} else {
			avgEdgeRTs[invocationID] = int64(avg)
		}
	}

	sum := int64(0)
	for _, v := range avgEdgeRTs {
		sum += v
	}
	return sum
}
