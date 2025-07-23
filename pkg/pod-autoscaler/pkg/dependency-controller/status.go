package dependencycontroller

import (
	"context"

	np "github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

func (c *Controller) updateGraphStatus(key string, nodes []np.FunctionNode) {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Error("[N+] Key couldn't be split in namespace/name pair")
		return
	}

	dag, err := c.listers.DependencyGraphs(namespace).Get(name)
	if err != nil {
		klog.Errorf("[N+] Could not find dependency graph %s:%s to update", namespace, name)
		return
	}

	statusNodes := make([]np.NodeStatus, len(nodes))
	for i, node := range nodes {

		key := MakeNamespaceNameKey(node.FunctionNamespace, node.FunctionName)
		ert, ok := c.SharedStatus.ExternalResponseTime(key)
		if !ok {
			ert = *resource.NewMilliQuantity(-1, resource.BinarySI)
		}

		statusNodes[i] = np.NodeStatus{
			FunctionNamespace:    node.FunctionName,
			FunctionName:         node.FunctionName,
			ExternalResponseTime: ert.MilliValue(),
		}
	}

	newDag := dag.DeepCopy()

	newDag.Status.Nodes = statusNodes
	_, err = c.customClientset.NeptuneplusV1alpha1().DependencyGraphs(namespace).UpdateStatus(context.TODO(), newDag, metav1.UpdateOptions{})
	if err != nil {
		klog.Error(err)
		return
	}
}
