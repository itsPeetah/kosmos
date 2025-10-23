// Derived from pkg/podscale-controller/pkg/controller/sync.go

package dependencycontroller

import (
	"context"
	"fmt"

	np "github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

// syncDependencyGraph handles the tracking of dependency graphs that exist in the system
func (c *Controller) syncDependencyGraph(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("[N+] invalid resource key: %s", key))
		return nil
	}

	dagKey := MakeNamespaceNameKey(namespace, name)
	dag, err := c.listers.DependencyGraphs(namespace).Get(name)

	klog.Infof("[N+] Syncinc dependency graph %s", dagKey)

	if err != nil {
		if errors.IsNotFound(err) {
			klog.Errorf("[N+] dependency graph was deleted (%s)", dagKey)
		} else {
			klog.Errorf("[N+] error while syncinc dependency graph %s", dagKey)
		}

		c.Status.DependencyGraphs.Delete(dagKey)
		return nil
	}

	klog.Info("sorting nodes")

	// Store the nodes in the map (overwriting old ones if already present)
	nodesSorted := sortNodesByDependencies(dag.Spec.Nodes)
	c.Status.DependencyGraphs.Store(dagKey, nodesSorted)

	// Store the nominal response times
	for _, node := range dag.Spec.Nodes {
		c.Status.NLRTsMap.Store(MakeNamespaceNameKey(node.FunctionNamespace, node.FunctionName), node.NominalLocalResponseTime.MilliValue())
	}

	c.recorder.Event(dag, corev1.EventTypeNormal, "Synced", fmt.Sprintf("[N+] Dependency graph %s synced successfully", dagKey))
	return nil
}

func (c *Controller) updateGraphStatus(key string, nodes []np.FunctionNode) {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		klog.Error("[N+ (Status)] Key couldn't be split in namespace/name pair")
		return
	}

	dag, err := c.listers.DependencyGraphs(namespace).Get(name)
	if err != nil {
		klog.Errorf("[N+ (Status)] Could not find dependency graph %s:%s to update", namespace, name)
		return
	}

	statusNodes := make([]np.NodeStatus, len(nodes))
	for i, node := range nodes {

		key := fmt.Sprintf("%s/%s", node.FunctionNamespace, node.FunctionName)
		ert, ok := c.Status.ExternalResponseTime(key)
		if !ok {
			ert = -1
		}

		statusNodes[i] = np.NodeStatus{
			FunctionNamespace:    node.FunctionNamespace,
			FunctionName:         node.FunctionName,
			ExternalResponseTime: ert,
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
