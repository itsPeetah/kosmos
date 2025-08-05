package dependencycontroller

import (
	"fmt"
	"time"

	"github.com/lterrac/system-autoscaler/pkg/apis/neptuneplus/v1alpha1"
	"github.com/lterrac/system-autoscaler/pkg/informers"
	metricsgetter "github.com/lterrac/system-autoscaler/pkg/pod-autoscaler/pkg/metrics"
	"github.com/lterrac/system-autoscaler/pkg/queue"
	"github.com/modern-go/concurrent"

	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"

	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	generatedclientset "github.com/lterrac/system-autoscaler/pkg/generated/clientset/versioned"
	samplescheme "github.com/lterrac/system-autoscaler/pkg/generated/clientset/versioned/scheme"
)

const controllerAgentName = "depdag-controller"

/*

TYPE DEFINITIONS + CONSTRUCTOR

*/

type Controller struct {
	customClientset generatedclientset.Interface
	listers         informers.Listers
	informersSynced cache.InformerSynced
	// kubernetesCLientset is the client-go of kubernetes
	kubernetesClientset kubernetes.Interface
	// status represents the state of the controller
	Status *Status
	// MetricClient is a client that polls the metrics from the pod.
	MetricClient     metricsgetter.MetricGetter
	depdagsWorkQueue queue.Queue
	// recorder is an event recorder for recording Event resources to the Kubernetes API.
	recorder record.EventRecorder
}

type Status struct {
	// Key: namespace:name of the graph, Value: nodes, sorted leaves-to-root
	DependencyGraphs *concurrent.Map
	// Key: namespace:name of the function, Value: computed external response time
	ExternalResponseTimesMap *concurrent.Map
	// Key: namespace:name of the function, Value: nominal response time as noted in the graph
	NominalResponseTimesMap *concurrent.Map
}

func NewController(
	kubernetesClientset kubernetes.Interface,
	podScalesClientset generatedclientset.Interface,
	metricsClient metricsgetter.MetricGetter,
	informers informers.Informers,
) *Controller {

	utilruntime.Must(samplescheme.AddToScheme(scheme.Scheme))
	klog.V(4).Info("[N+] Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	// Create Controller status
	status := &Status{
		DependencyGraphs:         concurrent.NewMap(),
		ExternalResponseTimesMap: concurrent.NewMap(),
		NominalResponseTimesMap:  concurrent.NewMap(),
	}

	// Instantiate the Controller
	controller := &Controller{
		customClientset:     podScalesClientset,
		listers:             informers.GetListers(),
		informersSynced:     informers.DependencyGraph.Informer().HasSynced,
		kubernetesClientset: kubernetesClientset,
		recorder:            recorder,
		Status:              status,
		MetricClient:        metricsClient,
		depdagsWorkQueue:    queue.NewQueue("DependencyGraphsQueue"),
	}

	klog.Info("[N+] Setting up event handlers")
	informers.DependencyGraph.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleDependencyGraphAdd,
		DeleteFunc: controller.handleDependencyGraphDelete,
		UpdateFunc: controller.handleDependencyGraphUpdate,
	})

	return controller
}

/*

CONTROLLER METHODS

*/

func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {

	// Start the informer factories to begin populating the informer caches
	klog.Info("[N+] Starting dependency graph controller")

	// Wait for the caches to be synced before starting workers
	klog.Info("[N+] Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.informersSynced); !ok {
		return fmt.Errorf("[N+] Failed to wait for caches to sync")
	}

	klog.Info("[N+] Starting dependency graph workers")
	// Launch the workers to process dependency graph resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorkerSync, time.Second, stopCh)
	}

	// go wait.Until(c.runDependencyGraphListerWorker, 5*time.Second, stopCh)
	go wait.Until(c.runAggregateGraphTimesWorker, 5*time.Second, stopCh)
	go wait.Until(c.runGraphStatusSync, 5*time.Second, stopCh)
	klog.Info("[N+] Started dependency graph workers")

	return nil
}

func (c *Controller) runWorkerSync() {
	klog.Info("[N+] Syncing dependency graphs")
	for c.depdagsWorkQueue.ProcessNextItem(c.syncDependencyGraph) {
	}
}

func (c *Controller) runAggregateGraphTimesWorker() {
	klog.Info("[N+] Aggregating external response times for all tracked dependency graphs")
	c.Status.DependencyGraphs.Range(func(key, value interface{}) bool {
		klog.Infof("[N+] Aggregating graph times for dependency graph %s", key)

		found, ok := c.Status.DependencyGraphs.Load(key)
		if !ok {
			klog.Errorf("[N+] Graph %s not found in controller map", key)
			return true
		}
		nodes, ok := found.([]v1alpha1.FunctionNode)
		if !ok {
			klog.Errorf("[N+] Error casting function node list for graph %s", key)
			return true
		}

		c.aggregateGraphTimes(nodes)
		return true
	})

}

func (c *Controller) Shutdown() {
	klog.Info("[N+] Shutting down Dependency Controller")
	utilruntime.HandleCrash()
	c.depdagsWorkQueue.ShutDown()
	klog.Info("[N+] Shut down Dependency Controller")
}

func (c *Controller) runGraphStatusSync() {
	c.Status.DependencyGraphs.Range(func(key, value interface{}) bool {
		keyAsStr, okKey := key.(string)
		valueAsNodes, okValue := value.([]v1alpha1.FunctionNode)
		if !okKey || !okValue {
			klog.Error("[N+] Non-parsable key/value pair in graph map. This should never happen.")
			return true // keep iterating
		}

		klog.Infof("[N+] Updating status for dependency graph %s", keyAsStr)
		c.updateGraphStatus(keyAsStr, valueAsNodes)
		return true
	})
}
