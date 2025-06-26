package dependencycontroller

import (
	"fmt"
	"time"

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

type Controller struct {
	customClientset generatedclientset.Interface
	listers         informers.Listers
	informersSynced cache.InformerSynced
	// kubernetesCLientset is the client-go of kubernetes
	kubernetesClientset kubernetes.Interface
	// status represents the state of the controller
	status       *Status
	SharedStatus *SharedStatus
	// MetricClient is a client that polls the metrics from the pod.
	MetricClient     metricsgetter.MetricGetter
	depdagsWorkQueue queue.Queue
	// recorder is an event recorder for recording Event resources to the Kubernetes API.
	recorder record.EventRecorder
}

func NewController(
	kubernetesClientset kubernetes.Interface,
	podScalesClientset generatedclientset.Interface,
	metricsClient metricsgetter.MetricGetter,
	informers informers.Informers,
) *Controller {

	utilruntime.Must(samplescheme.AddToScheme(scheme.Scheme))
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	// Create Controller status
	status := &Status{
		graphMap: *concurrent.NewMap(),
	}
	shared := &SharedStatus{
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
		status:              status,
		SharedStatus:        shared,
		MetricClient:        metricsClient,
		depdagsWorkQueue:    queue.NewQueue("DependencyGraphsQueue"),
	}

	klog.Info("Setting up event handlers")
	informers.DependencyGraph.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleDependencyGraphAdd,
		UpdateFunc: controller.handleDependencyGraphUpdate,
		DeleteFunc: controller.handleDependencyGraphDelete,
	})

	return controller
}

func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting dependency graph controller")

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.informersSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting dependency graph workers")
	// Launch the workers to process dependency graph resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorkerSync, time.Second, stopCh)
	}

	// lunch worker for aggregating the times
	go wait.Until(c.aggregateGraphTimes, 5*time.Second, stopCh)
	klog.Info("Started dependency graph workers")

	return nil
}

func (c *Controller) runWorkerSync() {
	for c.depdagsWorkQueue.ProcessNextItem(c.syncDependencyGraph) {
	}
}

func (c *Controller) Shutdown() {
	utilruntime.HandleCrash()
	c.depdagsWorkQueue.ShutDown()
}
