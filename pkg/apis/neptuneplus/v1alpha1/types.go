package v1alpha1

// +kubebuilder:object:generate=true

import (
	sa "github.com/lterrac/system-autoscaler/pkg/apis/systemautoscaler/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type InvocationEdge struct {
	// FunctionName is the name of the invoked function, used as a pod/service selector. It should match the function name in another node in the graph.
	FunctionName string `json:"functionName"`
	// FunctionNamespace is the namespace of the invoked function
	FunctionNamespace string `json:"functionNamespace"`
	// Id of the invocation. Edges with the same id are invoked concurrently, different ids imply the invocations happen sequentially.
	EdgeId int `json:"edgeId"`
	// Multiplier describes how many invocations to this function are performed by the caller function.
	EdgeMultiplier int `json:"edgeMultiplier"`
}

type FunctionNode struct {
	// FunctionName represents what function this node is assigned to and it is used as a selector for the pods running said function.
	FunctionName string `json:"functionName"`
	// FunctionNamespace is the namespace of the invoked function
	FunctionNamespace string `json:"functionNamespace"`
	// Invocations is the list of out-edges from the node to invoked functions.
	Invocations []InvocationEdge `json:"invocations"`
}

// DependencyGraphSpec defines the desired state of DependencyGraph.
type DependencyGraphSpec struct {
	// Nodes represents the collection of nodes in the graph
	Nodes []FunctionNode `json:"nodes"`
}

type NodeStatus struct {
	// FunctionName is the name of the function
	FunctionName string `json:"functionName"`
	// FunctionNamespace is the namespace of the function
	FunctionNamespace string `json:"functionNamespace"`
	// ExternalResponseTime is the aggregated metric describing the average response time of the function's dependencies
	ExternalResponseTime int64 `json:"externalResponseTime"`
}

// DependencyGraphStatus defines the observed state of DependencyGraph.
type DependencyGraphStatus struct {
	Nodes []NodeStatus `json:"nodes"`
}

// DependencyGraph is the Schema for the dependencygraphs API.
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:object:generate=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type DependencyGraph struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	Spec DependencyGraphSpec `json:"spec,omitempty"`
	// +kubebuilder:validation:Required
	Status DependencyGraphStatus `json:"status,omitempty"`
}

// DependencyGraphList is a list of DependencyGraph
// +kubebuilder:object:root=true
// +kubebuilder:object:generate=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// DependencyGraphList is a list of DependencyGraph resources
type DependencyGraphList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []DependencyGraph `json:"items"`
}

const (
	DependencyAware sa.RecommendLogic = "dependencyAware"
)
