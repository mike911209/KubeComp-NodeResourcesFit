/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package noderesources

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

var _ framework.PreFilterPlugin = &MyNoderesourcesfit{}
var _ framework.FilterPlugin = &MyNoderesourcesfit{}
var _ framework.EnqueueExtensions = &MyNoderesourcesfit{}
var _ framework.PreScorePlugin = &MyNoderesourcesfit{}
var _ framework.ScorePlugin = &MyNoderesourcesfit{}
var _ framework.PermitPlugin = &MyNoderesourcesfit{}

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = "MyNoderesourcesfit"

	// preFilterStateKey is the key in CycleState to NodeResourcesFit pre-computed data.
	// Using the name of the plugin will likely help us avoid collisions with other plugins.
	preFilterStateKey = "PreFilter" + Name

	// preScoreStateKey is the key in CycleState to NodeResourcesFit pre-computed data for Scoring.
	preScoreStateKey = "PreScore" + Name

	filterStateKey       = "Filter" + Name
	targetPodLabel       = "targetPod"
	targetNamespaceLabel = "targetNamespace"

	migResourcePrefix = "mig"
	devicePrefix      = "kubecomp/status-gpu-"
	gpuCountLabel     = "gpu-count"
	maxLabelSuffix    = "-max-" + migResourcePrefix
)

type patchStringValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

type gpuRequest struct {
	CPU    int64
	Memory int64
}

// InsufficientResource describes what kind of resource limit is hit and caused the pod to not fit the node.
type InsufficientResource struct {
	ResourceName v1.ResourceName
	// We explicitly have a parameter for reason to avoid formatting a message on the fly
	// for common resources, which is expensive for cluster autoscaler simulations.
	Reason    string
	Requested int64
	Used      int64
	Capacity  int64
}

// nodeResourceStrategyTypeMap maps strategy to scorer implementation
var nodeResourceStrategyTypeMap = map[config.ScoringStrategyType]scorer{
	config.LeastAllocated: func(args *config.NodeResourcesFitArgs) *resourceAllocationScorer {
		resources := args.ScoringStrategy.Resources
		return &resourceAllocationScorer{
			Name:      string(config.LeastAllocated),
			scorer:    leastResourceScorer(resources),
			resources: resources,
		}
	},
	config.MostAllocated: func(args *config.NodeResourcesFitArgs) *resourceAllocationScorer {
		resources := args.ScoringStrategy.Resources
		return &resourceAllocationScorer{
			Name:      string(config.MostAllocated),
			scorer:    mostResourceScorer(resources),
			resources: resources,
		}
	},
	config.RequestedToCapacityRatio: func(args *config.NodeResourcesFitArgs) *resourceAllocationScorer {
		resources := args.ScoringStrategy.Resources
		return &resourceAllocationScorer{
			Name:      string(config.RequestedToCapacityRatio),
			scorer:    requestedToCapacityRatioScorer(resources, args.ScoringStrategy.RequestedToCapacityRatio.Shape),
			resources: resources,
		}
	},
}

// Fit is a plugin that checks if a node has sufficient resources.
type MyNoderesourcesfit struct {
	ignoredResources                sets.Set[string]
	ignoredResourceGroups           sets.Set[string]
	enableInPlacePodVerticalScaling bool
	enableSidecarContainers         bool
	handle                          framework.Handle
	resourceAllocationScorer
}

// preFilterState computed at PreFilter and used at Filter.
type preFilterState struct {
	framework.Resource
}

type FilterState struct {
	// the node that was labeled -> score plugin should give the highest score
	nodeName   string
	toReconfig bool
}

// preScoreState computed at PreScore and used at Score.
type preScoreState struct {
	// podRequests have the same order as the resources defined in NodeResourcesBalancedAllocationArgs.Resources,
	// same for other place we store a list like that.
	podRequests []int64
}

// Clone the prefilter state.
func (s *preFilterState) Clone() framework.StateData {
	return s
}

// Clone the filter state
func (s *FilterState) Clone() framework.StateData {
	return s
}

// Clone implements the mandatory Clone interface. We don't really copy the data since
// there is no need for that.
func (s *preScoreState) Clone() framework.StateData {
	return s
}

// Name returns name of the plugin. It is used in logs, etc.
func (f *MyNoderesourcesfit) Name() string {
	return Name
}

// NewFit initializes a new plugin and returns it.
func NewFit(_ context.Context, plArgs runtime.Object, h framework.Handle) (framework.Plugin, error) {
	var fitArgs config.NodeResourcesFitArgs
	if plArgs != nil {
		args := plArgs.(*runtime.Unknown)
		if err := json.Unmarshal(args.Raw, &fitArgs); err != nil {
			fmt.Print("Error unmarshal: %v\n", err)
		}
	}

	if err := validation.ValidateNodeResourcesFitArgs(nil, &fitArgs); err != nil {
		return nil, err
	}

	if fitArgs.ScoringStrategy == nil {
		return nil, fmt.Errorf("scoring strategy not specified")
	}

	strategy := fitArgs.ScoringStrategy.Type
	scorePlugin, exists := nodeResourceStrategyTypeMap[strategy]
	if !exists {
		return nil, fmt.Errorf("scoring strategy %s is not supported", strategy)
	}

	return &MyNoderesourcesfit{
		ignoredResources:                sets.New(fitArgs.IgnoredResources...),
		ignoredResourceGroups:           sets.New(fitArgs.IgnoredResourceGroups...),
		enableInPlacePodVerticalScaling: true,
		enableSidecarContainers:         false,
		handle:                          h,
		resourceAllocationScorer:        *scorePlugin(&fitArgs),
	}, nil
}

// EventsToRegister returns the possible events that may make a Pod
// failed by this plugin schedulable.
func (f *MyNoderesourcesfit) EventsToRegister() []framework.ClusterEventWithHint {
	podActionType := framework.Delete
	if f.enableInPlacePodVerticalScaling {
		// If InPlacePodVerticalScaling (KEP 1287) is enabled, then PodUpdate event should be registered
		// for this plugin since a Pod update may free up resources that make other Pods schedulable.
		podActionType |= framework.Update
	}
	return []framework.ClusterEventWithHint{
		{Event: framework.ClusterEvent{Resource: framework.Pod, ActionType: podActionType}, QueueingHintFn: f.isSchedulableAfterPodChange},
		{Event: framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Add | framework.Update}, QueueingHintFn: f.isSchedulableAfterNodeChange},
	}
}

// isSchedulableAfterPodChange is invoked whenever a pod deleted or updated. It checks whether
// that change made a previously unschedulable pod schedulable.
func (f *MyNoderesourcesfit) isSchedulableAfterPodChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	originalPod, modifiedPod, err := schedutil.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return framework.Queue, err
	}

	if modifiedPod == nil {
		if originalPod.Spec.NodeName == "" {
			logger.V(5).Info("the deleted pod was unscheduled and it wouldn't make the unscheduled pod schedulable", "pod", klog.KObj(pod), "deletedPod", klog.KObj(originalPod))
			return framework.QueueSkip, nil
		}
		logger.V(5).Info("another scheduled pod was deleted, and it may make the unscheduled pod schedulable", "pod", klog.KObj(pod), "deletedPod", klog.KObj(originalPod))
		return framework.Queue, nil
	}

	if !f.enableInPlacePodVerticalScaling {
		// If InPlacePodVerticalScaling (KEP 1287) is disabled, it cannot free up resources.
		logger.V(5).Info("another pod was modified, but InPlacePodVerticalScaling is disabled, so it doesn't make the unscheduled pod schedulable", "pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
		return framework.QueueSkip, nil
	}

	// Modifications may or may not be relevant. We only care about modifications that
	// change the other pod's resource request and the resource is also requested by the
	// pod we are trying to schedule.
	if !f.isResourceScaleDown(pod, originalPod, modifiedPod) {
		if loggerV := logger.V(10); loggerV.Enabled() {
			// Log more information.
			loggerV.Info("another Pod got modified, but the modification isn't related to the resource request", "pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod), "diff", cmp.Diff(originalPod, modifiedPod))
		} else {
			logger.V(5).Info("another Pod got modified, but the modification isn't related to the resource request", "pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
		}
		return framework.QueueSkip, nil
	}

	logger.V(5).Info("the max request resources of another scheduled pod got reduced and it may make the unscheduled pod schedulable", "pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
	return framework.Queue, nil
}

// isResourceScaleDown checks whether an update event may make the pod schedulable. Specifically:
// - Returns true when an update event shows a scheduled pod's resource request got reduced.
// - Returns true when an update event is for the unscheduled pod itself, and it shows the pod's resource request got reduced.
func (f *MyNoderesourcesfit) isResourceScaleDown(targetPod, originalPod, modifiedPod *v1.Pod) bool {
	if modifiedPod.UID != targetPod.UID && modifiedPod.Spec.NodeName == "" {
		// If the update event is not for targetPod and a scheduled Pod,
		// it wouldn't make targetPod schedulable.
		return false
	}

	// the other pod was scheduled, so modification or deletion may free up some resources.
	originalMaxResourceReq, modifiedMaxResourceReq := &framework.Resource{}, &framework.Resource{}
	originalMaxResourceReq.SetMaxResource(resource.PodRequests(originalPod, resource.PodResourcesOptions{InPlacePodVerticalScalingEnabled: f.enableInPlacePodVerticalScaling}))
	modifiedMaxResourceReq.SetMaxResource(resource.PodRequests(modifiedPod, resource.PodResourcesOptions{InPlacePodVerticalScalingEnabled: f.enableInPlacePodVerticalScaling}))

	// check whether the resource request of the modified pod is less than the original pod.
	podRequests := resource.PodRequests(targetPod, resource.PodResourcesOptions{InPlacePodVerticalScalingEnabled: f.enableInPlacePodVerticalScaling})
	for rName, rValue := range podRequests {
		if rValue.IsZero() {
			// We only care about the resources requested by the pod we are trying to schedule.
			continue
		}
		switch rName {
		case v1.ResourceCPU:
			if originalMaxResourceReq.MilliCPU > modifiedMaxResourceReq.MilliCPU {
				return true
			}
		case v1.ResourceMemory:
			if originalMaxResourceReq.Memory > modifiedMaxResourceReq.Memory {
				return true
			}
		case v1.ResourceEphemeralStorage:
			if originalMaxResourceReq.EphemeralStorage > modifiedMaxResourceReq.EphemeralStorage {
				return true
			}
		default:
			if schedutil.IsScalarResourceName(rName) && originalMaxResourceReq.ScalarResources[rName] > modifiedMaxResourceReq.ScalarResources[rName] {
				return true
			}
		}
	}
	return false
}

// isSchedulableAfterNodeChange is invoked whenever a node added or changed. It checks whether
// that change made a previously unschedulable pod schedulable.
func (f *MyNoderesourcesfit) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	_, modifiedNode, err := schedutil.As[*v1.Node](oldObj, newObj)
	if err != nil {
		return framework.Queue, err
	}
	// TODO: also check if the original node meets the pod's resource requestments once preCheck is completely removed.
	// See: https://github.com/kubernetes/kubernetes/issues/110175
	if isFit(pod, modifiedNode) {
		logger.V(5).Info("node was updated, and may fit with the pod's resource requestments", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return framework.Queue, nil
	}

	logger.V(5).Info("node was created or updated, but it doesn't have enough resource(s) to accommodate this pod", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
	return framework.QueueSkip, nil
}

// isFit checks if the pod fits the node. If the node is nil, it returns false.
// It constructs a fake NodeInfo object for the node and checks if the pod fits the node.
func isFit(pod *v1.Pod, node *v1.Node) bool {
	if node == nil {
		return false
	}
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	return len(Fits(pod, nodeInfo)) == 0
}

// computePodResourceRequest returns a framework.Resource that covers the largest
// width in each resource dimension. Because init-containers run sequentially, we collect
// the max in each dimension iteratively. In contrast, we sum the resource vectors for
// regular containers since they run simultaneously.
//
// # The resources defined for Overhead should be added to the calculated Resource request sum
//
// Example:
//
// Pod:
//
//	InitContainers
//	  IC1:
//	    CPU: 2
//	    Memory: 1G
//	  IC2:
//	    CPU: 2
//	    Memory: 3G
//	Containers
//	  C1:
//	    CPU: 2
//	    Memory: 1G
//	  C2:
//	    CPU: 1
//	    Memory: 1G
//
// Result: CPU: 3, Memory: 3G
func computePodResourceRequest(pod *v1.Pod) *preFilterState {
	// pod hasn't scheduled yet so we don't need to worry about InPlacePodVerticalScalingEnabled
	reqs := resource.PodRequests(pod, resource.PodResourcesOptions{})
	result := &preFilterState{}
	result.SetMaxResource(reqs)
	return result
}

func hasRestartableInitContainer(pod *v1.Pod) bool {
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyAlways {
			return true
		}
	}
	return false
}

// PreFilter invoked at the prefilter extension point.
func (f *MyNoderesourcesfit) PreFilter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	log.Printf("Pod %s is in Prefilter phase.", pod.Name)

	if pod.ObjectMeta.Labels["preprocess"] != "done" {
		return nil, framework.NewStatus(framework.Unschedulable, "preprocess not done yet")
	}

	if !f.enableSidecarContainers && hasRestartableInitContainer(pod) {
		// Scheduler will calculate resources usage for a Pod containing
		// restartable init containers that will be equal or more than kubelet will
		// require to run the Pod. So there will be no overbooking. However, to
		// avoid the inconsistency in resource calculation between the scheduler
		// and the older (before v1.28) kubelet, make the Pod unschedulable.
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, "Pod has a restartable init container and the SidecarContainers feature is disabled")
	}
	cycleState.Write(preFilterStateKey, computePodResourceRequest(pod))
	return nil, nil
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (f *MyNoderesourcesfit) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func getPreFilterState(cycleState *framework.CycleState) (*preFilterState, error) {
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preFilterStateKey, err)
	}

	s, ok := c.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to NodeResourcesFit.preFilterState error", c)
	}
	return s, nil
}

// Filter invoked at the filter extension point.
// Checks if a node has sufficient resources, such as cpu, memory, gpu, opaque int resources etc to run a pod.
// It returns a list of insufficient resources, if empty, then the node has all the resources requested by the pod.
func (f *MyNoderesourcesfit) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	s, err := getPreFilterState(cycleState)
	if err != nil {
		return framework.AsStatus(err)
	}

	insufficientResources := fitsRequest(s, nodeInfo, f.ignoredResources, f.ignoredResourceGroups, cycleState)

	if len(insufficientResources) != 0 {
		// We will keep all failure reasons.
		failureReasons := make([]string, 0, len(insufficientResources))
		for i := range insufficientResources {
			failureReasons = append(failureReasons, insufficientResources[i].Reason)
		}
		return framework.NewStatus(framework.Unschedulable, failureReasons...)
	}
	return nil
}

// Fits checks if node have enough resources to host the pod.
func Fits(pod *v1.Pod, nodeInfo *framework.NodeInfo) []InsufficientResource {
	return fitsRequest(computePodResourceRequest(pod), nodeInfo, nil, nil, nil)
}

func fitsRequest(podRequest *preFilterState, nodeInfo *framework.NodeInfo, ignoredExtendedResources, ignoredResourceGroups sets.Set[string], cycleState *framework.CycleState) []InsufficientResource {
	log.Printf("Checking node: %s", nodeInfo.Node().Name)

	insufficientResources := make([]InsufficientResource, 0, 4)

	allowedPodNumber := nodeInfo.Allocatable.AllowedPodNumber
	if len(nodeInfo.Pods)+1 > allowedPodNumber {
		insufficientResources = append(insufficientResources, InsufficientResource{
			ResourceName: v1.ResourcePods,
			Reason:       "Too many pods",
			Requested:    1,
			Used:         int64(len(nodeInfo.Pods)),
			Capacity:     int64(allowedPodNumber),
		})
	}

	if podRequest.MilliCPU == 0 &&
		podRequest.Memory == 0 &&
		podRequest.EphemeralStorage == 0 &&
		len(podRequest.ScalarResources) == 0 {
		return insufficientResources
	}

	if podRequest.MilliCPU > 0 && podRequest.MilliCPU > (nodeInfo.Allocatable.MilliCPU-nodeInfo.Requested.MilliCPU) {
		insufficientResources = append(insufficientResources, InsufficientResource{
			ResourceName: v1.ResourceCPU,
			Reason:       "Insufficient cpu",
			Requested:    podRequest.MilliCPU,
			Used:         nodeInfo.Requested.MilliCPU,
			Capacity:     nodeInfo.Allocatable.MilliCPU,
		})
	}
	if podRequest.Memory > 0 && podRequest.Memory > (nodeInfo.Allocatable.Memory-nodeInfo.Requested.Memory) {
		insufficientResources = append(insufficientResources, InsufficientResource{
			ResourceName: v1.ResourceMemory,
			Reason:       "Insufficient memory",
			Requested:    podRequest.Memory,
			Used:         nodeInfo.Requested.Memory,
			Capacity:     nodeInfo.Allocatable.Memory,
		})
	}
	if podRequest.EphemeralStorage > 0 &&
		podRequest.EphemeralStorage > (nodeInfo.Allocatable.EphemeralStorage-nodeInfo.Requested.EphemeralStorage) {
		insufficientResources = append(insufficientResources, InsufficientResource{
			ResourceName: v1.ResourceEphemeralStorage,
			Reason:       "Insufficient ephemeral-storage",
			Requested:    podRequest.EphemeralStorage,
			Used:         nodeInfo.Requested.EphemeralStorage,
			Capacity:     nodeInfo.Allocatable.EphemeralStorage,
		})
	}

	useMig := false
	gpuRequest := gpuRequest{CPU: 0, Memory: 0}
	gpuRequestMap := make(map[string]int64)

	for rName, rQuant := range podRequest.ScalarResources {
		// Skip in case request quantity is zero
		if rQuant == 0 {
			continue
		}

		rNameSplit := strings.Split(string(rName), "/")

		if v1helper.IsExtendedResourceName(rName) {
			// If this resource is one of the extended resources that should be ignored, we will skip checking it.
			// rName is guaranteed to have a slash due to API validation.
			var rNamePrefix string
			if ignoredResourceGroups.Len() > 0 {
				rNamePrefix = rNameSplit[0]
			}
			if ignoredExtendedResources.Has(string(rName)) || ignoredResourceGroups.Has(rNamePrefix) {
				continue
			}
		}

		// check if using MIG resources
		if rNameSplit[1][:len(migResourcePrefix)] == migResourcePrefix {
			useMig = true
			// requestSlice is of the pattern like: 1g.5gb
			requestSlice := rNameSplit[1][len(migResourcePrefix)+1:]
			gpuRequestMap[requestSlice] += rQuant
			cpu, memory, err := extractCPUAndMemory(requestSlice)
			if err != nil {
				log.Printf("Error extracting CPU and Memory from %s", rName)
				continue
			}
			gpuRequest.CPU += cpu
			gpuRequest.Memory += memory
			// log.Printf("Pod request cpu: %d, mem: %d", gpuRequest.CPU, gpuRequest.Memory)
			continue
		}

		if rQuant > (nodeInfo.Allocatable.ScalarResources[rName] - nodeInfo.Requested.ScalarResources[rName]) {
			insufficientResources = append(insufficientResources, InsufficientResource{
				ResourceName: rName,
				Reason:       fmt.Sprintf("Insufficient %v", rName),
				Requested:    podRequest.ScalarResources[rName],
				Used:         nodeInfo.Requested.ScalarResources[rName],
				Capacity:     nodeInfo.Allocatable.ScalarResources[rName],
			})
		}
	}

	sufficientGPU := false
	toReconfig := true

	if useMig {
		totalGPUStr, exist := nodeInfo.Node().Labels[gpuCountLabel]

		if !exist {
			insufficientResources = append(insufficientResources, InsufficientResource{
				ResourceName: "gpu",
				Reason:       fmt.Sprintf("Node %s doesn't have mig resources", nodeInfo.Node().Name),
				Requested:    0,
				Used:         0,
				Capacity:     0,
			})
			return insufficientResources
		}

		totalGPU, _ := strconv.ParseInt(totalGPUStr, 10, 64)
		if totalGPU == 0 {
			insufficientResources = append(insufficientResources, InsufficientResource{
				ResourceName: "gpu",
				Reason:       fmt.Sprintf("Node %s doesn't have mig resources", nodeInfo.Node().Name),
				Requested:    0,
				Used:         0,
				Capacity:     0,
			})
			return insufficientResources
		}

		// obtain current gpu usage
		// & check whether there exist a gpu that the pod can be put into
		for i := range totalGPU {
			log.Printf("Checking GPU %d on node %s", i, nodeInfo.Node().Name)

			// check for remaining resources
			gpuID := devicePrefix + strconv.Itoa(int(i))
			maxLabel := gpuID + maxLabelSuffix

			// maxSlice is of the pattern like: 1g.5gb
			maxSlice, exist := nodeInfo.Node().Labels[maxLabel]
			cpuRemain, memRemain, err := extractCPUAndMemory(maxSlice)
			if err != nil {
				log.Printf("Error extracting CPU and Memory from %s", maxSlice)
				continue
			}

			log.Printf("GPU %d on node %s has remaining resources: CPU: %d, Memory: %d", i, nodeInfo.Node().Name, cpuRemain, memRemain)

			// check if the remaining resources >= pod's request
			if exist && cpuRemain >= gpuRequest.CPU && memRemain >= gpuRequest.Memory {
				sufficientGPU = true
				for request, num := range gpuRequestMap {
					// check if there exist resources just match request
					resourceCountStr := nodeInfo.Node().Labels[gpuID+"-"+request+"-free"]
					resourceCount, _ := strconv.ParseInt(resourceCountStr, 10, 64)
					// log.Printf("GPU %d has %d resources for %s", i, resourceCount, request)
					if resourceCount >= num {
						toReconfig = false
					}
				}
				log.Printf("GPU %d has enough resources to accomodate %s, reconfig: %t", i, podRequest, toReconfig)
				if !toReconfig {
					log.Printf("GPU %d no need to reconfig", i)
					// there exist a gpu that can run pod directly -> break
					break
				}
			}
		}

		if !sufficientGPU {
			insufficientResources = append(insufficientResources, InsufficientResource{
				ResourceName: "mig",
				Reason:       fmt.Sprintf("Insufficient %v", "mig"),
				Requested:    0,
				Used:         0,
				Capacity:     0,
			})
		}

		if toReconfig {
			filterState := &FilterState{nodeName: nodeInfo.Node().Name, toReconfig: true}
			key := framework.StateKey(filterStateKey + nodeInfo.Node().Name)
			cycleState.Write(key, filterState)
		} else {
			filterState := &FilterState{nodeName: nodeInfo.Node().Name, toReconfig: false}
			key := framework.StateKey(filterStateKey + nodeInfo.Node().Name)
			cycleState.Write(key, filterState)
		}
	}

	return insufficientResources
}

// input should be like: 1g.5gb, 3g.20gb, ...
func extractCPUAndMemory(rName string) (int64, int64, error) {
	cpu, err1 := strconv.ParseInt(string(rName[0]), 10, 64)
	if err1 != nil {
		return 0, 0, err1
	}
	memory, err2 := strconv.ParseInt(string(rName[3:len(rName)-2]), 10, 64)
	if err2 != nil {
		return 0, 0, err2
	}
	return cpu, memory, nil
}

// PreScore calculates incoming pod's resource requests and writes them to the cycle state used.
func (f *MyNoderesourcesfit) PreScore(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodes []*framework.NodeInfo) *framework.Status {
	state := &preScoreState{
		podRequests: f.calculatePodResourceRequestList(pod, f.resources),
	}
	cycleState.Write(preScoreStateKey, state)
	return nil
}

func getPreScoreState(cycleState *framework.CycleState) (*preScoreState, error) {
	c, err := cycleState.Read(preScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("reading %q from cycleState: %w", preScoreStateKey, err)
	}

	s, ok := c.(*preScoreState)
	if !ok {
		return nil, fmt.Errorf("invalid PreScore state, got type %T", c)
	}
	return s, nil
}

// Score invoked at the Score extension point.
func (f *MyNoderesourcesfit) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodeInfo, err := f.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.AsStatus(fmt.Errorf("getting node %q from Snapshot: %w", nodeName, err))
	}

	s, err := getPreScoreState(state)
	if err != nil {
		s = &preScoreState{
			podRequests: f.calculatePodResourceRequestList(pod, f.resources),
		}
	}
	score, status := f.score(ctx, pod, nodeInfo, s.podRequests)

	filterState, err := getFilterState(state, nodeName)
	if err == nil {
		log.Printf("Pod %s is in Score phase", pod.Name)
		log.Printf("node %s, reconfigure: ", nodeName, filterState.toReconfig)

		if !filterState.toReconfig {
			score += framework.MaxNodeScore
		}
	}

	log.Printf("Node %s, score: %d", nodeName, score)

	return score, status
}

// ScoreExtensions of the Score plugin.
func (f *MyNoderesourcesfit) ScoreExtensions() framework.ScoreExtensions {
	return f
}

// NormalizeScore : normalize scores since lower scores correspond to lower latency
func (cs *MyNoderesourcesfit) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	log.Printf("Pod %s is in NormalizeScore phase", pod.Name)
	log.Printf("before normalization: %v", scores)

	// Get Min and Max Scores to normalize between framework.MaxNodeScore and framework.MinNodeScore
	minScore, maxScore := getMinMaxScores(scores)

	// If all nodes were given the minimum score, return
	if minScore == 0 && maxScore == 0 {
		return nil
	}

	var normCost float64
	for i := range scores {
		if maxScore != minScore { // If max != min
			normCost = float64(framework.MaxNodeScore) * float64(scores[i].Score-minScore) / float64(maxScore-minScore)
			scores[i].Score = framework.MinNodeScore + int64(normCost)
		} else { // If maxCost = minCost, avoid division by 0
			scores[i].Score = framework.MinNodeScore
		}
	}
	log.Printf("after normalization: %v", scores)
	return nil
}

// MinMax : get min and max scores from NodeScoreList
func getMinMaxScores(scores framework.NodeScoreList) (int64, int64) {
	var max int64 = math.MinInt64 // Set to min value
	var min int64 = math.MaxInt64 // Set to max value

	for _, nodeScore := range scores {
		if nodeScore.Score > max {
			max = nodeScore.Score
		}
		if nodeScore.Score < min {
			min = nodeScore.Score
		}
	}
	// return min and max scores
	return min, max
}

func (cs *MyNoderesourcesfit) Permit(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (*framework.Status, time.Duration) {
	log.Printf("Pod %s is in Permit phase", p.Name)

	// read out the cycle state
	filterState, err := getFilterState(state, nodeName)
	if err != nil {
		return framework.NewStatus(framework.Success, "pod %s not using mig resources, pass permit!", p.Name), 0
	}

	if filterState.toReconfig {
		// reconfigure the node
		log.Printf("Updating %s's label to targetPod and targetNamespace", nodeName)

		// update the node's label to targetPod and targetNamespace
		node, err := cs.handle.ClientSet().CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
		if err != nil {
			log.Print(err)
			return framework.NewStatus(framework.Error, "Error getting node"), 0
		}
		node.Labels[targetPodLabel] = p.Name
		node.Labels[targetNamespaceLabel] = p.Namespace
		_, err = cs.handle.ClientSet().CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{})
		if err != nil {
			log.Print(err)
			return framework.NewStatus(framework.Error, "Error updating node labels"), 0
		}

		// Only update pod's expectedNode label after node was successfully label to be reconfigured
		labelPatch := []patchStringValue{{
			Op:    "replace",
			Path:  "/metadata/labels/expectedNode",
			Value: nodeName,
		}}

		labelPatchBytes, _ := json.Marshal(labelPatch)
		_, err = cs.handle.ClientSet().CoreV1().Pods(p.Namespace).Patch(context.TODO(), p.Name, types.JSONPatchType, labelPatchBytes, metav1.PatchOptions{})

		if err != nil {
			log.Print(err)
			return framework.NewStatus(framework.Error, "Error updating pod labels"), 0
		}

		return framework.NewStatus(framework.Unschedulable, "Wait for reconfigure"), 0
	}

	// the pod can be bind directly
	// read expectednode
	log.Printf("looking for pod: %s's expectedNode labels", p.Name)
	expectedNode, exist := p.ObjectMeta.Labels["expectedNode"]
	if exist {
		if expectedNode != nodeName {
			log.Printf("Pod %s is not expected to be scheduled on node %s", p.Name, nodeName)
			return framework.NewStatus(framework.Unschedulable, "unexpected node"), 0
		}
		return framework.NewStatus(framework.Success, "expected node"), 0
	}
	return framework.NewStatus(framework.Success, "success"), 0
}

func getFilterState(cycleState *framework.CycleState, nodeName string) (*FilterState, error) {
	key := framework.StateKey(filterStateKey + nodeName)
	no, err := cycleState.Read(key)
	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		return nil, fmt.Errorf("error reading %q from cycleState: %w", filterStateKey, err)
	}

	state, ok := no.Clone().(*FilterState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to NetworkOverhead.preFilterState error", no)
	}
	return state, nil
}
