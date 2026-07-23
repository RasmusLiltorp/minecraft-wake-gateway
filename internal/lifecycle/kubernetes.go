package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const ReplicasAnnotation = "platform.liltorp.dev/replicas"

var minecraftServerGVR = schema.GroupVersionResource{
	Group: "platform.liltorp.dev", Version: "v1alpha1", Resource: "minecraftservers",
}

func WakePatch() ([]byte, error) {
	return json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				ReplicasAnnotation: "1",
			},
		},
	})
}

type KubernetesWaker struct {
	Dynamic dynamic.Interface
}

func (w KubernetesWaker) Wake(ctx context.Context, target Target) error {
	patch, err := WakePatch()
	if err != nil {
		return err
	}
	_, err = w.Dynamic.Resource(minecraftServerGVR).Namespace(target.Namespace).Patch(
		ctx, target.Name, types.MergePatchType, patch, metav1.PatchOptions{},
	)
	return err
}

// KubernetesReadiness uses relist-then-watch cycles. Each WaitReady call gets
// fresh resource versions, so closed/expired watches recover on the next
// bounded coordinator cycle.
type KubernetesReadiness struct {
	Dynamic dynamic.Interface
	Client  kubernetes.Interface

	initialized atomic.Bool
}

func (r *KubernetesReadiness) Initialized() bool {
	return r.initialized.Load()
}

func (r *KubernetesReadiness) Check(ctx context.Context, target Target) (bool, error) {
	ready, _, _, err := r.snapshot(ctx, target)
	return ready, err
}

func (r *KubernetesReadiness) snapshot(
	ctx context.Context,
	target Target,
) (ready bool, serverResourceVersion, endpointResourceVersion string, err error) {
	server, err := r.Dynamic.Resource(minecraftServerGVR).Namespace(target.Namespace).Get(
		ctx, target.Name, metav1.GetOptions{},
	)
	if err != nil {
		return false, "", "", fmt.Errorf("read MinecraftServer: %w", err)
	}
	phase, _, _ := unstructuredString(server.Object, "status", "phase")
	slices, err := r.Client.DiscoveryV1().EndpointSlices(target.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: discoveryv1.LabelServiceName + "=" + target.Name,
	})
	if err != nil {
		return false, "", "", fmt.Errorf("list EndpointSlices: %w", err)
	}
	r.initialized.Store(true)
	return phase == "Running" && hasReadyEndpoint(slices.Items),
		server.GetResourceVersion(), slices.GetResourceVersion(), nil
}

func (r *KubernetesReadiness) WaitReady(ctx context.Context, target Target) error {
	ready, serverResourceVersion, endpointResourceVersion, err := r.snapshot(ctx, target)
	if err != nil {
		return err
	}
	if ready {
		return nil
	}

	serverWatch, err := r.Dynamic.Resource(minecraftServerGVR).Namespace(target.Namespace).Watch(
		ctx, metav1.ListOptions{
			FieldSelector:   "metadata.name=" + target.Name,
			ResourceVersion: serverResourceVersion,
		},
	)
	if err != nil {
		return fmt.Errorf("watch MinecraftServer: %w", err)
	}
	defer serverWatch.Stop()
	endpointWatch, err := r.Client.DiscoveryV1().EndpointSlices(target.Namespace).Watch(
		ctx, metav1.ListOptions{
			LabelSelector:   discoveryv1.LabelServiceName + "=" + target.Name,
			ResourceVersion: endpointResourceVersion,
		},
	)
	if err != nil {
		return fmt.Errorf("watch EndpointSlices: %w", err)
	}
	defer endpointWatch.Stop()

	// Close the handoff window between the relists and both watch requests. The
	// resource versions make the watches replay intervening changes; this check
	// also returns immediately if readiness changed while watches were starting.
	ready, err = r.Check(ctx, target)
	if err != nil {
		return err
	}
	if ready {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-serverWatch.ResultChan():
			if !ok {
				return errors.New("MinecraftServer watch closed")
			}
			if event.Type == watch.Error {
				return errors.New("MinecraftServer watch error")
			}
		case event, ok := <-endpointWatch.ResultChan():
			if !ok {
				return errors.New("EndpointSlice watch closed")
			}
			if event.Type == watch.Error {
				return errors.New("EndpointSlice watch error")
			}
		}
		ready, err = r.Check(ctx, target)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
	}
}

func hasReadyEndpoint(slices []discoveryv1.EndpointSlice) bool {
	for _, slice := range slices {
		for _, endpoint := range slice.Endpoints {
			if len(endpoint.Addresses) == 0 {
				continue
			}
			if endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready {
				return true
			}
		}
	}
	return false
}

func unstructuredString(object map[string]any, fields ...string) (string, bool, error) {
	current := any(object)
	for _, field := range fields {
		values, ok := current.(map[string]any)
		if !ok {
			return "", false, nil
		}
		current, ok = values[field]
		if !ok {
			return "", false, nil
		}
	}
	value, ok := current.(string)
	return value, ok, nil
}
