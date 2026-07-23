package lifecycle

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestWakePatchChangesOnlyReplicasAnnotation(t *testing.T) {
	patch, err := WakePatch()
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(patch, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				ReplicasAnnotation: "1",
			},
		},
	}
	if asJSON(got) != asJSON(want) {
		t.Fatalf("patch = %s, want %s", asJSON(got), asJSON(want))
	}
}

func asJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func TestWaitReadyUsesResourceVersionsAndClosesWatchHandoffRace(t *testing.T) {
	var mu sync.Mutex
	ready := false
	serverWatchResourceVersion := ""
	endpointWatchResourceVersion := ""

	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	dynamicClient.PrependReactor("get", "minecraftservers", func(clienttesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		phase := "Provisioning"
		if ready {
			phase = "Running"
		}
		server := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "platform.liltorp.dev/v1alpha1",
			"kind":       "MinecraftServer",
			"metadata": map[string]any{
				"name":            "survival",
				"namespace":       "minecraft",
				"resourceVersion": "10",
			},
			"status": map[string]any{"phase": phase},
		}}
		return true, server, nil
	})
	dynamicClient.PrependWatchReactor("minecraftservers", func(action clienttesting.Action) (bool, watch.Interface, error) {
		mu.Lock()
		serverWatchResourceVersion = action.(clienttesting.WatchAction).GetWatchRestrictions().ResourceVersion
		mu.Unlock()
		return true, watch.NewRaceFreeFake(), nil
	})

	clientset := kubernetesfake.NewSimpleClientset()
	clientset.PrependReactor("list", "endpointslices", func(clienttesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		slices := &discoveryv1.EndpointSliceList{
			ListMeta: metav1.ListMeta{ResourceVersion: "20"},
		}
		if ready {
			isReady := true
			slices.Items = []discoveryv1.EndpointSlice{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "survival-ready",
					Namespace: "minecraft",
					Labels: map[string]string{
						discoveryv1.LabelServiceName: "survival",
					},
				},
				Endpoints: []discoveryv1.Endpoint{{
					Addresses:  []string{"10.42.0.10"},
					Conditions: discoveryv1.EndpointConditions{Ready: &isReady},
				}},
			}}
		}
		return true, slices, nil
	})
	clientset.PrependWatchReactor("endpointslices", func(action clienttesting.Action) (bool, watch.Interface, error) {
		mu.Lock()
		endpointWatchResourceVersion = action.(clienttesting.WatchAction).GetWatchRestrictions().ResourceVersion
		ready = true // Readiness changes after relist but before both watches are established.
		mu.Unlock()
		return true, watch.NewRaceFreeFake(), nil
	})

	readiness := &KubernetesReadiness{Dynamic: dynamicClient, Client: clientset}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := readiness.WaitReady(ctx, Target{Namespace: "minecraft", Name: "survival"}); err != nil {
		t.Fatalf("handoff readiness was missed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if serverWatchResourceVersion != "10" || endpointWatchResourceVersion != "20" {
		t.Fatalf(
			"watch resource versions = server %q endpoint %q, want 10/20",
			serverWatchResourceVersion,
			endpointWatchResourceVersion,
		)
	}
}
