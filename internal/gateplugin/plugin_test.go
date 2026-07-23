package gateplugin

import (
	"testing"

	"go.minekube.com/common/minecraft/component"
	"go.minekube.com/gate/pkg/edition/java/proxy"
)

type fakeConnectionResult struct {
	status proxy.ConnectionStatus
}

func (r fakeConnectionResult) Status() proxy.ConnectionStatus { return r.status }
func (fakeConnectionResult) Reason() component.Component      { return nil }

func TestTransferResultAcceptsOnlySuccessfulSwitch(t *testing.T) {
	if err := transferResult(fakeConnectionResult{status: proxy.SuccessConnectionStatus}, nil); err != nil {
		t.Fatalf("successful switch returned %v", err)
	}
	for _, status := range []proxy.ConnectionStatus{
		proxy.AlreadyConnectedConnectionStatus,
		proxy.InProgressConnectionStatus,
		proxy.CanceledConnectionStatus,
		proxy.ServerDisconnectedConnectionStatus,
	} {
		if err := transferResult(fakeConnectionResult{status: status}, nil); err == nil {
			t.Fatalf("status %d returned nil, want retryable error", status)
		}
	}
}
