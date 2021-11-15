package bga_test

import (
	"fmt"
	"github-background-activity/pkg/common"
	"testing"
	"time"
)

type mockLogger struct {
	T *testing.T
}

func getTime() string {
	return time.Now().Format("2006-01-02 15:04:05.000")
}

func (inst *mockLogger) Tracef(format string, args ...interface{}) {
	if inst.T != nil {
		inst.T.Logf("[%v]   %v", getTime(), fmt.Sprintf(format, args...))
	}
}
func (inst *mockLogger) Errorf(format string, args ...interface{}) {
	if inst.T != nil {
		inst.T.Logf("[%v]   %v", getTime(), fmt.Sprintf(format, args...))
	}
}
func (inst *mockLogger) Infof(format string, args ...interface{}) {
	if inst.T != nil {
		inst.T.Logf("[%v]   %v", getTime(), fmt.Sprintf(format, args...))
	}
}

type counter struct {
	Val uint32
}

func (inst *counter) Inc() {
	inst.Val++
}

type observeDuration struct{}

func (observeDuration) ObserveDuration() time.Duration {
	return time.Minute
}

type mockBackgroundActivityMonitoring struct {
	RecoveryCounter        *counter
	RunCounter             *counter
	ErrorInvocationCounter *counter
}

func newMockBackgroundActivityMonitoring() *mockBackgroundActivityMonitoring {
	return &mockBackgroundActivityMonitoring{
		RecoveryCounter:        &counter{Val: 0},
		RunCounter:             &counter{Val: 0},
		ErrorInvocationCounter: &counter{Val: 0},
	}
}

func (inst *mockBackgroundActivityMonitoring) GetRecoveryCounter() common.CanIncrement {
	return inst.RecoveryCounter
}
func (inst *mockBackgroundActivityMonitoring) GetRunCounter() common.CanIncrement {
	return inst.RunCounter

}
func (inst *mockBackgroundActivityMonitoring) GetErrorInvocationCounter() common.CanIncrement {
	return inst.ErrorInvocationCounter
}
func (inst *mockBackgroundActivityMonitoring) GetDurationTimer(processId string) common.CanObserveDuration {
	return &observeDuration{}
}
