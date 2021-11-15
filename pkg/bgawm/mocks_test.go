package bgawm_test

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

type mockWorkingManagerMonitoring struct {
	RecoveryCounter            *counter
	RunCounter                 *counter
	ErrorInvocationCounter     *counter
	WorkerRecoveryCounter      *counter
	WorkerRunnableErrorCounter *counter
}

func newMockWorkingManagerMonitoring() *mockWorkingManagerMonitoring {
	return &mockWorkingManagerMonitoring{
		RecoveryCounter:            &counter{Val: 0},
		RunCounter:                 &counter{Val: 0},
		ErrorInvocationCounter:     &counter{Val: 0},
		WorkerRecoveryCounter:      &counter{Val: 0},
		WorkerRunnableErrorCounter: &counter{Val: 0},
	}
}

func (inst *mockWorkingManagerMonitoring) GetWorkerRecoveryCounter() common.CanIncrement {
	return inst.WorkerRecoveryCounter
}

func (inst *mockWorkingManagerMonitoring) GetWorkerRunnableErrorCounter() common.CanIncrement {
	return inst.WorkerRunnableErrorCounter
}

func (inst *mockWorkingManagerMonitoring) GetRecoveryCounter() common.CanIncrement {
	return inst.RecoveryCounter
}
func (inst *mockWorkingManagerMonitoring) GetRunCounter() common.CanIncrement {
	return inst.RunCounter

}
func (inst *mockWorkingManagerMonitoring) GetWorkerRunnableExecutionDurationTimer() common.CanObserveDuration {
	return &observeDuration{}
}
func (inst *mockWorkingManagerMonitoring) GetErrorInvocationCounter() common.CanIncrement {
	return inst.ErrorInvocationCounter
}
func (inst *mockWorkingManagerMonitoring) GetDurationTimer(processId string) common.CanObserveDuration {
	return &observeDuration{}
}
