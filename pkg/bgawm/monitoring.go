package bgawm

import (
	"github-background-activity/pkg/bga"
	"github-background-activity/pkg/common"
)

type WorkingManagerMonitoring interface {
	bga.BackgroundActivityMonitoring
	GetWorkerRecoveryCounter() common.CanIncrement
	GetWorkerRunnableErrorCounter() common.CanIncrement
	GetWorkerRunnableExecutionDurationTimer() common.CanObserveDuration
}
