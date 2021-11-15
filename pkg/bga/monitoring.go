package bga

import "github-background-activity/pkg/common"

type BackgroundActivityMonitoring interface {
	GetRecoveryCounter() common.CanIncrement
	GetRunCounter() common.CanIncrement
	GetErrorInvocationCounter() common.CanIncrement
	GetDurationTimer(processId string) common.CanObserveDuration
}
