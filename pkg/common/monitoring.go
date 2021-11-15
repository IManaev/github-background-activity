package common

import "time"

type CanObserveDuration interface {
	ObserveDuration() time.Duration
}

type CanIncrement interface {
	Inc()
}
