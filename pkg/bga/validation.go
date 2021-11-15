package bga

import (
	"errors"
)

var loggerIsUndefined = errors.New("logger should be defined")
var instantProviderIsUndefined = errors.New("instant provider should be defined")
var runnableIsUndefined = errors.New("runnable should be defined")
var durationIsUndefined = errors.New("duration should be defined and be more than 0")
var idIsUndefined = errors.New("id should be defined")
var optsIsUndefined = errors.New("options should be defined")

func validateOptions(opts *BackgroundActivityOptions) error {
	if opts == nil {
		return optsIsUndefined
	}
	if opts.Logger == nil {
		return loggerIsUndefined
	}
	if opts.InstantProvider == nil {
		return instantProviderIsUndefined
	}
	if opts.Runnable == nil {
		return runnableIsUndefined
	}
	if opts.Delay == 0 {
		return durationIsUndefined
	}
	if opts.ID == "" {
		return idIsUndefined
	}
	return nil
}
