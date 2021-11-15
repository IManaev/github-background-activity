package bga

import (
	"errors"
	"runtime/debug"
	"sync/atomic"
	"time"
)

var activityAlreadyStarted = errors.New("activity was already started")

type BackgroundActivityArguments interface{}

type BackgroundActivityOptions struct {
	ID              string
	Delay           time.Duration
	Timeout         time.Duration
	Runnable        func(args BackgroundActivityArguments) (bool, error)
	Monitoring      BackgroundActivityMonitoring
	Logger          BPLogger
	Args            BackgroundActivityArguments
	OnFinished      func(args BackgroundActivityArguments, ID string, isSuccess bool, isStopped bool, isTimeout bool)
	InstantProvider func() time.Time
}

type BackgroundActivity struct {
	opts                   *BackgroundActivityOptions
	started                *uint32
	startTime              time.Time
	lastIterationStartTime time.Time
	iterationNum           uint32
}

func NewBackgroundActivity(opts *BackgroundActivityOptions) (*BackgroundActivity, error) {
	var started uint32
	atomic.StoreUint32(&started, 0)
	var err = validateOptions(opts)
	if err != nil {
		return nil, err
	}
	return &BackgroundActivity{
		started:      &started,
		iterationNum: 0,
		opts:         opts,
	}, nil
}

func (inst *BackgroundActivity) GetOptions() *BackgroundActivityOptions {
	return inst.opts
}

func (inst *BackgroundActivity) Run() error {
	if atomic.CompareAndSwapUint32(inst.started, 0, 1) {
		inst.opts.Logger.Infof("scheduler started, iteration %v id %v", inst.iterationNum, inst.opts.ID)
		inst.startTime = inst.opts.InstantProvider()
		go inst.doRunIteration()
		return nil
	}
	return activityAlreadyStarted
}

func (inst *BackgroundActivity) doRunIteration() {
	if inst.IsStopped() {
		inst.opts.Logger.Infof("background activity stopped, iteration interrupted, iteration %v id %v", inst.iterationNum, inst.opts.ID)
		if inst.opts.OnFinished != nil {
			inst.opts.OnFinished(inst.opts.Args, inst.opts.ID, false, true, false)
		}
		return
	}
	if inst.isTimeout() {
		inst.opts.Logger.Infof("background activity timeout, iteration interrupted, iteration %v id %v", inst.iterationNum, inst.opts.ID)
		if inst.opts.OnFinished != nil {
			inst.opts.OnFinished(inst.opts.Args, inst.opts.ID, false, false, true)
		}
		inst.doStop(false)
		return
	}
	if inst.opts.Monitoring != nil {
		inst.opts.Monitoring.GetRunCounter().Inc()
	}
	inst.lastIterationStartTime = inst.opts.InstantProvider()
	time.Sleep(inst.opts.Delay)
	inst.iterationNum++
	if inst.opts.Monitoring != nil {
		timer := inst.opts.Monitoring.GetDurationTimer(inst.opts.ID)
		defer timer.ObserveDuration()
	}
	isFinished, err := inst.doInvokeRunnable()
	if isFinished {
		if inst.opts.OnFinished != nil {
			inst.opts.OnFinished(inst.opts.Args, inst.opts.ID, err == nil, false, false)
		}
		inst.doStop(false)
		return
	}
	go inst.doRunIteration()
}

func (inst *BackgroundActivity) isTimeout() bool {
	if inst.opts.Timeout <= 0 {
		return false
	}
	var endTime = inst.startTime.Add(inst.opts.Timeout)
	return time.Now().After(endTime)
}

func (inst *BackgroundActivity) IsStopped() bool {
	return atomic.LoadUint32(inst.started) == 0
}

func (inst *BackgroundActivity) doInvokeRunnable() (bool, error) {
	defer func(inst *BackgroundActivity) {
		if err := recover(); err != nil {
			if inst.opts.Monitoring != nil {
				inst.opts.Monitoring.GetRecoveryCounter().Inc()
			}
			inst.opts.Logger.Errorf("background activity recovered from error %+s, %v, iteration %v id %v", debug.Stack(), err, inst.iterationNum, inst.opts.ID)
		}
	}(inst)
	inst.opts.Logger.Tracef("background activity iteration started, iteration %v id %v", inst.iterationNum, inst.opts.ID)
	finished, err := inst.opts.Runnable(inst.opts.Args)
	if err != nil {
		if inst.opts.Monitoring != nil {
			inst.opts.Monitoring.GetErrorInvocationCounter().Inc()
		}
		inst.opts.Logger.Errorf("background activity iteration finished with error, iteration %v id %v", inst.iterationNum, inst.opts.ID, err)
	} else {
		inst.opts.Logger.Tracef("background activity iteration success, iteration %v id %v, err %v", inst.iterationNum, inst.opts.ID, err)
	}
	return finished, err
}

func (inst *BackgroundActivity) GetStartTime() time.Time {
	return inst.startTime
}

func (inst *BackgroundActivity) GetLastRunTime() time.Time {
	return inst.lastIterationStartTime
}

func (inst *BackgroundActivity) GetIterationsRun() uint32 {
	return inst.iterationNum
}

func (inst *BackgroundActivity) Stop() {
	inst.doStop(true)
}

func (inst *BackgroundActivity) doStop(doInvokeOnFinished bool) {
	if atomic.CompareAndSwapUint32(inst.started, 1, 0) {
		atomic.SwapUint32(inst.started, 0)
		inst.opts.Logger.Infof("background activity stopped, iteration %v id %v", inst.iterationNum, inst.opts.ID)
		if inst.opts.OnFinished != nil && doInvokeOnFinished {
			inst.opts.OnFinished(inst.opts.Args, inst.opts.ID, false, true, false)
		}
	}
}
