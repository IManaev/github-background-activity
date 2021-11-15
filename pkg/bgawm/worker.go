package bgawm

import (
	"errors"
	"github-background-activity/pkg/bga"
	"github-background-activity/pkg/common"
	"runtime/debug"
	"sync/atomic"
)

var workerOptionsUndefinedError = errors.New("worker options must be defined")
var workerOptionsRunnableUndefinedError = errors.New("worker runnable must be defined")
var workerOptionsLoggerUndefinedError = errors.New("worker logger must be defined")
var workerOptionsReadChannelUndefinedError = errors.New("worker read channel must be defined")
var workerOptionsIDUndefinedError = errors.New("worker ID must be defined")
var workerOptionsMonitoringUndefinedError = errors.New("worker monitoring must be defined")
var workerAlreadyStartedError = errors.New("worker already started")

type Worker struct {
	*WorkerOptions
	done    chan struct{}
	started *uint32
}

type WorkerOptions struct {
	ID          string
	Runnable    func(data PeriodicalProducerOutput, args WorkerArguments) error
	ReadChannel <-chan PeriodicalProducerOutput
	Logger      bga.BPLogger
	Args        WorkerArguments
	Monitoring  WorkingManagerMonitoring
}

func NewWorker(opts *WorkerOptions) (*Worker, error) {
	var err = validateWorkerOptions(opts)
	if err != nil {
		return nil, err
	}
	var started uint32
	atomic.StoreUint32(&started, 0)
	return &Worker{
		WorkerOptions: opts,
		started:       &started,
	}, nil
}

func (inst *Worker) Stop() {
	if atomic.CompareAndSwapUint32(inst.started, 1, 0) {
		inst.done <- struct{}{}
		close(inst.done)
	}
}

func (inst *Worker) IsStopped() bool {
	return atomic.LoadUint32(inst.started) == 0
}

func (inst *Worker) Run() error {
	if atomic.CompareAndSwapUint32(inst.started, 0, 1) {
		inst.done = make(chan struct{})
		go inst.doRun()
		return nil
	}
	return workerAlreadyStartedError
}

func (inst *Worker) doRun() {
	inst.Logger.Infof("Worker %v run start", inst.ID)
	for {
		select {
		case data, ok := <-inst.ReadChannel:
			if !ok {
				if atomic.CompareAndSwapUint32(inst.started, 1, 0) {
					inst.Logger.Infof("Worker %v read channel stopped", inst.ID)
					close(inst.done)
					return
				}
				break
			}
			inst.Logger.Tracef("Worker %v received arg %v", inst.ID, common.Prettify(data))
			inst.invokeRunnable(data)
		case <-inst.done:
			inst.Logger.Infof("Worker %v stopped, read by done channel", inst.ID)
			return
		}
	}
}

func (inst *Worker) invokeRunnable(arg PeriodicalProducerOutput) {
	if inst.IsStopped() {
		inst.Logger.Infof("Worker %v stopped, identified in runnable", inst.ID)
		return
	}
	defer func(inst *Worker) {
		if err := recover(); err != nil {
			inst.Monitoring.GetWorkerRecoveryCounter().Inc()
			inst.Logger.Errorf("Worker recovered from error %+s, %v id %v, arg %v", debug.Stack(), err, inst.ID, common.Prettify(arg))
		}
	}(inst)
	timer := inst.Monitoring.GetWorkerRunnableExecutionDurationTimer()
	defer timer.ObserveDuration()

	var err = inst.Runnable(arg, inst.Args)
	inst.Logger.Tracef("worker %v invoked runnable with arg %v", inst.ID, common.Prettify(arg))
	if err != nil {
		inst.Logger.Errorf("Worker runnable error, %v id %v, arg %v", err, inst.ID, common.Prettify(arg))
		inst.Monitoring.GetWorkerRunnableErrorCounter().Inc()
	}
}

func validateWorkerOptions(opts *WorkerOptions) error {
	if opts == nil {
		return workerOptionsUndefinedError
	}
	if opts.Runnable == nil {
		return workerOptionsRunnableUndefinedError
	}
	if opts.Logger == nil {
		return workerOptionsLoggerUndefinedError
	}
	if opts.ReadChannel == nil {
		return workerOptionsReadChannelUndefinedError
	}
	if opts.Monitoring == nil {
		return workerOptionsMonitoringUndefinedError
	}
	if opts.ID == "" {
		return workerOptionsIDUndefinedError
	}
	return nil
}
