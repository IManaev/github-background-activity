package bgawm

import (
	"errors"
	"github-background-activity/pkg/bga"
	"sync/atomic"
	"time"
)

var workerManagerOptsUndefined = errors.New("worker manager options should be defined")
var workerManagerPeriodicalProducerIDUndefined = errors.New("worker manager periodical producer id should be defined")
var workerManagerPeriodicalProducerUndefined = errors.New("worker manager periodical producer should be defined")
var workerManagerAlreadyStartedError = errors.New("worker manager already started")
var workerManagerProducersNumNotSetError = errors.New("worker manager producers number must be set > 0")
var workerManagerWorkerNumNotSetError = errors.New("worker manager producers number must be set > 0")

type WorkerArguments interface{}
type PeriodicalProducer = func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error)
type PeriodicalProducerOutput interface{}
type workerManagerProducerArgs struct {
	producerChannel chan<- PeriodicalProducerOutput
	opts            *BackgroundActivityWorkerManagerOptions
	wmId            string
}

type BackgroundActivityWorkerManagerOptions struct {
	PeriodicalProducer      PeriodicalProducer
	PeriodicalProducerArgs  bga.BackgroundActivityArguments
	WorkerArgs              WorkerArguments
	PeriodicalProducerID    string
	PeriodicalProducerDelay time.Duration
	ProducerNum             uint8
	Monitoring              WorkingManagerMonitoring
	Logger                  bga.BPLogger
	InstantProvider         func() time.Time
	WorkersNum              uint8
	WorkerRunnable          func(data PeriodicalProducerOutput, args WorkerArguments) error
}

type BackgroundActivityWorkerManager struct {
	opts               *BackgroundActivityWorkerManagerOptions
	producerChannel    chan PeriodicalProducerOutput
	workers            map[string]*Worker
	producerActivities []*bga.BackgroundActivity
	started            *uint32
	initialized        *uint32
}

func validateBackgroundActivityWorkingManagerOptions(opts *BackgroundActivityWorkerManagerOptions) error {
	if opts == nil {
		return workerManagerOptsUndefined
	}
	if opts.PeriodicalProducerID == "" {
		return workerManagerPeriodicalProducerIDUndefined
	}
	if opts.PeriodicalProducer == nil {
		return workerManagerPeriodicalProducerUndefined
	}
	if opts.ProducerNum <= 0 {
		return workerManagerProducersNumNotSetError
	}
	if opts.WorkersNum <= 0 {
		return workerManagerWorkerNumNotSetError
	}
	return nil
}

func NewBackgroundActivityWorkingManager(opts *BackgroundActivityWorkerManagerOptions) (*BackgroundActivityWorkerManager, error) {
	var err = validateBackgroundActivityWorkingManagerOptions(opts)
	if err != nil {
		return nil, err
	}
	var started uint32
	var initialized uint32
	atomic.StoreUint32(&started, 0)
	atomic.StoreUint32(&initialized, 0)
	var wm = &BackgroundActivityWorkerManager{
		opts:        opts,
		started:     &started,
		initialized: &initialized,
	}
	err = wm.initialize()
	if err != nil {
		return nil, err
	}
	return wm, nil
}

func (inst *BackgroundActivityWorkerManager) IsStopped() bool {
	return atomic.LoadUint32(inst.started) == 0
}

func (inst *BackgroundActivityWorkerManager) Stop() {
	if atomic.CompareAndSwapUint32(inst.started, 1, 0) {
		inst.stopProducers()
		for _, worker := range inst.workers {
			worker.Stop()
		}
		close(inst.producerChannel)
		inst.producerActivities = nil
		inst.workers = nil
		inst.producerChannel = nil
		atomic.StoreUint32(inst.initialized, 0)
	}
}

func (inst *BackgroundActivityWorkerManager) stopProducers() {
	for _, producer := range inst.producerActivities {
		producer.Stop()
	}
}

func (inst *BackgroundActivityWorkerManager) startProducers() error {
	for _, producer := range inst.producerActivities {
		err := producer.Run()
		if err != nil {
			return err
		}
	}
	return nil
}

func (inst *BackgroundActivityWorkerManager) initialize() error {
	if atomic.CompareAndSwapUint32(inst.initialized, 0, 1) {
		var producerChannel = make(chan PeriodicalProducerOutput, inst.opts.ProducerNum*inst.opts.WorkersNum)
		producerActivities := make([]*bga.BackgroundActivity, inst.opts.ProducerNum)
		for i := 0; i < int(inst.opts.ProducerNum); i++ {
			producerActivity, err := buildBackgroundProducerActivity(inst.opts, producerChannel, i)
			if err != nil {
				return err
			}
			producerActivities[i] = producerActivity
		}

		workers, err := buildWorkers(inst.opts, producerChannel)
		if err != nil {
			return err
		}
		inst.producerChannel = producerChannel
		inst.workers = workers
		inst.producerActivities = producerActivities
		return nil
	}
	return nil
}

func (inst *BackgroundActivityWorkerManager) Run() error {
	if atomic.CompareAndSwapUint32(inst.started, 0, 1) {
		var err = inst.initialize()
		if err != nil {
			atomic.StoreUint32(inst.started, 0)
			return err
		}
		err = inst.startProducers()
		if err != nil {
			atomic.StoreUint32(inst.started, 0)
			inst.stopProducers()
			return err
		}
		for _, worker := range inst.workers {
			err = worker.Run()
			if err != nil {
				atomic.StoreUint32(inst.started, 0)
				inst.Stop()
				return err
			}
		}
		return nil
	}
	return workerManagerAlreadyStartedError
}
