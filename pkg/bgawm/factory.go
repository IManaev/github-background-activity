package bgawm

import (
	"fmt"
	"github-background-activity/pkg/bga"
	"github-background-activity/pkg/common"
)

func buildWorkerId(opts *BackgroundActivityWorkerManagerOptions, i int) string {
	return fmt.Sprintf("%v-worker-%d", opts.PeriodicalProducerID, i)
}

func buildWorkerOptions(opts *BackgroundActivityWorkerManagerOptions, id string, producerChannel <-chan PeriodicalProducerOutput) *WorkerOptions {
	return &WorkerOptions{
		ID:          id,
		Runnable:    opts.WorkerRunnable,
		ReadChannel: producerChannel,
		Logger:      opts.Logger,
		Monitoring:  opts.Monitoring,
		Args:        opts.WorkerArgs,
	}
}

func buildWorkerManagerPeriodicalProducerRunner() func(args bga.BackgroundActivityArguments) (bool, error) {
	return func(args bga.BackgroundActivityArguments) (bool, error) {
		if rArgs, ok := args.(*workerManagerProducerArgs); ok {
			rArgs.opts.Logger.Tracef("worker manager id %v producer invoked", rArgs.wmId)
			var result, err = rArgs.opts.PeriodicalProducer(rArgs.opts.PeriodicalProducerArgs)
			rArgs.opts.Logger.Tracef("worker manager id %v producer result %v", rArgs.wmId, common.Prettify(result))
			if err != nil {
				rArgs.opts.Logger.Errorf("worker manager id %v unexpected error", rArgs.wmId, err)
				return false, err
			}
			if result != nil {
				rArgs.producerChannel <- result
				rArgs.opts.Logger.Tracef("worker manager id %v producer result written to producer channel", rArgs.wmId)
				return false, nil
			}
			rArgs.opts.Logger.Tracef("worker manager id %v nil producer result received", rArgs.wmId)
			return false, nil
		}
		panic("unexpected cast error - *workerManagerProducerArgs expected")
	}
}

func buildWorkerManagerId(opts *BackgroundActivityWorkerManagerOptions, id int) string {
	return fmt.Sprintf("%v-%v-wm", opts.PeriodicalProducerID, id)
}

func buildBackgroundProducerActivity(opts *BackgroundActivityWorkerManagerOptions, out chan PeriodicalProducerOutput, id int) (*bga.BackgroundActivity, error) {
	var wmId = buildWorkerManagerId(opts, id)
	producerActivity, err := bga.NewBackgroundActivity(&bga.BackgroundActivityOptions{
		ID:         wmId,
		Delay:      opts.PeriodicalProducerDelay,
		Runnable:   buildWorkerManagerPeriodicalProducerRunner(),
		Monitoring: opts.Monitoring,
		Logger:     opts.Logger,
		Args: &workerManagerProducerArgs{
			opts:            opts,
			wmId:            wmId,
			producerChannel: out,
		},
		InstantProvider: opts.InstantProvider,
	})
	if err != nil {
		return nil, err
	}
	return producerActivity, nil
}

func buildWorkers(opts *BackgroundActivityWorkerManagerOptions, producerChannel <-chan PeriodicalProducerOutput) (map[string]*Worker, error) {
	var workers = make(map[string]*Worker, opts.WorkersNum)
	for i := 0; i < int(opts.WorkersNum); i++ {
		var workerId = buildWorkerId(opts, i)
		if worker, err := NewWorker(buildWorkerOptions(opts, workerId, producerChannel)); err == nil {
			workers[workerId] = worker
		} else {
			return nil, err
		}
	}
	return workers, nil
}
