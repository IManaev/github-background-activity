package bgawm_test

import (
	"errors"
	"fmt"
	"github-background-activity/pkg/bga"
	. "github-background-activity/pkg/bgawm"
	"github-background-activity/pkg/common"
	"go.uber.org/goleak"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

type mockWorkerArgument struct {
	Val int
}
type mockPeriodicalProducerOutput struct {
	Iteration int
}

func TestBackgroundActivityWorkerManager_Run(t *testing.T) {
	defer goleak.VerifyNone(t)
	var goroutinesAtStart = runtime.NumGoroutine()
	var wg sync.WaitGroup
	wg.Add(4)
	t.Run("should not run twice", func(t *testing.T) {
		var monitoring = newMockWorkingManagerMonitoring()

		var manager, _ = NewBackgroundActivityWorkingManager(&BackgroundActivityWorkerManagerOptions{
			PeriodicalProducer: func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error) {
				panic("some panic")
			},
			PeriodicalProducerArgs: &mockArgument{
				Val: 101,
			},
			PeriodicalProducerID:    "1",
			PeriodicalProducerDelay: 5 * time.Millisecond,
			Monitoring:              monitoring,
			Logger: &mockLogger{
				T: t,
			},
			InstantProvider: func() time.Time {
				return time.Now()
			},
			WorkerArgs: &mockWorkerArgument{
				Val: 102,
			},
			ProducerNum: 1,
			WorkersNum:  3,
			WorkerRunnable: func(data PeriodicalProducerOutput, args WorkerArguments) error {
				panic(errors.New("some error in iteration"))
			},
		})
		defer wg.Done()
		defer manager.Stop()
		var err = manager.Run()
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		err = manager.Run()
		if err == nil {
			t.Errorf("expected error %v on run twice", err)
			return
		}
	})
	t.Run("should run, process event, stop, increment panics in worker runnable", func(t *testing.T) {
		var iterations = 3
		var iteration = -1
		var monitoring = newMockWorkingManagerMonitoring()
		var noticedIterations = make([]int, iterations)
		var manager, _ = NewBackgroundActivityWorkingManager(&BackgroundActivityWorkerManagerOptions{
			PeriodicalProducer: func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error) {
				if _, ok := args.(*mockArgument); !ok {
					t.Errorf("unexpected type in PeriodicalProducer arguments %v", common.Prettify(args))
					return nil, fmt.Errorf("unexpected type in PeriodicalProducer arguments %v", common.Prettify(args))
				}
				iteration++
				if iteration >= iterations {
					return nil, errors.New("overflow")
				}
				return &mockPeriodicalProducerOutput{
					Iteration: iteration,
				}, nil
			},
			PeriodicalProducerArgs: &mockArgument{
				Val: 101,
			},
			ProducerNum:             1,
			PeriodicalProducerID:    "1",
			PeriodicalProducerDelay: 5 * time.Millisecond,
			Monitoring:              monitoring,
			Logger: &mockLogger{
				T: t,
			},
			InstantProvider: func() time.Time {
				return time.Now()
			},
			WorkerArgs: &mockWorkerArgument{
				Val: 102,
			},
			WorkersNum: 3,
			WorkerRunnable: func(data PeriodicalProducerOutput, args WorkerArguments) error {
				var rData, ok = data.(*mockPeriodicalProducerOutput)
				if !ok {
					t.Errorf("unexpected type in WorkerRunnable data %v", common.Prettify(data))
					return fmt.Errorf("unexpected type in WorkerRunnable data %v", common.Prettify(data))
				}
				_, ok = args.(*mockWorkerArgument)
				if !ok {
					t.Errorf("unexpected type in WorkerRunnable arguments %v", common.Prettify(args))
					return fmt.Errorf("unexpected type in WorkerRunnable arguments %v", common.Prettify(args))
				}
				noticedIterations[rData.Iteration] = rData.Iteration
				panic(errors.New("some error in iteration"))
			},
		})
		if !manager.IsStopped() {
			t.Error("expected manager to be stopped")
			return
		}
		_ = manager.Run()
		if manager.IsStopped() {
			t.Error("expected manager to be started")
			return
		}
		var try = 0
		for iteration < iterations && try < iterations*2 {
			try++
			time.Sleep(5 * time.Millisecond)
		}
		if !reflect.DeepEqual(noticedIterations, createIntArr(iterations)) {
			t.Errorf("unexpected result %v", common.Prettify(noticedIterations))
		}
		if monitoring.WorkerRecoveryCounter.Val != uint32(iterations) {
			t.Errorf("unexpected recovery accumulated %d, expected %d", monitoring.WorkerRecoveryCounter.Val, iterations)
		}
		manager.Stop()
		wg.Done()
	})
	t.Run("should run, process event, stop, increment error in worker runnable", func(t *testing.T) {
		var iterations = 3
		var iteration = -1
		var monitoring = newMockWorkingManagerMonitoring()
		var noticedIterations = make([]int, iterations)
		var manager, _ = NewBackgroundActivityWorkingManager(&BackgroundActivityWorkerManagerOptions{
			PeriodicalProducer: func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error) {
				if _, ok := args.(*mockArgument); !ok {
					t.Errorf("unexpected type in PeriodicalProducer arguments %v", common.Prettify(args))
					return nil, fmt.Errorf("unexpected type in PeriodicalProducer arguments %v", common.Prettify(args))
				}
				iteration++
				if iteration >= iterations {
					return nil, errors.New("overflow")
				}
				return &mockPeriodicalProducerOutput{
					Iteration: iteration,
				}, nil
			},
			PeriodicalProducerArgs: &mockArgument{
				Val: 101,
			},
			PeriodicalProducerID:    "1",
			PeriodicalProducerDelay: 5 * time.Millisecond,
			Monitoring:              monitoring,
			Logger: &mockLogger{
				T: t,
			},
			ProducerNum: 1,
			InstantProvider: func() time.Time {
				return time.Now()
			},
			WorkerArgs: &mockWorkerArgument{
				Val: 102,
			},
			WorkersNum: 3,
			WorkerRunnable: func(data PeriodicalProducerOutput, args WorkerArguments) error {
				var rData, ok = data.(*mockPeriodicalProducerOutput)
				if !ok {
					t.Errorf("unexpected type in WorkerRunnable data %v", common.Prettify(data))
					return fmt.Errorf("unexpected type in WorkerRunnable data %v", common.Prettify(data))
				}
				_, ok = args.(*mockWorkerArgument)
				if !ok {
					t.Errorf("unexpected type in WorkerRunnable arguments %v", common.Prettify(args))
					return fmt.Errorf("unexpected type in WorkerRunnable arguments %v", common.Prettify(args))
				}
				noticedIterations[rData.Iteration] = rData.Iteration
				return errors.New("some error in iteration")
			},
		})
		_ = manager.Run()
		var try = 0
		for iteration < iterations && try < iterations*2 {
			try++
			time.Sleep(5 * time.Millisecond)
		}
		if !reflect.DeepEqual(noticedIterations, createIntArr(iterations)) {
			t.Errorf("unexpected result %v", common.Prettify(noticedIterations))
		}
		if monitoring.WorkerRunnableErrorCounter.Val != uint32(iterations) {
			t.Errorf("unexpected errors accumulated %d, expected %d", monitoring.WorkerRunnableErrorCounter.Val, iterations)
		}
		manager.Stop()
		wg.Done()
	})
	t.Run("should run, process event and stop", func(t *testing.T) {
		var iterations = 10
		var iteration = -1
		var noticedIterations = make([]int, iterations)
		var manager, _ = NewBackgroundActivityWorkingManager(&BackgroundActivityWorkerManagerOptions{
			PeriodicalProducer: func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error) {
				if _, ok := args.(*mockArgument); !ok {
					t.Errorf("unexpected type in PeriodicalProducer arguments %v", common.Prettify(args))
					return nil, fmt.Errorf("unexpected type in PeriodicalProducer arguments %v", common.Prettify(args))
				}
				iteration++
				if iteration >= iterations {
					return nil, errors.New("overflow")
				}
				return &mockPeriodicalProducerOutput{
					Iteration: iteration,
				}, nil
			},
			PeriodicalProducerArgs: &mockArgument{
				Val: 101,
			},
			ProducerNum:             1,
			PeriodicalProducerID:    "1",
			PeriodicalProducerDelay: 5 * time.Millisecond,
			Monitoring:              newMockWorkingManagerMonitoring(),
			Logger: &mockLogger{
				T: t,
			},
			InstantProvider: func() time.Time {
				return time.Now()
			},
			WorkerArgs: &mockWorkerArgument{
				Val: 102,
			},
			WorkersNum: 3,
			WorkerRunnable: func(data PeriodicalProducerOutput, args WorkerArguments) error {
				var rData, ok = data.(*mockPeriodicalProducerOutput)
				if !ok {
					t.Errorf("unexpected type in WorkerRunnable data %v", common.Prettify(data))
					return fmt.Errorf("unexpected type in WorkerRunnable data %v", common.Prettify(data))
				}
				_, ok = args.(*mockWorkerArgument)
				if !ok {
					t.Errorf("unexpected type in WorkerRunnable arguments %v", common.Prettify(args))
					return fmt.Errorf("unexpected type in WorkerRunnable arguments %v", common.Prettify(args))
				}
				noticedIterations[rData.Iteration] = rData.Iteration
				return nil
			},
		})
		_ = manager.Run()
		var try = 0
		for iteration < iterations && try < iterations*2 {
			try++
			time.Sleep(5 * time.Millisecond)
		}
		if !reflect.DeepEqual(noticedIterations, createIntArr(iterations)) {
			t.Errorf("unexpected result %v", common.Prettify(noticedIterations))
		}
		manager.Stop()
		wg.Done()
	})
	wg.Wait()
	time.Sleep(2000 * time.Millisecond)
	var goroutinesAtEnd = runtime.NumGoroutine()
	t.Logf("Goroutine at start %v end %v", goroutinesAtStart, goroutinesAtEnd)
	if goroutinesAtStart != goroutinesAtEnd {
		t.Errorf("BackgroundActivity() leak, got %v goroutines at end, want same as at start %v", goroutinesAtEnd, goroutinesAtStart)
	}
}

func createIntArr(size int) []int {
	var result = make([]int, size)
	for i := 0; i < size; i++ {
		result[i] = i
	}
	return result
}

func TestNewBackgroundActivityWorkingManager(t *testing.T) {
	var testArgs = []struct {
		opts *BackgroundActivityWorkerManagerOptions
	}{
		{
			opts: nil,
		},
		{
			opts: &BackgroundActivityWorkerManagerOptions{},
		},
		{
			opts: &BackgroundActivityWorkerManagerOptions{
				PeriodicalProducer: func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error) {
					return nil, nil
				},
				PeriodicalProducerArgs:  nil,
				PeriodicalProducerID:    "",
				PeriodicalProducerDelay: 1,
				Monitoring:              newMockWorkingManagerMonitoring(),
				Logger:                  &mockLogger{},
				InstantProvider: func() time.Time {
					return time.Now()
				},
				WorkersNum: 1,
				WorkerRunnable: func(data PeriodicalProducerOutput, args WorkerArguments) error {
					return nil
				},
			},
		},
		{
			opts: &BackgroundActivityWorkerManagerOptions{
				PeriodicalProducerArgs:  nil,
				PeriodicalProducerID:    "1",
				PeriodicalProducerDelay: 1,
				Monitoring:              newMockWorkingManagerMonitoring(),
				Logger:                  &mockLogger{},
				InstantProvider: func() time.Time {
					return time.Now()
				},
				WorkersNum: 1,
				WorkerRunnable: func(data PeriodicalProducerOutput, args WorkerArguments) error {
					return nil
				},
			},
		},
		{
			opts: &BackgroundActivityWorkerManagerOptions{
				PeriodicalProducer: func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error) {
					return nil, nil
				},
				PeriodicalProducerArgs:  nil,
				PeriodicalProducerID:    "1",
				PeriodicalProducerDelay: 0,
				Monitoring:              newMockWorkingManagerMonitoring(),
				Logger:                  &mockLogger{},
				InstantProvider: func() time.Time {
					return time.Now()
				},
				WorkersNum: 1,
				WorkerRunnable: func(data PeriodicalProducerOutput, args WorkerArguments) error {
					return nil
				},
			},
		},
		{
			opts: &BackgroundActivityWorkerManagerOptions{
				PeriodicalProducer: func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error) {
					return nil, nil
				},
				PeriodicalProducerArgs:  nil,
				PeriodicalProducerID:    "1",
				PeriodicalProducerDelay: 1,
				Logger:                  &mockLogger{},
				InstantProvider: func() time.Time {
					return time.Now()
				},
				WorkersNum: 1,
				WorkerRunnable: func(data PeriodicalProducerOutput, args WorkerArguments) error {
					return nil
				},
			},
		},
		{
			opts: &BackgroundActivityWorkerManagerOptions{
				PeriodicalProducer: func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error) {
					return nil, nil
				},
				PeriodicalProducerArgs:  nil,
				PeriodicalProducerID:    "1",
				PeriodicalProducerDelay: 0,
				Monitoring:              newMockWorkingManagerMonitoring(),
				InstantProvider: func() time.Time {
					return time.Now()
				},
				WorkersNum: 1,
				WorkerRunnable: func(data PeriodicalProducerOutput, args WorkerArguments) error {
					return nil
				},
			},
		},
		{
			opts: &BackgroundActivityWorkerManagerOptions{
				PeriodicalProducer: func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error) {
					return nil, nil
				},
				PeriodicalProducerArgs:  nil,
				PeriodicalProducerID:    "1",
				PeriodicalProducerDelay: 0,
				Monitoring:              newMockWorkingManagerMonitoring(),
				Logger:                  &mockLogger{},
				WorkersNum:              1,
				WorkerRunnable: func(data PeriodicalProducerOutput, args WorkerArguments) error {
					return nil
				},
			},
		},
		{
			opts: &BackgroundActivityWorkerManagerOptions{
				PeriodicalProducer: func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error) {
					return nil, nil
				},
				PeriodicalProducerArgs:  nil,
				PeriodicalProducerID:    "1",
				PeriodicalProducerDelay: 0,
				Monitoring:              newMockWorkingManagerMonitoring(),
				Logger:                  &mockLogger{},
				InstantProvider: func() time.Time {
					return time.Now()
				},
				WorkersNum: 1,
				WorkerRunnable: func(data PeriodicalProducerOutput, args WorkerArguments) error {
					return nil
				},
			},
		},
		{
			opts: &BackgroundActivityWorkerManagerOptions{
				PeriodicalProducer: func(args bga.BackgroundActivityArguments) (PeriodicalProducerOutput, error) {
					return nil, nil
				},
				PeriodicalProducerArgs:  nil,
				PeriodicalProducerID:    "1",
				PeriodicalProducerDelay: 1,
				Monitoring:              newMockWorkingManagerMonitoring(),
				Logger:                  &mockLogger{},
				InstantProvider: func() time.Time {
					return time.Now()
				},
				WorkersNum: 1,
			},
		},
	}
	for _, args := range testArgs {
		var worker, err = NewBackgroundActivityWorkingManager(args.opts)
		if worker != nil {
			t.Errorf("expected nil worker if validation has errors, args %v", common.Prettify(args.opts))
			return
		}
		if err == nil {
			t.Errorf("expected non nil error if validation has errors, args %v", common.Prettify(args.opts))
			return
		}
	}
}
