package bgawm_test

import (
	"github-background-activity/pkg/bgawm"
	"github-background-activity/pkg/common"
	"go.uber.org/goleak"
	"runtime"
	"sync"
	"testing"
	"time"
)

type mockArgument struct {
	Val int
}

func runWorkerConcurrencyTest(t *testing.T) {
	defer goleak.VerifyNone(t)
	var goroutinesAtStart = runtime.NumGoroutine()
	var wg sync.WaitGroup
	wg.Add(4)
	t.Run("should not run twice", func(t *testing.T) {
		var readChannel = make(chan bgawm.PeriodicalProducerOutput, 1)
		var argument = &mockArgument{
			Val: 10,
		}
		var invoked = 0
		var worker, _ = bgawm.NewWorker(&bgawm.WorkerOptions{
			ID: "1",
			Runnable: func(args bgawm.PeriodicalProducerOutput, temp bgawm.WorkerArguments) error {
				if _, ok := args.(*mockArgument); !ok {
					t.Errorf("expected same item in arg, but was %v", common.Prettify(args))
					return nil
				}
				if args.(*mockArgument).Val != argument.Val {
					t.Errorf("expected %v, but was %v", common.Prettify(argument), common.Prettify(args))
				}
				invoked = invoked + 1
				panic("some unexpected panics")
			},
			ReadChannel: readChannel,
			Logger: &mockLogger{
				T: t,
			},
			Monitoring: newMockWorkingManagerMonitoring(),
		})
		defer wg.Done()
		defer worker.Stop()
		var err = worker.Run()
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		err = worker.Run()
		if err == nil {
			t.Errorf("expected error %v on run twice", err)
			return
		}
	})
	t.Run("should run, stop, close ignoring panics", func(t *testing.T) {
		var readChannel = make(chan bgawm.PeriodicalProducerOutput, 1)
		var argument = &mockArgument{
			Val: 10,
		}
		var invoked = 0
		var worker, _ = bgawm.NewWorker(&bgawm.WorkerOptions{
			ID: "1",
			Runnable: func(args bgawm.PeriodicalProducerOutput, temp bgawm.WorkerArguments) error {
				if _, ok := args.(*mockArgument); !ok {
					t.Errorf("expected same item in arg, but was %v", common.Prettify(args))
					return nil
				}
				if args.(*mockArgument).Val != argument.Val {
					t.Errorf("expected %v, but was %v", common.Prettify(argument), common.Prettify(args))
				}
				invoked = invoked + 1
				panic("some unexpected panics")
			},
			ReadChannel: readChannel,
			Logger: &mockLogger{
				T: t,
			},
			Monitoring: newMockWorkingManagerMonitoring(),
		})
		var err = worker.Run()
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		readChannel <- argument
		readChannel <- argument
		readChannel <- argument
		readChannel <- argument
		var cycles = 0
		for invoked != 4 || cycles == 10 {
			cycles++
			time.Sleep(10 * time.Millisecond)
		}
		if invoked != 4 {
			t.Error("expected worker to be invoked")
		}
		worker.Stop()
		time.Sleep(10 * time.Millisecond)
		close(readChannel)
		wg.Done()

	})
	t.Run("should run, stop, close", func(t *testing.T) {
		var readChannel = make(chan bgawm.PeriodicalProducerOutput, 1)
		var argument = &mockArgument{
			Val: 10,
		}
		var invoked = 0
		var worker, _ = bgawm.NewWorker(&bgawm.WorkerOptions{
			ID: "1",
			Runnable: func(args bgawm.PeriodicalProducerOutput, temp bgawm.WorkerArguments) error {
				if _, ok := args.(*mockArgument); !ok {
					t.Errorf("expected same item in arg, but was %v", common.Prettify(args))
					return nil
				}
				if args.(*mockArgument).Val != argument.Val {
					t.Errorf("expected %v, but was %v", common.Prettify(argument), common.Prettify(args))
				}
				invoked = 1
				return nil
			},
			ReadChannel: readChannel,
			Logger: &mockLogger{
				T: t,
			},
			Monitoring: newMockWorkingManagerMonitoring(),
		})
		var err = worker.Run()
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		readChannel <- argument
		var cycles = 0
		for invoked != 1 || cycles == 10 {
			cycles++
			time.Sleep(10 * time.Millisecond)
		}
		if invoked != 1 {
			t.Error("expected worker to be invoked")
		}
		worker.Stop()
		time.Sleep(10 * time.Millisecond)
		close(readChannel)
		wg.Done()
	})
	t.Run("should run, close, stop", func(t *testing.T) {
		var readChannel = make(chan bgawm.PeriodicalProducerOutput, 1)
		var argument = &mockArgument{
			Val: 10,
		}
		var invoked = 0
		var worker, _ = bgawm.NewWorker(&bgawm.WorkerOptions{
			ID: "1",
			Runnable: func(args bgawm.PeriodicalProducerOutput, temp bgawm.WorkerArguments) error {
				if _, ok := args.(*mockArgument); !ok {
					t.Errorf("expected same item in arg, but was %v", common.Prettify(args))
					return nil
				}
				if args.(*mockArgument).Val != argument.Val {
					t.Errorf("expected %v, but was %v", common.Prettify(argument), common.Prettify(args))
				}
				invoked = 1
				return nil
			},
			ReadChannel: readChannel,
			Logger: &mockLogger{
				T: t,
			},
			Monitoring: newMockWorkingManagerMonitoring(),
		})
		var err = worker.Run()
		if err != nil {
			t.Errorf("unexpected error %v", err)
			return
		}
		readChannel <- argument
		var cycles = 0
		for invoked != 1 || cycles == 10 {
			cycles++
			time.Sleep(10 * time.Millisecond)
		}
		if invoked != 1 {
			t.Error("expected worker to be invoked")
		}
		close(readChannel)
		time.Sleep(5 * time.Millisecond)
		worker.Stop()
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

func TestWorker_Run(t *testing.T) {
	for i := 0; i < 3; i++ {
		runWorkerConcurrencyTest(t)
	}
}

func TestNewWorker(t *testing.T) {
	t.Run("should validate nil opts", func(t *testing.T) {
		var process, err = bgawm.NewWorker(nil)
		if process != nil {
			t.Error("expected nil worker if options are nil")
		}
		if err == nil {
			t.Error("expected non nil error if options is nil")
		}
	})
	t.Run("should validate opts", func(t *testing.T) {
		var testArgs = []struct {
			Opts *bgawm.WorkerOptions
		}{
			{Opts: &bgawm.WorkerOptions{
				ID: "",
				Runnable: func(args bgawm.PeriodicalProducerOutput, temp bgawm.WorkerArguments) error {
					return nil
				},
				ReadChannel: make(chan bgawm.PeriodicalProducerOutput),
				Logger:      &mockLogger{},
				Monitoring:  newMockWorkingManagerMonitoring(),
			}},
			{Opts: &bgawm.WorkerOptions{
				ID:          "1",
				ReadChannel: make(chan bgawm.PeriodicalProducerOutput),
				Logger:      &mockLogger{},
				Monitoring:  newMockWorkingManagerMonitoring(),
			}},
			{Opts: &bgawm.WorkerOptions{
				ID: "1",
				Runnable: func(args bgawm.PeriodicalProducerOutput, temp bgawm.WorkerArguments) error {
					return nil
				},
				Logger:     &mockLogger{},
				Monitoring: newMockWorkingManagerMonitoring(),
			}},
			{Opts: &bgawm.WorkerOptions{
				ID: "1",
				Runnable: func(args bgawm.PeriodicalProducerOutput, temp bgawm.WorkerArguments) error {
					return nil
				},
				ReadChannel: make(chan bgawm.PeriodicalProducerOutput),
				Monitoring:  newMockWorkingManagerMonitoring(),
			}},
			{Opts: &bgawm.WorkerOptions{
				ID: "1",
				Runnable: func(args bgawm.PeriodicalProducerOutput, temp bgawm.WorkerArguments) error {
					return nil
				},
				ReadChannel: make(chan bgawm.PeriodicalProducerOutput),
				Logger:      &mockLogger{},
			}},
		}
		for _, args := range testArgs {
			var worker, err = bgawm.NewWorker(args.Opts)
			if worker != nil {
				t.Errorf("expected nil process if validation has errors %v", common.Prettify(args))
			}
			if err == nil {
				t.Errorf("expected non nil error if validation has errors %v", common.Prettify(args))
			}
		}
	})
}
