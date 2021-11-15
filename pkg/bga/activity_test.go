package bga_test

import (
	"github-background-activity/pkg/bga"
	"github-background-activity/pkg/common"
	"go.uber.org/goleak"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"
)

var mockRunnable = func(args bga.BackgroundActivityArguments) (bool, error) {
	return false, nil
}

func TestBackgroundActivity_Run_SupportTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)
	var goroutinesAtStart = runtime.NumGoroutine()
	var wg sync.WaitGroup
	wg.Add(2)
	t.Run("should run with respect to runnable stop", func(t *testing.T) {
		defer wg.Done()
		var monitoring = newMockBackgroundActivityMonitoring()
		var iterations = 0
		var maxIterations = 3
		var onFinishedInvocations = 0
		var process, _ = bga.NewBackgroundActivity(&bga.BackgroundActivityOptions{
			Delay:      time.Duration(time.Millisecond * 5),
			ID:         randString(5),
			Monitoring: monitoring,
			Runnable: func(args bga.BackgroundActivityArguments) (bool, error) {
				iterations++
				return iterations == maxIterations, nil
			},
			OnFinished: func(args bga.BackgroundActivityArguments, ID string, isSuccess bool, isStopped bool, isTimeout bool) {
				onFinishedInvocations++
				if isStopped == true || isTimeout == true {
					t.Errorf("unexpected onFinished result, isStopped %v isTimeout %v", isStopped, isTimeout)
				}
			},
			InstantProvider: func() time.Time {
				return time.Now()
			},
			Logger: &mockLogger{T: t},
		})
		_ = process.Run()
		time.Sleep(time.Millisecond * 25)
		if !process.IsStopped() {
			t.Error("expected worker to be stopped")
		}
		if !process.IsStopped() {
			t.Error("expected worker to be stopped")
		}
		if iterations != maxIterations {
			t.Errorf("expected %v iterations but was %v", maxIterations, iterations)
		}
		if onFinishedInvocations != 1 {
			t.Errorf("expected %v onFinishedInvocations but was %v", 1, onFinishedInvocations)
		}
	})
	t.Run("should run with respect to timeout", func(t *testing.T) {
		defer wg.Done()
		var monitoring = newMockBackgroundActivityMonitoring()
		var iterations = 0
		var onFinishedInvocations = 0
		var process, _ = bga.NewBackgroundActivity(&bga.BackgroundActivityOptions{
			Delay:      time.Millisecond * 5,
			ID:         randString(5),
			Monitoring: monitoring,
			Timeout:    time.Millisecond * 15,
			Runnable: func(args bga.BackgroundActivityArguments) (bool, error) {
				iterations++
				return false, nil
			},
			InstantProvider: func() time.Time {
				return time.Now()
			},
			OnFinished: func(args bga.BackgroundActivityArguments, ID string, isSuccess bool, isStopped bool, isTimeout bool) {
				onFinishedInvocations++
				if isStopped == true || isTimeout != true {
					t.Errorf("unexpected onFinished result, isStopped %v isTimeout %v", isStopped, isTimeout)
				}
			},
			Logger: &mockLogger{T: t},
		})
		_ = process.Run()
		time.Sleep(time.Millisecond * 20)
		if !process.IsStopped() {
			t.Error("expected worker to be stopped")
		}
		if iterations > 5 || iterations < 2 {
			t.Errorf("expected 3 or 4 iterations but was %v", iterations)
		}
		if onFinishedInvocations != 1 {
			t.Errorf("expected %v onFinishedInvocations but was %v", 1, onFinishedInvocations)
		}
	})
	wg.Wait()
	time.Sleep(2000 * time.Millisecond)
	var goroutinesAtEnd = runtime.NumGoroutine()
	t.Logf("Goroutine at start %v end %v", goroutinesAtStart, goroutinesAtEnd)
	if goroutinesAtStart != goroutinesAtEnd {
		t.Errorf("BackgroundActivity() leak, got %v goroutines at end, want same as at start %v", goroutinesAtEnd, goroutinesAtStart)
	}
}

func TestNewBackgroundActivity(t *testing.T) {
	t.Run("should validate nil opts", func(t *testing.T) {
		var process, err = bga.NewBackgroundActivity(nil)
		if process != nil {
			t.Error("expected nil worker if options are nil")
		}
		if err == nil {
			t.Error("expected non nil error if options is nil")
		}
	})
	t.Run("should validate opts", func(t *testing.T) {
		var testArgs = []struct {
			Opts *bga.BackgroundActivityOptions
		}{
			{Opts: &bga.BackgroundActivityOptions{
				ID:         "",
				Delay:      1,
				Runnable:   mockRunnable,
				Monitoring: newMockBackgroundActivityMonitoring(),
				Logger:     &mockLogger{T: t},
				Args:       nil,
				InstantProvider: func() time.Time {
					return time.Now()
				},
			}},
			{Opts: &bga.BackgroundActivityOptions{
				ID:         "1",
				Delay:      0,
				Runnable:   mockRunnable,
				Monitoring: newMockBackgroundActivityMonitoring(),
				Logger:     &mockLogger{T: t},
				Args:       nil,
				InstantProvider: func() time.Time {
					return time.Now()
				},
			}},
			{Opts: &bga.BackgroundActivityOptions{
				ID:         "1",
				Delay:      1,
				Monitoring: newMockBackgroundActivityMonitoring(),
				Logger:     &mockLogger{T: t},
				Args:       nil,
				InstantProvider: func() time.Time {
					return time.Now()
				},
			}},
			{Opts: &bga.BackgroundActivityOptions{
				ID:         "1",
				Delay:      1,
				Runnable:   mockRunnable,
				Monitoring: newMockBackgroundActivityMonitoring(),
				Args:       nil,
				InstantProvider: func() time.Time {
					return time.Now()
				},
			}},
			{Opts: &bga.BackgroundActivityOptions{
				ID:         "1",
				Delay:      1,
				Runnable:   mockRunnable,
				Monitoring: newMockBackgroundActivityMonitoring(),
				Logger:     &mockLogger{T: t},
				Args:       nil,
			}},
		}
		for _, args := range testArgs {
			var process, err = bga.NewBackgroundActivity(args.Opts)
			if process != nil {
				t.Errorf("expected nil process if validation has errors %v", common.Prettify(args))
			}
			if err == nil {
				t.Errorf("expected non nil error if validation has errors %v", common.Prettify(args))
			}
		}
	})
}

func TestBackgroundActivity(t *testing.T) {
	defer goleak.VerifyNone(t)
	var goroutinesAtStart = runtime.NumGoroutine()
	var wg sync.WaitGroup
	wg.Add(3)

	t.Run("should pass args as interface", func(t *testing.T) {
		var process, _ = createActivityWithArgs(t, func(args bga.BackgroundActivityArguments) (bool, error) {
			t.Logf("test process iteration")
			if _, ok := args.(*mockArgs); !ok {
				t.Error("same struct argument must be passed")
			}
			return false, nil
		}, &mockArgs{Val: 10})
		_ = process.Run()
		time.Sleep(process.GetOptions().Delay * time.Duration(2))
		process.Stop()
	})
	t.Run("should not run twice", func(t *testing.T) {
		var process, _ = createActivity(t, func(args bga.BackgroundActivityArguments) (bool, error) {
			t.Logf("test process iteration")
			return false, nil
		})
		err := process.Run()
		if err != nil {
			t.Errorf("BackgroundActivity.Run() unexpected error %v", err)
		}
		err = process.Run()
		if err == nil {
			t.Error("BackgroundActivity.Run() expected error on second run, but was nil")
		}
		process.Stop()
		wg.Done()
	})

	t.Run("should run and interrupt", func(t *testing.T) {
		_, _ = runAndCheck(t, 4, func(args bga.BackgroundActivityArguments) (bool, error) {
			t.Logf("test process iteration")
			return false, nil
		})
		wg.Done()
	})

	t.Run("should run and interrupt ignoring panics", func(t *testing.T) {
		process, monitoring := runAndCheck(t, 6, func(args bga.BackgroundActivityArguments) (bool, error) {
			t.Logf("test process iteration")
			panic("test panic ")
		})
		if monitoring.RecoveryCounter.Val < 3 {
			t.Errorf("BackgroundActivity.GetRecoveredFromPanics() expected 3 recoverings but was %v", process.GetIterationsRun())
		}
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

func createActivity(t *testing.T, runnable func(args bga.BackgroundActivityArguments) (bool, error)) (*bga.BackgroundActivity, *mockBackgroundActivityMonitoring) {
	var monitoring = newMockBackgroundActivityMonitoring()
	var process, _ = bga.NewBackgroundActivity(&bga.BackgroundActivityOptions{
		Delay:      time.Duration(time.Millisecond * 10),
		ID:         randString(5),
		Monitoring: monitoring,
		InstantProvider: func() time.Time {
			return time.Now()
		},
		Runnable: runnable,
		Logger:   &mockLogger{T: t},
	})
	return process, monitoring
}

type mockArgs struct {
	Val int
}

func createActivityWithArgs(t *testing.T, runnable func(args bga.BackgroundActivityArguments) (bool, error), args *mockArgs) (*bga.BackgroundActivity, *mockBackgroundActivityMonitoring) {
	var monitoring = newMockBackgroundActivityMonitoring()
	var process, _ = bga.NewBackgroundActivity(&bga.BackgroundActivityOptions{
		Delay:      time.Duration(time.Millisecond * 10),
		ID:         randString(5),
		Args:       args,
		Monitoring: monitoring,
		Runnable:   runnable,
		InstantProvider: func() time.Time {
			return time.Now()
		},
		Logger: &mockLogger{T: t},
	})
	return process, monitoring
}

func runAndCheck(t *testing.T, runs int, runnable func(args bga.BackgroundActivityArguments) (bool, error)) (*bga.BackgroundActivity, *mockBackgroundActivityMonitoring) {
	var process, monitoring = createActivity(t, runnable)
	var start = time.Now()
	_ = process.Run()
	time.Sleep(process.GetOptions().Delay * time.Duration(runs))
	process.Stop()
	if process.GetIterationsRun() < uint32(runs-2) {
		t.Errorf("BackgroundActivity.Run() expected %v iterations but was %v", uint32(runs-2), process.GetIterationsRun())
	}
	if process.GetStartTime().Before(start.Add(-time.Duration(1 * time.Millisecond))) {
		t.Errorf("BackgroundActivity.GetStartTime() expected after stop time")
	}
	if process.GetLastRunTime().Before(process.GetStartTime()) || !process.GetLastRunTime().After(process.GetStartTime().Add(process.GetOptions().Delay*2)) {
		t.Errorf("BackgroundActivity.GetLastRunTime() expected after start and representing last run")
	}
	return process, monitoring
}

var src = rand.NewSource(time.Now().UnixNano())

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = 63 / letterIdxBits
)

func randString(n int) string {
	b := make([]byte, n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}
