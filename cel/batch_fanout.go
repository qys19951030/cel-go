// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cel

import (
	"context"
	"errors"
	"sync"

	"cel.dev/cel-go/common/types/ref"
)

// FanoutResult records the outcome of a single named program within a FanoutEval call.
//
// Complete reports whether the program settled before the evaluation context ended.
// When Complete is true, Val, EvalDetails, and Err mirror the EvalResult delivered by
// the program's ConcurrentEval: Err is non-nil if the program itself failed. When
// Complete is false, the program was still in flight when the context ended and no
// outcome was recorded.
type FanoutResult struct {
	Val         ref.Val
	EvalDetails *EvalDetails
	Err         error
	Complete    bool
}

// FanoutResults maps each program name to its outcome. The map is fully populated
// before FanoutEval returns and is not mutated afterwards, so callers may read it
// from any goroutine without synchronization.
type FanoutResults map[string]FanoutResult

// FanoutEval evaluates a set of named programs concurrently against the same input,
// returning one FanoutResult per name.
//
// Each program is driven by its own ConcurrentEval call, and a dedicated goroutine
// per name collects the outcome. Failures are isolated: a program that returns an
// error records that error under its own name without affecting the other programs,
// and FanoutEval itself does not return an error.
//
// The input is passed through to every program unmodified, and results are not
// cached. If the same Program instance appears under multiple names, each name
// drives an independent evaluation and records its own result.
//
// When ctx ends, programs still in flight stop as their result channels close, any
// results that had already settled are kept, and names without a settled result are
// recorded as incomplete (Complete == false). FanoutEval never blocks waiting for a
// program after the context has ended. An empty program set yields an empty result
// set without launching any goroutines.
func FanoutEval(ctx context.Context, input any, programs map[string]Program) FanoutResults {
	results := make(FanoutResults, len(programs))
	if len(programs) == 0 {
		return results
	}
	if ctx == nil {
		err := errors.New("context can not be nil")
		for name := range programs {
			results[name] = FanoutResult{Err: err, Complete: true}
		}
		return results
	}
	if ctx.Err() != nil {
		// The context has already ended: every name is recorded as incomplete
		// without launching any goroutines.
		for name := range programs {
			results[name] = FanoutResult{}
		}
		return results
	}

	names := make([]string, 0, len(programs))
	slots := make([]FanoutResult, len(programs))
	for name := range programs {
		names = append(names, name)
	}

	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			slots[i] = fanoutOne(ctx, input, programs[name])
		}(i, name)
	}
	wg.Wait()

	for i, name := range names {
		results[name] = slots[i]
	}
	return results
}

// fanoutOne drives a single program's ConcurrentEval and collects its outcome.
// It reports an incomplete result if the context ends before the program settles
// or if the program's result channel closes without delivering a result.
func fanoutOne(ctx context.Context, input any, prg Program) FanoutResult {
	resCh := prg.ConcurrentEval(ctx, input)
	select {
	case res, ok := <-resCh:
		return settleFanoutResult(ctx, res, ok)
	case <-ctx.Done():
		// The context ended while the program was still in flight. One last
		// non-blocking check in case the result settled concurrently.
		select {
		case res, ok := <-resCh:
			return settleFanoutResult(ctx, res, ok)
		default:
			return FanoutResult{}
		}
	}
}

// settleFanoutResult converts a program's EvalResult into a FanoutResult. A closed
// channel, or a result whose error matches the context's own termination cause, means
// the program was cut short by the context ending rather than settling on its own,
// and is recorded as incomplete.
func settleFanoutResult(ctx context.Context, res EvalResult, ok bool) FanoutResult {
	if !ok {
		return FanoutResult{}
	}
	if ctx.Err() != nil && errors.Is(res.Err, ctx.Err()) {
		return FanoutResult{}
	}
	return FanoutResult{
		Val:         res.Val,
		EvalDetails: res.EvalDetails,
		Err:         res.Err,
		Complete:    true,
	}
}
