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

package cel_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
)

func mustCompileProgram(t *testing.T, env *cel.Env, expr string) cel.Program {
	t.Helper()
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		t.Fatalf("env.Compile(%q) failed: %v", expr, iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("env.Program(%q) failed: %v", expr, err)
	}
	return prg
}

func concurrentEvalValue(t *testing.T, prg cel.Program, input any) any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, ok := <-prg.ConcurrentEval(ctx, input)
	if !ok {
		t.Fatal("ConcurrentEval() channel closed without a result")
	}
	if res.Err != nil {
		t.Fatalf("ConcurrentEval() failed: %v", res.Err)
	}
	return res.Val.Value()
}

func TestFanoutEvalAllSuccess(t *testing.T) {
	env, err := cel.NewEnv(cel.Variable("x", cel.IntType))
	if err != nil {
		t.Fatalf("cel.NewEnv() failed: %v", err)
	}
	add := mustCompileProgram(t, env, `x + 1`)
	mul := mustCompileProgram(t, env, `x * 2`)
	input := map[string]any{"x": 10}
	programs := map[string]cel.Program{
		"add":       add,
		"mul":       mul,
		"add_again": add, // same Program instance under a second name
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := cel.FanoutEval(ctx, input, programs)

	if len(results) != len(programs) {
		t.Fatalf("FanoutEval() returned %d results, wanted %d", len(results), len(programs))
	}
	for name, prg := range programs {
		res, found := results[name]
		if !found {
			t.Errorf("FanoutEval() missing result for %q", name)
			continue
		}
		if !res.Complete {
			t.Errorf("FanoutEval() result %q incomplete, wanted complete", name)
			continue
		}
		if res.Err != nil {
			t.Errorf("FanoutEval() result %q got error: %v", name, res.Err)
			continue
		}
		want := concurrentEvalValue(t, prg, input)
		if got := res.Val.Value(); got != want {
			t.Errorf("FanoutEval() result %q got %v, wanted %v", name, got, want)
		}
	}
}

func TestFanoutEvalProgramError(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Variable("x", cel.IntType),
		cel.Variable("denom", cel.IntType),
	)
	if err != nil {
		t.Fatalf("cel.NewEnv() failed: %v", err)
	}
	programs := map[string]cel.Program{
		"ok":       mustCompileProgram(t, env, `x + 1`),
		"div_zero": mustCompileProgram(t, env, `x / denom`),
		"ok_again": mustCompileProgram(t, env, `x * 2`),
	}
	input := map[string]any{"x": 10, "denom": 0}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := cel.FanoutEval(ctx, input, programs)

	if len(results) != len(programs) {
		t.Fatalf("FanoutEval() returned %d results, wanted %d", len(results), len(programs))
	}
	bad, found := results["div_zero"]
	if !found {
		t.Fatal("FanoutEval() missing result for \"div_zero\"")
	}
	if !bad.Complete {
		t.Error("FanoutEval() result \"div_zero\" incomplete, wanted a completed failure")
	}
	if bad.Err == nil {
		t.Error("FanoutEval() result \"div_zero\" got nil error, wanted division by zero")
	}
	for _, name := range []string{"ok", "ok_again"} {
		res, found := results[name]
		if !found {
			t.Errorf("FanoutEval() missing result for %q", name)
			continue
		}
		if !res.Complete || res.Err != nil {
			t.Errorf("FanoutEval() result %q got (complete=%v, err=%v), wanted success", name, res.Complete, res.Err)
		}
	}
	if got := results["ok"].Val.Value(); got != int64(11) {
		t.Errorf("FanoutEval() result \"ok\" got %v, wanted 11", got)
	}
	if got := results["ok_again"].Val.Value(); got != int64(20) {
		t.Errorf("FanoutEval() result \"ok_again\" got %v, wanted 20", got)
	}
}

func TestFanoutEvalContextCancelMidFlight(t *testing.T) {
	settled := make(chan struct{})
	var markOnce sync.Once
	env, err := cel.NewEnv(
		cel.Function("mark",
			cel.Overload("mark_bool", []*cel.Type{}, cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					markOnce.Do(func() { close(settled) })
					return types.True
				}),
			),
		),
		cel.Function("slow",
			cel.Overload("slow_bool", []*cel.Type{}, cel.BoolType,
				cel.AsyncBinding(func(ctx context.Context, args ...ref.Val) ref.Val {
					<-ctx.Done()
					return types.True
				}),
			),
		),
	)
	if err != nil {
		t.Fatalf("cel.NewEnv() failed: %v", err)
	}
	programs := map[string]cel.Program{
		"fast": mustCompileProgram(t, env, `mark()`),
		"slow": mustCompileProgram(t, env, `slow()`),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		results cel.FanoutResults
	}
	done := make(chan outcome, 1)
	go func() {
		done <- outcome{results: cel.FanoutEval(ctx, map[string]any{}, programs)}
	}()

	// Wait for the fast program to evaluate, give its result a moment to settle,
	// then cancel the context while the slow program is still in flight.
	<-settled
	time.Sleep(100 * time.Millisecond)
	cancel()

	var results cel.FanoutResults
	select {
	case o := <-done:
		results = o.results
	case <-time.After(5 * time.Second):
		t.Fatal("FanoutEval() did not return after context cancellation")
	}

	if len(results) != len(programs) {
		t.Fatalf("FanoutEval() returned %d results, wanted %d", len(results), len(programs))
	}
	fast := results["fast"]
	if !fast.Complete || fast.Err != nil {
		t.Errorf("FanoutEval() result \"fast\" got (complete=%v, err=%v), wanted settled success", fast.Complete, fast.Err)
	} else if got := fast.Val.Value(); got != true {
		t.Errorf("FanoutEval() result \"fast\" got %v, wanted true", got)
	}
	if slow := results["slow"]; slow.Complete {
		t.Errorf("FanoutEval() result \"slow\" got complete, wanted incomplete after context cancellation")
	}
}

func TestFanoutEvalPreCancelledContext(t *testing.T) {
	env, err := cel.NewEnv(cel.Variable("x", cel.IntType))
	if err != nil {
		t.Fatalf("cel.NewEnv() failed: %v", err)
	}
	programs := map[string]cel.Program{
		"add": mustCompileProgram(t, env, `x + 1`),
		"mul": mustCompileProgram(t, env, `x * 2`),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results := cel.FanoutEval(ctx, map[string]any{"x": 10}, programs)

	if len(results) != len(programs) {
		t.Fatalf("FanoutEval() returned %d results, wanted %d", len(results), len(programs))
	}
	for name, res := range results {
		if res.Complete {
			t.Errorf("FanoutEval() result %q got complete, wanted incomplete for a pre-cancelled context", name)
		}
	}
}

func TestFanoutEvalEmpty(t *testing.T) {
	results := cel.FanoutEval(context.Background(), map[string]any{}, map[string]cel.Program{})
	if results == nil {
		t.Error("FanoutEval() with empty programs got nil results, wanted an empty result set")
	}
	if len(results) != 0 {
		t.Errorf("FanoutEval() with empty programs got %d results, wanted 0", len(results))
	}

	results = cel.FanoutEval(context.Background(), nil, nil)
	if results == nil || len(results) != 0 {
		t.Errorf("FanoutEval() with nil programs got %v, wanted an empty result set", results)
	}
}
