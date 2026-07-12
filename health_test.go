package health_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/powerman/check"

	"github.com/powerman/health"
)

var baseCtx = context.Background()

func TestMain(m *testing.M) {
	check.TestMain(m)
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

func TestStateZeroValue(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := &health.State{}
	v := s.Provider()

	// Zero-value State reports Unknown.
	t.Equal(v.Health().Check(baseCtx, health.Overall), health.Unknown)

	// Zero-value State is ready to use.
	s.SetServingStatus(health.Serving)
	t.Equal(v.Health().Check(baseCtx, health.Overall), health.Serving)
}

func TestStateSetServingStatus(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := &health.State{}
	v := s.Provider()

	ctx := baseCtx
	t.Equal(v.Health().Check(ctx, health.Overall), health.Unknown)

	s.SetServingStatus(health.Serving)
	t.Equal(v.Health().Check(ctx, health.Overall), health.Serving)
	list := v.Health().List(ctx)
	t.Equal(list[health.Overall].ServingStatus, health.Serving)
	t.Equal(list[health.Overall].Transitions, uint64(1))
	t.NotZero(list[health.Overall].Since)

	// Coalescing: same status is no-op.
	s.SetServingStatus(health.Serving)
	list2 := v.Health().List(ctx)
	t.Equal(list2[health.Overall].Transitions, uint64(1))
	t.True(list2[health.Overall].Since.Equal(list[health.Overall].Since))

	// Effective transition.
	s.SetServingStatus(health.NotServing)
	t.Equal(v.Health().Check(ctx, health.Overall), health.NotServing)
	list3 := v.Health().List(ctx)
	t.Equal(list3[health.Overall].Transitions, uint64(2))
}

func TestStateClose(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := &health.State{}
	v := s.Provider()

	s.SetServingStatus(health.Serving)
	t.Equal(v.Health().Check(baseCtx, health.Overall), health.Serving)

	s.Close()
	t.Equal(v.Health().Check(baseCtx, health.Overall), health.NotServing)

	// SetServingStatus is silently ignored after Close.
	s.SetServingStatus(health.Serving)
	t.Equal(v.Health().Check(baseCtx, health.Overall), health.NotServing)

	// Close is idempotent.
	s.Close()
	t.Equal(v.Health().Check(baseCtx, health.Overall), health.NotServing)
}

func TestStateCheckUnknownService(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := &health.State{}
	v := s.Provider()

	t.Equal(v.Health().Check(baseCtx, "unknown"), health.ServiceUnknown)
}

func TestStateWatchImmediate(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := &health.State{}
	s.SetServingStatus(health.Serving)

	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	ch := s.Provider().Health().Watch(ctx, health.Overall)
	st := <-ch
	t.Equal(st.ServingStatus, health.Serving)
	t.Equal(st.Transitions, uint64(1))
}

func TestStateWatchChanges(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := &health.State{}

	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	ch := s.Provider().Health().Watch(ctx, health.Overall)
	st := <-ch
	t.Equal(st.ServingStatus, health.Unknown)

	s.SetServingStatus(health.Serving)
	st = <-ch
	t.Equal(st.ServingStatus, health.Serving)
	t.Equal(st.Transitions, uint64(1))

	s.SetServingStatus(health.NotServing)
	st = <-ch
	t.Equal(st.ServingStatus, health.NotServing)
	t.Equal(st.Transitions, uint64(2))
}

func TestStateWatchConflation(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := &health.State{}

	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	ch := s.Provider().Health().Watch(ctx, health.Overall)
	<-ch // drain initial Unknown.

	// Rapid transitions — receiver doesn't read.
	s.SetServingStatus(health.Serving)
	s.SetServingStatus(health.NotServing)
	s.SetServingStatus(health.Serving)

	// Read — should get the latest.
	st := <-ch
	t.Equal(st.ServingStatus, health.Serving)
	// Gap means conflation.
	t.Greater(st.Transitions, uint64(1))
}

func TestStateWatchUnknownName(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(tt *testing.T) {
		t := check.Must(tt)

		s := &health.State{}
		ctx, cancel := context.WithCancel(tt.Context())

		ch := s.Provider().Health().Watch(ctx, "unknown")
		st := <-ch
		t.Equal(st.ServingStatus, health.ServiceUnknown)

		// Inside synctest bubble time advances only when all goroutines
		// are durably blocked, so this select is deterministic.
		select {
		case <-ch:
			t.Fatal("channel should stay open")
		case <-time.After(time.Hour):
		}

		cancel()
		synctest.Wait()
		_, ok := <-ch
		t.False(ok)
	})
}

func TestStateCloseNotifiesWatchers(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := &health.State{}
	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	ch := s.Provider().Health().Watch(ctx, health.Overall)
	<-ch // drain initial.

	s.Close()
	st := <-ch
	t.Equal(st.ServingStatus, health.NotServing)
}

func TestWatchEmptyContext(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(tt *testing.T) {
		t := check.Must(tt)

		s := &health.State{}
		s.SetServingStatus(health.Serving)

		ctx, cancel := context.WithCancel(tt.Context())
		cancel()

		ch := s.Provider().Health().Watch(ctx, health.Overall)
		st, ok := <-ch
		t.True(ok)
		t.Equal(st.ServingStatus, health.Serving)

		synctest.Wait()
		_, ok = <-ch
		t.False(ok)
	})
}

// ---------------------------------------------------------------------------
// States
// ---------------------------------------------------------------------------

func TestNewStatesPanics(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	t.PanicMatch(func() { health.NewStates() }, "at least one name")
	t.PanicMatch(func() { health.NewStates(health.Overall) }, "cannot be empty")
	t.PanicMatch(func() { health.NewStates("a/b") }, `contains "/"`)
	t.PanicMatch(func() { health.NewStates("a", "a") }, "duplicate")
}

func TestStatesSetServingStatusPanics(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := health.NewStates("db")
	t.PanicMatch(func() { s.SetServingStatus("unknown", health.Serving) }, "unknown service")
}

func TestStatesAggregation(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := health.NewStates("a", "b")
	v := s.Provider()
	ctx := baseCtx

	t.Equal(v.Health().Check(ctx, health.Overall), health.Unknown)

	s.SetServingStatus("a", health.Serving)
	t.Equal(v.Health().Check(ctx, health.Overall), health.Unknown) // b still Unknown.

	s.SetServingStatus("b", health.Serving)
	t.Equal(v.Health().Check(ctx, health.Overall), health.Serving)

	s.SetServingStatus("b", health.NotServing)
	t.Equal(v.Health().Check(ctx, health.Overall), health.NotServing)

	// Recovery.
	s.SetServingStatus("b", health.Serving)
	t.Equal(v.Health().Check(ctx, health.Overall), health.Serving)
}

func TestStatesCoalescing(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := health.NewStates("a")
	v := s.Provider()

	s.SetServingStatus("a", health.Serving)
	list := v.Health().List(baseCtx)
	trans := list["a"].Transitions

	// Same status: no-op.
	s.SetServingStatus("a", health.Serving)
	list2 := v.Health().List(baseCtx)
	t.Equal(list2["a"].Transitions, trans)
}

func TestStatesClose(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := health.NewStates("a", "b")
	v := s.Provider()

	s.SetServingStatus("a", health.Serving)
	s.SetServingStatus("b", health.Serving)

	s.Close()
	t.Equal(v.Health().Check(baseCtx, "a"), health.NotServing)
	t.Equal(v.Health().Check(baseCtx, "b"), health.NotServing)
	t.Equal(v.Health().Check(baseCtx, health.Overall), health.NotServing)

	s.SetServingStatus("a", health.Serving)
	t.Equal(v.Health().Check(baseCtx, "a"), health.NotServing)

	s.Close() // idempotent
}

func TestStatesWatch(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := health.NewStates("db", "cache")
	v := s.Provider()

	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	chDB := v.Health().Watch(ctx, "db")
	st := <-chDB
	t.Equal(st.ServingStatus, health.Unknown)

	s.SetServingStatus("db", health.Serving)
	st = <-chDB
	t.Equal(st.ServingStatus, health.Serving)

	chOverall := v.Health().Watch(ctx, health.Overall)
	st = <-chOverall
	t.Equal(st.ServingStatus, health.Unknown) // cache still Unknown.

	s.SetServingStatus("cache", health.Serving)
	st = <-chOverall
	t.Equal(st.ServingStatus, health.Serving)
}

func TestStatesWatchUnknownName(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(tt *testing.T) {
		t := check.Must(tt)

		s := health.NewStates("db")
		v := s.Provider()
		ctx, cancel := context.WithCancel(tt.Context())

		ch := v.Health().Watch(ctx, "unknown")
		st := <-ch
		t.Equal(st.ServingStatus, health.ServiceUnknown)

		select {
		case <-ch:
			t.Fatal("should stay open")
		case <-time.After(time.Hour):
		}

		cancel()
		synctest.Wait()
		_, ok := <-ch
		t.False(ok)
	})
}

func TestStatesTransitionsTracking(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := health.NewStates("a")
	v := s.Provider()

	s.SetServingStatus("a", health.Serving)
	list := v.Health().List(baseCtx)
	t.Equal(list["a"].Transitions, uint64(1))
	t.Equal(list[health.Overall].Transitions, uint64(1))

	s.SetServingStatus("a", health.NotServing)
	list = v.Health().List(baseCtx)
	t.Equal(list["a"].Transitions, uint64(2))
	t.Equal(list[health.Overall].Transitions, uint64(2))
}

func TestStatesCloseNotifiesWatchers(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := health.NewStates("db", "cache")
	v := s.Provider()

	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	chOverall := v.Health().Watch(ctx, health.Overall)
	<-chOverall // drain initial.

	chDB := v.Health().Watch(ctx, "db")
	<-chDB // drain initial.

	s.Close()

	st := <-chOverall
	t.Equal(st.ServingStatus, health.NotServing)

	st = <-chDB
	t.Equal(st.ServingStatus, health.NotServing)
}

// ---------------------------------------------------------------------------
// AlwaysServing
// ---------------------------------------------------------------------------

func TestAlwaysServing(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	var as health.AlwaysServing
	h := as.Health()

	t.Equal(h.Check(baseCtx, health.Overall), health.Serving)
	t.Equal(h.Check(baseCtx, "any"), health.ServiceUnknown)

	list := h.List(baseCtx)
	t.Equal(list[health.Overall].ServingStatus, health.Serving)
	t.Equal(list[health.Overall].Transitions, uint64(0))
	t.True(list[health.Overall].Since.IsZero())

	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	ch := h.Watch(ctx, health.Overall)
	st := <-ch
	t.Equal(st.ServingStatus, health.Serving)

	ch2 := h.Watch(ctx, "unknown")
	st2 := <-ch2
	t.Equal(st2.ServingStatus, health.ServiceUnknown)
}

// ---------------------------------------------------------------------------
// AlwaysServing embedding as HealthProvider
// ---------------------------------------------------------------------------

type testComponent struct {
	health.HealthProvider
}

func newTestComponent() *testComponent {
	return &testComponent{HealthProvider: health.AlwaysServing{}}
}

func TestAlwaysServingEmbedding(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	c := newTestComponent()
	h := c.Health()
	t.Equal(h.Check(baseCtx, health.Overall), health.Serving)
}

// A constructor that forgets to assign the embedded HealthProvider
// leaves a nil interface — first use panics, which is loud enough.
func TestNilHealthProviderPanics(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	var c testComponent
	t.Panic(func() { _ = c.Health() })
}

// ---------------------------------------------------------------------------
// NewAggregate — materialized mode
// ---------------------------------------------------------------------------

func TestNewAggregatePanics(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	t.PanicMatch(func() { health.NewAggregate(nil) }, "at least one child")
	t.PanicMatch(func() { health.NewAggregate(make(map[health.ServiceName]health.Health)) }, "at least one child")

	var as health.AlwaysServing
	t.PanicMatch(func() {
		health.NewAggregate(map[health.ServiceName]health.Health{
			health.Overall: as.Health(),
		})
	}, "cannot be empty")
	t.PanicMatch(func() {
		health.NewAggregate(map[health.ServiceName]health.Health{
			"a/b": as.Health(),
		})
	}, `contains "/"`)
}

func TestNewAggregateMaterialized(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	db := &health.State{}
	cache := &health.State{}

	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"db":    db.Provider().Health(),
		"cache": cache.Provider().Health(),
	})
	h := agg.Health()
	ctx := baseCtx

	t.Equal(h.Check(ctx, health.Overall), health.Unknown)

	db.SetServingStatus(health.Serving)
	t.Equal(h.Check(ctx, health.Overall), health.Unknown) // cache still Unknown.

	cache.SetServingStatus(health.Serving)
	t.Equal(h.Check(ctx, health.Overall), health.Serving)

	db.SetServingStatus(health.NotServing)
	t.Equal(h.Check(ctx, health.Overall), health.NotServing)

	db.SetServingStatus(health.Serving)
	t.Equal(h.Check(ctx, health.Overall), health.Serving)
}

func TestNewAggregateMaterializedTransitions(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	db := &health.State{}
	cache := &health.State{}

	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"db":    db.Provider().Health(),
		"cache": cache.Provider().Health(),
	})
	h := agg.Health()
	ctx := baseCtx

	db.SetServingStatus(health.Serving)
	cache.SetServingStatus(health.Serving)

	list := h.List(ctx)
	t.Equal(list[health.Overall].ServingStatus, health.Serving)
	t.Equal(list[health.Overall].Transitions, uint64(1))

	db.SetServingStatus(health.NotServing)
	list = h.List(ctx)
	t.Equal(list[health.Overall].ServingStatus, health.NotServing)
	t.Equal(list[health.Overall].Transitions, uint64(2))
}

func TestNewAggregateMaterializedSeeding(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	db := &health.State{}
	db.SetServingStatus(health.Serving)

	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"db": db.Provider().Health(),
	})
	h := agg.Health()

	t.Equal(h.Check(baseCtx, health.Overall), health.Serving)
	list := h.List(baseCtx)
	t.Equal(list["db"].ServingStatus, health.Serving)
}

func TestNewAggregateSubPopulation(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	db := &health.State{}
	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"db": db.Provider().Health(),
	})
	h := agg.Health()

	list := h.List(baseCtx)
	t.NotNil(list["db"].Sub)
	t.Nil(list[health.Overall].Sub)
}

func TestNewAggregateCheckMember(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	db := &health.State{}
	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"db": db.Provider().Health(),
	})
	h := agg.Health()

	t.Equal(h.Check(baseCtx, "db"), health.Unknown)
	db.SetServingStatus(health.Serving)
	t.Equal(h.Check(baseCtx, "db"), health.Serving)
	t.Equal(h.Check(baseCtx, "unknown"), health.ServiceUnknown)
}

func TestNewAggregateWatchMember(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	db := &health.State{}
	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"db": db.Provider().Health(),
	})
	h := agg.Health()

	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	ch := h.Watch(ctx, "db")
	st := <-ch
	t.Equal(st.ServingStatus, health.Unknown)

	db.SetServingStatus(health.Serving)
	st = <-ch
	t.Equal(st.ServingStatus, health.Serving)
}

func TestNewAggregateWatchUnknownName(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	db := &health.State{}
	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"db": db.Provider().Health(),
	})
	h := agg.Health()

	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	ch := h.Watch(ctx, "unknown")
	st := <-ch
	t.Equal(st.ServingStatus, health.ServiceUnknown)
}

func TestNewAggregateNilChild(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	t.PanicMatch(func() {
		health.NewAggregate(map[health.ServiceName]health.Health{
			"db": nil,
		})
	}, "nil child")
}

// ---------------------------------------------------------------------------
// NewAggregate — nested materialized aggregates (two levels)
// ---------------------------------------------------------------------------

func TestNewAggregateTwoLevelTransitions(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	db := &health.State{}
	cache := &health.State{}

	inner := health.NewAggregate(map[health.ServiceName]health.Health{
		"db":    db.Provider().Health(),
		"cache": cache.Provider().Health(),
	})
	outer := health.NewAggregate(map[health.ServiceName]health.Health{
		"inner": inner.Health(),
	})
	h := outer.Health()
	ctx := baseCtx

	t.Equal(h.Check(ctx, health.Overall), health.Unknown)

	db.SetServingStatus(health.Serving)
	t.Equal(h.Check(ctx, health.Overall), health.Unknown) // cache still Unknown

	cache.SetServingStatus(health.Serving)
	t.Equal(h.Check(ctx, health.Overall), health.Serving)

	// Transitions are tracked across two levels.
	list := h.List(ctx)
	t.Equal(list[health.Overall].ServingStatus, health.Serving)
	t.Equal(list[health.Overall].Transitions, uint64(1))

	db.SetServingStatus(health.NotServing)
	t.Equal(h.Check(ctx, health.Overall), health.NotServing)
	list = h.List(ctx)
	t.Equal(list[health.Overall].Transitions, uint64(2))

	// Recovery.
	db.SetServingStatus(health.Serving)
	t.Equal(h.Check(ctx, health.Overall), health.Serving)
	list = h.List(ctx)
	t.Equal(list[health.Overall].Transitions, uint64(3))
}

// ---------------------------------------------------------------------------
// NewAggregate — materialized fold coalescing
// ---------------------------------------------------------------------------

func TestNewAggregateMaterializedFoldCoalescing(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	a := &health.State{}
	b := &health.State{}
	b.SetServingStatus(health.NotServing)

	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"a": a.Provider().Health(),
		"b": b.Provider().Health(),
	})
	h := agg.Health()

	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	ch := h.Watch(ctx, health.Overall)
	st := <-ch
	t.Equal(st.ServingStatus, health.NotServing)

	// Change a from Unknown to Serving — aggregate stays NotServing (b dominates).
	a.SetServingStatus(health.Serving)
	// Notification is synchronous: if an event had fired,
	// it would already be in the buffered channel.
	select {
	case <-ch:
		t.Fatal("should not fire — aggregate stays NotServing")
	default:
	}
}

// ---------------------------------------------------------------------------
// Watch channels stay open after Close
// ---------------------------------------------------------------------------

func TestStateWatchStaysOpenAfterClose(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(tt *testing.T) {
		t := check.Must(tt)

		s := &health.State{}
		s.SetServingStatus(health.Serving)
		ctx, cancel := context.WithCancel(tt.Context())

		ch := s.Provider().Health().Watch(ctx, health.Overall)
		st := <-ch
		t.Equal(st.ServingStatus, health.Serving)

		// Close sends NotServing but does not close the channel.
		s.Close()
		st = <-ch
		t.Equal(st.ServingStatus, health.NotServing)

		// Channel stays open after Close (no more events, but not closed).
		select {
		case _, ok := <-ch:
			if !ok {
				t.Fatal("channel should stay open after Close")
			}
			t.Fatal("unexpected event after Close")
		case <-time.After(time.Hour):
		}

		// Channel closes only when ctx is done.
		cancel()
		synctest.Wait()
		_, ok := <-ch
		t.False(ok)
	})
}

func TestStatesWatchStaysOpenAfterClose(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(tt *testing.T) {
		t := check.Must(tt)

		s := health.NewStates("a")
		s.SetServingStatus("a", health.Serving)
		ctx, cancel := context.WithCancel(tt.Context())

		ch := s.Provider().Health().Watch(ctx, health.Overall)
		st := <-ch
		t.Equal(st.ServingStatus, health.Serving)

		s.Close()
		st = <-ch
		t.Equal(st.ServingStatus, health.NotServing)

		// Channel stays open after Close.
		select {
		case _, ok := <-ch:
			if !ok {
				t.Fatal("channel should stay open after Close")
			}
			t.Fatal("unexpected event after Close")
		case <-time.After(time.Hour):
		}

		cancel()
		synctest.Wait()
		_, ok := <-ch
		t.False(ok)
	})
}

// ---------------------------------------------------------------------------
// NewAggregate — mirror consistency under -race
// ---------------------------------------------------------------------------

func TestNewAggregateMaterializedConcurrent(tt *testing.T) {
	tt.Parallel()

	a := &health.State{}
	b := &health.State{}

	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"a": a.Provider().Health(),
		"b": b.Provider().Health(),
	})
	h := agg.Health()

	ctx, cancel := context.WithCancel(baseCtx)

	var wg sync.WaitGroup

	wg.Go(func() {
		ch := h.Watch(ctx, health.Overall)
		for range ch {
		}
	})

	wg.Go(func() {
		for range 100 {
			a.SetServingStatus(health.Serving)
			a.SetServingStatus(health.NotServing)
			b.SetServingStatus(health.Serving)
			b.SetServingStatus(health.NotServing)
			_ = h.Check(ctx, health.Overall)
			_ = h.List(ctx)
		}
		a.Close()
		b.Close()
		cancel()
	})

	wg.Wait()
}

// ---------------------------------------------------------------------------
// NewAggregate — Close propagation
// ---------------------------------------------------------------------------

func TestNewAggregateMaterializedClosePropagation(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	db := &health.State{}
	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"db": db.Provider().Health(),
	})
	h := agg.Health()
	ctx := baseCtx

	db.SetServingStatus(health.Serving)
	t.Equal(h.Check(ctx, health.Overall), health.Serving)

	db.Close()
	t.Equal(h.Check(ctx, health.Overall), health.NotServing)

	// Close is terminal.
	db.SetServingStatus(health.Serving)
	t.Equal(h.Check(ctx, health.Overall), health.NotServing)
}

// ---------------------------------------------------------------------------
// NewAggregate — foreign Health children are rejected
// ---------------------------------------------------------------------------

// foreignHealth implements Health outside the package,
// without the internal notifier contract.
type foreignHealth struct{}

func (foreignHealth) Check(_ context.Context, _ health.ServiceName) health.ServingStatus {
	return health.Serving
}

func (foreignHealth) List(_ context.Context) map[health.ServiceName]health.Status {
	return map[health.ServiceName]health.Status{
		health.Overall: {ServingStatus: health.Serving},
	}
}

func (foreignHealth) Watch(ctx context.Context, _ health.ServiceName) <-chan health.Status {
	ch := make(chan health.Status, 1)
	ch <- health.Status{ServingStatus: health.Serving}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}

func TestNewAggregateForeignChildPanics(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	t.PanicMatch(func() {
		health.NewAggregate(map[health.ServiceName]health.Health{
			"svc": foreignHealth{},
		})
	}, "foreign Health")
}

// An AlwaysServing member must not break the aggregate's materialization.
func TestNewAggregateAlwaysServingMember(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	db := &health.State{}
	var as health.AlwaysServing

	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"db":  db.Provider().Health(),
		"cfg": as.Health(),
	})
	h := agg.Health()
	ctx := baseCtx

	t.Equal(h.Check(ctx, health.Overall), health.Unknown) // db still Unknown.
	t.Equal(h.Check(ctx, "cfg"), health.Serving)

	db.SetServingStatus(health.Serving)
	t.Equal(h.Check(ctx, health.Overall), health.Serving)

	// Overall transitions are tracked (the aggregate is materialized).
	list := h.List(ctx)
	t.Equal(list[health.Overall].Transitions, uint64(1))
}

// ---------------------------------------------------------------------------
// Flatten
// ---------------------------------------------------------------------------

func TestFlattenSingleLevel(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := health.NewStates("a", "b")
	s.SetServingStatus("a", health.Serving)

	flat := health.Flatten(baseCtx, s.Provider().Health())
	t.Equal(flat[health.Overall].ServingStatus, health.Unknown)
	t.Equal(flat["a"].ServingStatus, health.Serving)
	t.Equal(flat["b"].ServingStatus, health.Unknown)
	t.Nil(flat[health.Overall].Sub)
	t.Nil(flat["a"].Sub)
}

func TestFlattenTwoLevel(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	db := &health.State{}
	db.SetServingStatus(health.Serving)

	agg := health.NewAggregate(map[health.ServiceName]health.Health{
		"db": db.Provider().Health(),
	})

	flat := health.Flatten(baseCtx, agg.Health())
	t.Equal(flat[health.Overall].ServingStatus, health.Serving)
	t.Equal(flat["db"].ServingStatus, health.Serving)
	t.Nil(flat[health.Overall].Sub)
	t.Nil(flat["db"].Sub)
	// db's internal Overall is already represented by "db" —
	// there must be no parasitic "db/" key.
	if _, ok := flat["db/"]; ok {
		t.Fatal("unexpected key \"db/\" — child's Overall is already \"db\"")
	}
}

// ---------------------------------------------------------------------------
// WaitSettled
// ---------------------------------------------------------------------------

func TestWaitSettledServing(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(tt *testing.T) {
		t := check.Must(tt)

		s := &health.State{}

		ch := s.Provider().Health().Watch(tt.Context(), health.Overall)
		go func() {
			time.Sleep(time.Hour)
			s.SetServingStatus(health.Serving)
		}()

		// synctest advances virtual time until the sleeping goroutine
		// wakes and sets the status to Serving.
		synctest.Wait()

		st, err := health.WaitSettled(tt.Context(), ch)
		t.Nil(err)
		t.Equal(st, health.Serving)
	})
}

func TestWaitSettledNotServing(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := &health.State{}
	s.SetServingStatus(health.NotServing)

	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	ch := s.Provider().Health().Watch(ctx, health.Overall)
	st, err := health.WaitSettled(ctx, ch)
	t.Nil(err)
	t.Equal(st, health.NotServing)
}

func TestWaitSettledServiceUnknown(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := &health.State{}
	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	ch := s.Provider().Health().Watch(ctx, "unknown")
	st, err := health.WaitSettled(ctx, ch)
	t.Nil(err)
	t.Equal(st, health.ServiceUnknown)
}

func TestWaitSettledContextCancelled(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	ch := make(chan health.Status)
	ctx, cancel := context.WithCancel(baseCtx)
	cancel()

	_, err := health.WaitSettled(ctx, ch)
	t.NotNil(err)
	t.Equal(err, context.Canceled)
}

func TestWaitSettledWatchClosed(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	ch := make(chan health.Status)
	close(ch)

	_, err := health.WaitSettled(baseCtx, ch)
	t.Equal(err, health.ErrWatchClosed)
}

// ---------------------------------------------------------------------------
// Type assertion safety
// ---------------------------------------------------------------------------

func TestStateProviderTypeAssertion(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := &health.State{}
	prov := s.Provider()
	h := prov.Health()

	var anyH any = h
	_, ok := anyH.(*health.State)
	t.False(ok)

	var anyProv any = prov
	_, ok = anyProv.(*health.State)
	t.False(ok)
}

func TestStatesProviderTypeAssertion(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	s := health.NewStates("a")
	prov := s.Provider()
	h := prov.Health()

	var anyH any = h
	_, ok := anyH.(*health.States)
	t.False(ok)

	var anyProv any = prov
	_, ok = anyProv.(*health.States)
	t.False(ok)
}

// ---------------------------------------------------------------------------
// Concurrent access under -race
// ---------------------------------------------------------------------------

func TestStateConcurrent(tt *testing.T) {
	tt.Parallel()

	s := &health.State{}
	ctx, cancel := context.WithCancel(baseCtx)

	var wg sync.WaitGroup

	h := s.Provider().Health()
	wg.Go(func() {
		ch := h.Watch(ctx, health.Overall)
		for range ch {
		}
	})

	wg.Go(func() {
		for range 100 {
			s.SetServingStatus(health.Serving)
			s.SetServingStatus(health.NotServing)
			_ = h.Check(ctx, health.Overall)
			_ = h.List(ctx)
		}
		s.Close()
		cancel()
	})

	wg.Wait()
}

func TestStatesConcurrent(tt *testing.T) {
	tt.Parallel()

	s := health.NewStates("a", "b")
	ctx, cancel := context.WithCancel(baseCtx)

	var wg sync.WaitGroup

	h := s.Provider().Health()
	wg.Go(func() {
		ch := h.Watch(ctx, health.Overall)
		for range ch {
		}
	})

	wg.Go(func() {
		for range 100 {
			s.SetServingStatus("a", health.Serving)
			s.SetServingStatus("b", health.NotServing)
			s.SetServingStatus("a", health.NotServing)
			s.SetServingStatus("b", health.Serving)
			_ = h.Check(ctx, health.Overall)
			_ = h.List(ctx)
		}
		s.Close()
		cancel()
	})

	wg.Wait()
}
