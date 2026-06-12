package health

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// States is a fixed set of owned statuses, each starting as Unknown.
// Must be created by NewStates.
// It intentionally has no Health() method (see "Isolation by inversion"):
// the read-only view is reachable only via Provider().
type States struct {
	mu      sync.Mutex
	latched bool

	entries map[ServiceName]*Status
	agg     Status // Aggregate state.

	watchers   map[ServiceName][]chan Status
	onTransCbs map[ServiceName][]func(ServingStatus)
}

// NewStates creates a States with the given service names.
// Panics on zero names and on duplicate, empty, or "/"-containing names.
func NewStates(names ...ServiceName) *States {
	if len(names) == 0 {
		panic("health: NewStates requires at least one name")
	}
	entries := make(map[ServiceName]*Status, len(names))
	for _, name := range names {
		switch {
		case name == Overall:
			panic("health: NewStates name cannot be empty (Overall)")
		case strings.Contains(string(name), "/"):
			panic(fmt.Sprintf(`health: NewStates name %q contains "/"`, name))
		case entries[name] != nil:
			panic(fmt.Sprintf("health: NewStates duplicate name %q", name))
		}
		entries[name] = &Status{ServingStatus: Unknown}
	}
	return &States{
		entries:    entries,
		watchers:   make(map[ServiceName][]chan Status, len(entries)+1),
		onTransCbs: make(map[ServiceName][]func(ServingStatus), len(entries)+1),
	}
}

// SetServingStatus sets the status for a named service.
// Panics on a name that was not passed to NewStates.
// Setting the current status again is a no-op:
// no watcher wakeup, no transition counter increment.
func (s *States) SetServingStatus(name ServiceName, st ServingStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.latched {
		return
	}

	e, ok := s.entries[name]
	if !ok {
		panic(fmt.Sprintf("health: SetServingStatus for unknown service name %q", name))
	}
	if !e.update(st) {
		return
	}

	notifyWatchers(s.watchers[name], *e)
	fireCallbacks(s.onTransCbs[name], e.ServingStatus)

	// Recompute aggregate.
	if s.agg.update(s.computeAggregate()) {
		notifyWatchers(s.watchers[Overall], s.agg)
		fireCallbacks(s.onTransCbs[Overall], s.agg.ServingStatus)
	}
}

// Close sets every name to NotServing and latches the whole States
// (same semantics as State.Close).
func (s *States) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.latched {
		return
	}
	s.latched = true

	for name, e := range s.entries {
		if !e.update(NotServing) {
			continue
		}
		notifyWatchers(s.watchers[name], *e)
		fireCallbacks(s.onTransCbs[name], e.ServingStatus)
	}

	if s.agg.update(NotServing) {
		notifyWatchers(s.watchers[Overall], s.agg)
		fireCallbacks(s.onTransCbs[Overall], s.agg.ServingStatus)
	}
}

// Provider returns a read-only view of this States.
func (s *States) Provider() HealthProvider {
	return &statesView{s: s}
}

// onTransition registers f for the aggregate Overall.
func (s *States) onTransition(f func(ServingStatus)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f(s.agg.ServingStatus)
	s.onTransCbs[Overall] = append(s.onTransCbs[Overall], f)
}

// computeAggregate folds member statuses.
// NotServing wins, then Unknown, then Serving.
// Must be called under s.mu.
func (s *States) computeAggregate() ServingStatus {
	anyUnknown := false
	for _, e := range s.entries {
		if e.ServingStatus == NotServing {
			return NotServing
		}
		if e.ServingStatus == Unknown {
			anyUnknown = true
		}
	}
	if anyUnknown {
		return Unknown
	}
	return Serving
}

// statesView is the read-only view of a States.
type statesView struct {
	s *States
}

func (v *statesView) Health() Health { return v }

// onTransition delegates to the writer.
func (v *statesView) onTransition(f func(ServingStatus)) {
	v.s.onTransition(f)
}

func (v *statesView) Check(_ context.Context, service ServiceName) ServingStatus {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()

	if service == Overall {
		return v.s.agg.ServingStatus
	}

	e, ok := v.s.entries[service]
	if !ok {
		return ServiceUnknown
	}
	return e.ServingStatus
}

func (v *statesView) List(_ context.Context) map[ServiceName]Status {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()

	m := make(map[ServiceName]Status, len(v.s.entries)+1)
	m[Overall] = v.s.agg
	for name, e := range v.s.entries {
		m[name] = *e
	}
	return m
}

func (v *statesView) Watch(ctx context.Context, service ServiceName) <-chan Status {
	ch := make(chan Status, 1)

	if _, ok := v.s.entries[service]; service != Overall && !ok {
		ch <- Status{ServingStatus: ServiceUnknown}
		context.AfterFunc(ctx, func() { close(ch) })
		return ch
	}

	v.s.mu.Lock()
	if service == Overall {
		ch <- v.s.agg
	} else {
		e := v.s.entries[service]
		ch <- *e
	}
	v.s.watchers[service] = append(v.s.watchers[service], ch)
	v.s.mu.Unlock()

	context.AfterFunc(ctx, func() {
		v.s.mu.Lock()
		defer v.s.mu.Unlock()

		match := func(w chan Status) bool { return w == ch }
		v.s.watchers[service] = slices.DeleteFunc(v.s.watchers[service], match)

		close(ch)
	})

	return ch
}
