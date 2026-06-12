package health

import (
	"context"
	"slices"
	"sync"
)

// State is a single owned status.
// The zero value is ready to use and reports Unknown.
// It intentionally has no Health() method (see "Isolation by inversion"):
// the read-only view is reachable only via Provider().
type State struct {
	_       noCopy
	mu      sync.Mutex
	latched bool

	status Status

	watchers   []chan Status
	onTransCbs []func(ServingStatus) // callbacks registered via transitionNotifier
}

// SetServingStatus sets the status.
// Setting the current status again is a no-op:
// no watcher wakeup, no transition counter increment.
func (s *State) SetServingStatus(st ServingStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.latched {
		return
	}
	if !s.status.update(st) {
		return
	}

	notifyWatchers(s.watchers, s.status)
	fireCallbacks(s.onTransCbs, s.status.ServingStatus)
}

// Close sets the status to NotServing and latches it:
// subsequent SetServingStatus calls are silently ignored.
// Idempotent; the NotServing transition is counted and notified as usual.
// Watch channels stay open (they close only when the watcher's ctx is done).
// Close is terminal: watchers observe the latched NotServing and will never see a recovery.
func (s *State) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.latched {
		return
	}
	s.latched = true

	if !s.status.update(NotServing) {
		return
	}

	notifyWatchers(s.watchers, s.status)
	fireCallbacks(s.onTransCbs, s.status.ServingStatus)
}

// Provider returns a read-only view of this State.
func (s *State) Provider() HealthProvider {
	return &stateView{s: s}
}

// onTransition registers f for Overall (the only service in a State).
func (s *State) onTransition(f func(ServingStatus)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f(s.status.ServingStatus)
	s.onTransCbs = append(s.onTransCbs, f)
}

// stateView is the read-only view of a State.
// It implements both Health and HealthProvider.
type stateView struct {
	s *State
}

func (v *stateView) Health() Health { return v }

// onTransition delegates to the writer.
func (v *stateView) onTransition(f func(ServingStatus)) {
	v.s.onTransition(f)
}

func (v *stateView) Check(_ context.Context, service ServiceName) ServingStatus {
	if service != Overall {
		return ServiceUnknown
	}

	v.s.mu.Lock()
	defer v.s.mu.Unlock()

	return v.s.status.ServingStatus
}

func (v *stateView) List(_ context.Context) map[ServiceName]Status {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()

	return map[ServiceName]Status{
		Overall: v.s.status,
	}
}

func (v *stateView) Watch(ctx context.Context, service ServiceName) <-chan Status {
	ch := make(chan Status, 1)

	if service != Overall {
		ch <- Status{ServingStatus: ServiceUnknown}
		context.AfterFunc(ctx, func() { close(ch) })
		return ch
	}

	v.s.mu.Lock()
	ch <- v.s.status
	v.s.watchers = append(v.s.watchers, ch)
	v.s.mu.Unlock()

	context.AfterFunc(ctx, func() {
		v.s.mu.Lock()
		defer v.s.mu.Unlock()

		match := func(w chan Status) bool { return w == ch }
		v.s.watchers = slices.DeleteFunc(v.s.watchers, match)

		close(ch)
	})

	return ch
}
