package health

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// NewAggregate combines child Health views into one read-only health:
// {Overall: aggregate, name: child's own Overall, …},
// with Status.Sub of each member entry set to the child itself.
// Check(name) returns the child's Check(Overall).
// Membership is fixed at construction.
//
// Children must originate from this package
// (State/States providers, NewAggregate results, AlwaysServing):
// the aggregate's Overall is materialized through synchronous
// transition propagation, which needs an internal notifier contract.
//
// Panics on an empty map, on invalid names (same rules as NewStates),
// and on nil or foreign Health children —
// bridge a foreign Health with an owner goroutine feeding an own State instead.
//
//nolint:iface // Returning the interface is the API contract: views stay unexported types.
func NewAggregate(children map[ServiceName]Health) HealthProvider {
	if len(children) == 0 {
		panic("health: NewAggregate requires at least one child")
	}
	for name, child := range children {
		switch _, ok := child.(transitionNotifier); true {
		case name == Overall:
			panic("health: NewAggregate name cannot be empty (Overall)")
		case strings.Contains(string(name), "/"):
			panic(fmt.Sprintf(`health: NewAggregate name %q contains "/"`, name))
		case child == nil:
			panic(fmt.Sprintf("health: NewAggregate nil child for name %q", name))
		case !ok:
			panic(fmt.Sprintf("health: NewAggregate foreign Health child %q (bridge it via an owner-side State)", name))
		}
	}

	v := &aggregateView{
		children: maps.Clone(children),
		mirror:   NewStates(slices.Collect(maps.Keys(children))...),
	}
	for name, health := range children {
		health.(transitionNotifier).onTransition(func(st ServingStatus) { //nolint:forcetypeassert // Checked above.
			v.mirror.SetServingStatus(name, st)
		})
	}
	return v
}

// aggregateView is the read-only view returned by NewAggregate.
// Its Overall is materialized through an internal mirror States,
// kept in sync by synchronous callbacks from the children.
type aggregateView struct {
	children map[ServiceName]Health // immutable copy
	mirror   *States
}

func (v *aggregateView) Health() Health { return v }

// onTransition registers a callback on the mirror's Overall.
// It implements transitionNotifier so that nested aggregates
// propagate transitions to their parent aggregates.
func (v *aggregateView) onTransition(f func(ServingStatus)) {
	v.mirror.onTransition(f)
}

func (v *aggregateView) Check(ctx context.Context, service ServiceName) ServingStatus {
	return v.mirror.Provider().Health().Check(ctx, service)
}

func (v *aggregateView) List(ctx context.Context) map[ServiceName]Status {
	m := v.mirror.Provider().Health().List(ctx)
	for name, child := range v.children {
		st := m[name]
		st.Sub = child
		m[name] = st
	}
	return m
}

func (v *aggregateView) Watch(ctx context.Context, service ServiceName) <-chan Status {
	return v.mirror.Provider().Health().Watch(ctx, service)
}
