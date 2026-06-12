package health_test

import (
	"context"
	"fmt"

	"github.com/powerman/health"
)

// Subsystems reported by Repo's health.
// The subsys prefix keeps these identifiers clear of
// the variables holding the subsystems themselves (db, cache, …).
const (
	subsysDB        health.ServiceName = "db"
	subsysMigration health.ServiceName = "migration"
)

// Repo embeds HealthProvider, so any caller can read repo.Health();
// statuses can be set only by Repo itself, through the private writer.
type Repo struct {
	health.HealthProvider

	hc *health.States
}

func NewRepo() *Repo {
	// Every subsystem starts as Unknown, so Overall is Unknown too.
	hc := health.NewStates(subsysDB, subsysMigration)
	return &Repo{HealthProvider: hc.Provider(), hc: hc}
}

func Example_quickStart() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := NewRepo()
	fmt.Println(repo.Health().Check(ctx, health.Overall))

	// The owner reports transitions from wherever it learns about them.
	repo.hc.SetServingStatus(subsysDB, health.Serving)        // Overall: still Unknown.
	repo.hc.SetServingStatus(subsysMigration, health.Serving) // Overall: Serving.

	// WaitSettled discards Unknown and returns the first settled status.
	ch := repo.Health().Watch(ctx, health.Overall)
	st, err := health.WaitSettled(ctx, ch)
	fmt.Println(st, err)

	// Output:
	// Unknown
	// Serving <nil>
}

// Listener embeds HealthProvider and wraps health.State like its own status.
// Unlike Repo which tracks multiple named subsystems via health.States,
// Listener tracks a single implicit service (Overall) via health.State.
type Listener struct {
	health.HealthProvider

	hc *health.State
}

func NewListener() *Listener {
	// &health.State{} is ready to use — no constructor call needed.
	hc := &health.State{}
	return &Listener{HealthProvider: hc.Provider(), hc: hc}
}

// ExampleState shows the recommended embedding pattern
// for a single-service health tracker.
func ExampleState() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := NewListener()
	fmt.Println(l.Health().Check(ctx, health.Overall))

	l.hc.SetServingStatus(health.Serving)

	ch := l.Health().Watch(ctx, health.Overall)
	st, err := health.WaitSettled(ctx, ch)
	fmt.Println(st, err)

	// Output:
	// Unknown
	// Serving <nil>
}
