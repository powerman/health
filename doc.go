// Package health implements a push-based readiness (serving status) model
// where owners report transitions from wherever they learn them
// (migration finished, connection established, listener bound)
// rather than pulling checks on a schedule.
//
// It is NOT a dependency-checking library: the core never runs checks.
// A status is the last observed ability to serve,
// reported by the code that already knows the fact first-hand —
// the code that connected, migrated, or bound the listener —
// at the moment the fact changes.
//
// The model exists because readiness is never a final state:
// a subsystem can degrade and recover at any moment
// (a migration finishes, a schema changes, a dependency goes away and returns),
// so "is this process ready?" needs a dynamically changing status
// per named service plus their aggregate, with change notification —
// not a one-shot check or a "ready" log line.
// One mechanism then answers that question for every consumer:
// production probes (HTTP and gRPC), integration tests
// (WaitSettled instead of port polling), startup ordering,
// and flapping diagnosis (transition counters as data).
// Push is the core because layering works in one direction only:
// an owner can implement a pull check by polling and setting its own status,
// while a pull-based core cannot be turned into a push one.
//
// With zero traffic a dead dependency may keep reporting Serving — accepted:
// readiness gates traffic, and with no traffic there is nothing to gate;
// the first real call updates the status through the owner's own detection.
// Monitoring a dependency's availability is that dependency's monitoring,
// not this process's readiness.
//
// # Status aggregation
//
// The Overall aggregate follows these rules:
//   - NotServing if any member is NotServing;
//   - Unknown if any member is Unknown (and none is NotServing);
//   - Serving otherwise.
//
// There is no "empty is vacuously Serving" rule:
// an owner that exposes health must report something
// (AlwaysServing covers unconditional cases).
//
// # Coalescing writes
//
// Setting the current status again is a no-op:
// no watcher wakeup, no transition counter increment, no Since update.
// Writers may call SetServingStatus from hot paths.
//
// # Watch and loss detection
//
// Watch returns a conflating buffer-1 channel:
// a pending undelivered event is replaced by the newer one.
//   - A receiver always converges to the latest state
//     and is never blocked on.
//   - Intermediate transitions may be skipped;
//     the skip is detectable as a gap in Status.Transitions
//     (consecutive events normally differ by exactly 1).
//   - Status.Transitions is the service's global transition counter
//     in every stream, so the gap arithmetic works uniformly.
//
// # Writer isolation (the Provider pattern)
//
// Writers (State, States) live in unexported fields of the owning object;
// only read-only views are exposed.
// No API may allow mutating someone else's health.
//
//   - Writers do not implement HealthProvider or Health,
//     so a writer cannot be accidentally exposed as a view.
//     The only path outward is writer.Provider(),
//     which returns a dedicated unexported proxy type.
//   - Every returned Health/HealthProvider is an unexported view type,
//     so a type assertion cannot recover write access.
//
// The default style is embedding HealthProvider,
// with service names declared as constants —
// the recommended subsys prefix keeps these identifiers clear of
// the variables holding the subsystems themselves (db, cache, …):
//
//	const (
//	    subsysDB        health.ServiceName = "db"
//	    subsysMigration health.ServiceName = "migration"
//	)
//
//	type Repo struct {
//	    health.HealthProvider
//	    hc *health.States // Private: only Repo can set statuses.
//	}
//
//	func New(/*…*/) *Repo {
//	    hc := health.NewStates(subsysDB, subsysMigration)
//	    return &Repo{HealthProvider: hc.Provider(), hc: hc}
//	}
//
// # The "/" reservation
//
// "/" is forbidden in ServiceName:
// it is the join separator of the Flatten helper,
// and it keeps path-style addressing
// (Check(ctx, "auth/repo"))
// available as a backward-compatible future extension.
//
// # Startup convention: Unknown while determining
//
// NotServing means "determined unable to serve",
// not "not determined yet":
// an owner performing its initial determination
// (connecting with retries, waiting for a sibling, migrating)
// keeps Unknown until the first definite outcome.
// This convention is what makes WaitSettled a reliable fail-fast primitive:
// the first non-Unknown aggregate status is meaningful,
// never a startup transient.
//
// # Shutdown: writer-local latch
//
// Writers have a terminal Close():
// it sets NotServing (every name, for States) and latches —
// subsequent SetServingStatus calls are silently ignored.
// It exists for shutdown ordering:
// without the latch an owner entering shutdown races its own monitoring goroutine,
// which may flip the status back to Serving after shutdown began.
//
// # Readiness vs liveness
//
// This package models readiness (grpc.health.v1 "serving") only.
// Liveness is a separate trivial endpoint
// and is out of scope.
//
// # Check/List/Watch ctx
//
// Check, List, and Watch accept ctx for interface uniformity
// and future remote-backed implementations;
// the in-memory core ignores it (instant reads).
//
// # Typical consumers
//
//   - Probes: a per-app /ready endpoint or the gRPC health protocol,
//     fed by the app's aggregate.
//   - Metrics: rate() over the exported Transitions counter
//     is literally a flapping graph.
//   - Tests: WaitSettled on an aggregate replaces port polling
//     and ad-hoc readiness hacks.
//   - In-process consumers: startup ordering
//     (wait for a sibling's Serving before warming a cache),
//     graceful degradation (react to a subsystem going NotServing).
//
// # Deliberate non-goals
//
//   - No critical/non-critical member weights.
//     An aggregate is an honest AND over its members.
//     If a member must not affect a probe,
//     wire a narrower aggregate for that probe
//     instead of teaching the library to ignore members.
//   - No error/details payload.
//     A status answers what is not serving; the owner logs the why.
//
// Positioning non-goals (no dependency checking, no DEGRADED status)
// are covered in the README.
package health
