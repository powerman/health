# health

[![License MIT](https://img.shields.io/badge/license-MIT-royalblue.svg)](LICENSE)
[![Go version](https://img.shields.io/github/go-mod/go-version/powerman/health?color=blue)](https://go.dev/)
[![Test](https://img.shields.io/github/actions/workflow/status/powerman/health/test.yml?label=test)](https://github.com/powerman/health/actions/workflows/test.yml)
[![Coverage Status](https://raw.githubusercontent.com/powerman/health/gh-badges/coverage.svg)](https://github.com/powerman/health/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/powerman/health)](https://goreportcard.com/report/github.com/powerman/health)
[![Release](https://img.shields.io/github/v/release/powerman/health?color=blue)](https://github.com/powerman/health/releases/latest)
[![Go Reference](https://pkg.go.dev/badge/github.com/powerman/health.svg)](https://pkg.go.dev/github.com/powerman/health)

![Linux | amd64 arm64 armv7 ppc64le s390x riscv64](https://img.shields.io/badge/Linux-amd64%20arm64%20armv7%20ppc64le%20s390x%20riscv64-royalblue)
![macOS | amd64 arm64](https://img.shields.io/badge/macOS-amd64%20arm64-royalblue)
![Windows | amd64 arm64](https://img.shields.io/badge/Windows-amd64%20arm64-royalblue)

Go package health implements a push-based dynamic readiness (serving status) model.

## Rationale

A readiness probe answers one question:
**should this process receive traffic right now?**
The Go ecosystem answers it almost exclusively with **pull-based checkers**
([alexliesenfeld/health](https://github.com/alexliesenfeld/health),
[hellofresh/health-go](https://github.com/hellofresh/health-go),
[AppsFlyer/go-sundheit](https://github.com/AppsFlyer/go-sundheit), …):
you register check functions that ping each dependency on probe or on a schedule,
and an HTTP handler serves the folded result.

That approach gets the direction of information flow backwards:

- **The owner already knows.**
  The code that runs migrations, binds listeners, and talks to the database
  learns about every readiness change first-hand, the moment it happens.
  A pull checker re-discovers the same fact later and indirectly,
  through a synthetic operation (a ping, a `SELECT 1`)
  that exercises neither the real query paths nor the real failure modes.
- **Pull checks duplicate monitoring.**
  A dependency's availability is that dependency's monitoring concern;
  when each of N client processes runs its own checker against some DB,
  the result is N redundant monitors —
  and a health endpoint drifting into a poor man's monitoring system,
  complete with error payloads that love to leak DSNs.
- **Readiness is never a final state.**
  A migration finishes; an external actor migrates the DB schema to an unsupported version;
  a dependency goes away and comes back.
  A one-shot `WaitReady` or a "Ready" log line is meaningless a second later.
  The model must be a dynamically changing status with change notification, not a snapshot.
- **Tests need the same answer as probes.**
  "Ready for the first API call" is exactly what an integration test waits for,
  yet with pull checkers tests fall back to port polling and sleeps.
  The test synchronization primitive and the production probe should be one mechanism.

So this library inverts the model: the core never runs checks.
A status is the last **observed** ability to serve,
**pushed** by its owner from wherever the owner learns about changes
(migration finished, listener bound, a schema-version callback fired).
The inversion is also the only direction that works:
an owner can always layer a pull check on top of a push API (poll, then set the status),
while the reverse is impossible.

gRPC defined exactly this model years ago — `grpc.health.v1`:
a dynamically changing status per named service plus an overall status,
with a streaming `Watch` —
but its Go implementations are transport pieces without a model
(grpc-go's [health.Server](https://pkg.go.dev/google.golang.org/grpc/health#Server)
is a flat map behind a gRPC service).
This package is that model as a transport-agnostic core,
extended with what production wiring actually needs:
type-enforced writer isolation, hierarchical composition,
transition counters for flapping detection,
and `WaitSettled` — one primitive for tests, startup ordering, and probes.

## Quick start

The owner side: a subsystem keeps the writer private
and embeds HealthProvider to expose the read-only view,
then reports transitions from wherever it learns about them.
Declare service names as constants —
the recommended `subsys` prefix keeps these identifiers
clear of the variables holding the subsystems themselves (`db`, `cache`, …),
while the string values stay short:
they are the wire names shown by probes and metrics.

```go
// Subsystems reported by Repo's health.
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

func (r *Repo) Serve(ctx context.Context) error {
    if err := r.connect(ctx); err != nil {
        r.hc.SetServingStatus(subsysDB, health.NotServing) // Overall: NotServing.
        return err
    }
    r.hc.SetServingStatus(subsysDB, health.Serving) // Overall: still Unknown.

    if err := r.migrate(ctx); err != nil {
        r.hc.SetServingStatus(subsysMigration, health.NotServing) // Overall: NotServing.
        return err
    }
    r.hc.SetServingStatus(subsysMigration, health.Serving) // Overall: Serving.

    // …
}
```

The reader side: probes, startup code, and tests see only the read-only view.

```go
func main() {
    ctx := context.Background()

    repo := NewRepo()
    go func() {
        if err := repo.Serve(ctx); err != nil {
            slog.Error("repo failed", "err", err)
        }
    }()

    // A readiness endpoint needs only Check.
    http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
        if repo.Health().Check(r.Context(), health.Overall) != health.Serving {
            w.WriteHeader(http.StatusServiceUnavailable)
        }
    })

    // Startup ordering and tests wait for the first definite status.
    ch := repo.Health().Watch(ctx, health.Overall)
    st, err := health.WaitSettled(ctx, ch)
    if err != nil || st != health.Serving {
        slog.Error("repo is not ready", "status", st, "err", err)
        return
    }
    slog.Info("ready")
    // …
}
```

## Positioning

This library is **not a dependency-checking library** (see Rationale above):
the core never runs checks, owners report what they observed.

With zero traffic a dead dependency may keep reporting Serving — accepted:
readiness gates traffic, and with no traffic there is nothing to gate;
the first real call updates the status through the owner's own detection.
Monitoring a dependency's availability is that dependency's monitoring,
not this process's readiness.

### Status aggregation

The Overall aggregate follows these rules:

- NotServing if any member is NotServing;
- Unknown if any member is Unknown (and none is NotServing);
- Serving otherwise.

### Comparison with alternatives

Most Go "health check" libraries solve a different problem: they are dependency checkers —
schedulers for ping functions with an HTTP handler on top.
A feature-by-feature comparison would be unfair in both directions:
they have no push model to compare,
and this library has no checker machinery — by design, see Rationale.
Pick one of them if you want scheduled dependency pings folded into an endpoint;
pick this library if you want owners to report the readiness they already know.

The meaningful comparison is with the existing implementations of the same push model:

- **grpc-go `health.Server`** — the reference implementation of `grpc.health.v1`:
  a flat map of per-service statuses behind a gRPC service.
  No aggregation, no writer isolation
  (any holder of the server can set any status), gRPC-only.
- **connectrpc.com/grpchealth `StaticChecker`** — the same protocol over net/http,
  the same flat model.

If all you need is the protocol on a single gRPC server,
grpc-go's implementation is fine.
This library is for what both leave out —
the model behind the transport:

- a transport-agnostic core mirroring `grpc.health.v1` 1:1,
  so gRPC, HTTP, metrics, and tests show the same data;
- hierarchical composition (subsystem → app → binary)
  with NewAggregate, Status.Sub, and Flatten;
- type-enforced writer isolation: only the owner can set its statuses;
- transition counters as data:
  flapping is visible in metrics even when nobody was watching;
- WaitSettled — one synchronization primitive for tests, startup ordering, and probes;
- static wiring (membership is fixed at construction, a typo panics)
  and no goroutines at rest.

### Deliberate non-goals

- **No error/details payload.**
  A status answers _what_ is not serving; _why_ is answered by the owner's logs.
  Error strings in a health endpoint would duplicate logs
  and add a secret-leak channel (DB error texts love to contain DSNs).
- **No DEGRADED status.**
  The vocabulary mirrors `grpc.health.v1`: Unknown / Serving / NotServing.
  "Degraded" is an external judgment, not a self-reported state:
  either an SLO observation (error rate, latency)
  made outside the process over an honestly-Serving service,
  or a structural one — some member is NotServing
  while the traffic-gating aggregate stays Serving —
  already visible via List and metrics without a dedicated status value.

## Use cases

### Probe guidance

Which Health feeds which probe is deployment policy, not a library decision.

- **gRPC services** — expose health through the gRPC health protocol
  (healthgrpc subpackage, planned),
  so clients and load balancers get per-service statuses natively.
- **HTTP services** — expose `/ready` backed by the service's aggregate:
  200/503 by `Check(ctx, health.Overall)`.
- **Liveness is out of scope** — keep `/live` a separate trivial endpoint;
  this library models readiness only.
- **Several services in one binary** (a modular monolith, an all-in-one dev build) —
  give each service its own aggregate and `/ready`,
  then combine them with NewAggregate into a whole-binary view
  for the single readiness probe a container gets.
  Gating all traffic on the whole-binary view couples the services;
  that coupling is inherent to co-deployment, not introduced by the library —
  the per-service endpoints remain the serious monitoring surface.

### Tests

Replace port-polling loops and ad-hoc readiness hacks
with a single WaitSettled call:

```go
func TestApp(t *testing.T) {
    app := startApp()
    ch := app.Health().Watch(t.Context(), health.Overall)
    st, err := health.WaitSettled(t.Context(), ch)
    if err != nil || st != health.Serving {
        t.Fatalf("app is not ready: %v, %v", st, err)
    }
    // Ready for the first API call: migrations finished AND ports listening.
}
```

`WaitSettled` relies on the owner contract "Unknown while determining":
`NotServing` means "determined unable to serve", not "not determined yet",
so an owner performing its initial determination
(connecting with retries, migrating)
keeps `Unknown` until the first definite outcome.
This makes the first non-`Unknown` status meaningful — never a startup transient —
and turns `WaitSettled` into a fail-fast primitive instead of a wait-for-timeout one.

### Startup ordering

Wait for a subsystem to become `Serving` before warming a cache:

```go
ch := dbHealth.Watch(ctx, "migration")
st, err := health.WaitSettled(ctx, ch)
if err != nil || st != health.Serving {
    return fmt.Errorf("db migration is not ready: %v, %v", st, err)
}
warmCache(ctx)
```

### Graceful degradation

React to a subsystem going NotServing by reducing functionality:

```go
ch := cacheHealth.Watch(ctx, health.Overall)
for st := range ch {
    if st.ServingStatus == health.NotServing {
        slog.Warn("cache unavailable, serving degraded")
    }
}
```

### Shutdown ordering

Use the terminal Close latch to prevent racing monitor goroutines from
reverting the status after shutdown starts:

```go
func (s *Server) Shutdown() {
    s.health.Close() // latches NotServing, monitor cannot revert
    s.listener.Close()
}
```

A runtime that wants "report `NotServing` while draining"
adds its own lifecycle State as one more member of its aggregate
and Closes it when shutdown starts —
the fold (see Status aggregation) expresses the override
without any privileged mutation path.

### Degradation recovery (poll-on-degraded, helper planned)

Failure detection is event-driven, but recovery detection is asymmetric:
once `NotServing` removed the traffic that generated the events,
nothing will report the dependency's return.

The pattern is **poll-on-degraded**: the owner starts a recovery-probe goroutine
on entering `NotServing` and stops it on success.
This keeps "no goroutines at rest" true (goroutines exist only while degraded).

A future subpackage will provide this helper.
Today the owner implements it directly:

```go
db.SetServingStatus(health.NotServing)
go func() {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()
    for range ticker.C {
        if pingDB() == nil {
            db.SetServingStatus(health.Serving)
            return
        }
    }
}()
```
