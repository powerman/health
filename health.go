package health

import (
	"context"
	"fmt"
	"time"
)

// ServingStatus mirrors grpc.health.v1.HealthCheckResponse.ServingStatus 1:1.
type ServingStatus int32

const (
	Unknown        ServingStatus = 0 // Not reported yet / not determined.
	Serving        ServingStatus = 1 // Able to serve requests.
	NotServing     ServingStatus = 2 // Unable to serve requests.
	ServiceUnknown ServingStatus = 3 // Requested name is not known to this Health.
)

// String implements [fmt.Stringer].
func (s ServingStatus) String() string {
	switch s {
	case Unknown:
		return "Unknown"
	case Serving:
		return "Serving"
	case NotServing:
		return "NotServing"
	case ServiceUnknown:
		return "ServiceUnknown"
	default:
		return fmt.Sprintf("ServingStatus(%d)", int32(s))
	}
}

// ServiceName names one service inside a Health.
// Users are expected to declare constants of this type to avoid typos.
// "/" is reserved (Flatten join separator, future hierarchical addressing) and forbidden.
type ServiceName string

// Overall addresses the aggregate of a whole Health.
// grpc.health.v1 calls it "the overall health status",
// addressed by the empty name;
// it cannot be used as a member name.
const Overall ServiceName = ""

// Status is one service's status snapshot, used by both List and Watch.
type Status struct {
	ServingStatus ServingStatus

	// Transitions counts effective status changes.
	// In a Watch stream it increases monotonically;
	// a gap between consecutive events means intermediate transitions were conflated away.
	// Tracked by State/States and by NewAggregate's materialized Overall
	// (synchronous transition propagation).
	Transitions uint64

	// Since is the time of the last transition.
	// Zero for AlwaysServing and for synthetic (unknown name) responses.
	Since time.Time

	// Sub is non-nil only when this member is itself a composite Health
	// (currently: member entries in NewAggregate's List).
	// It enables recursive introspection without path parsing.
	Sub Health
}

// update updates the three transition fields and reports whether ServingStatus changed.
func (s *Status) update(st ServingStatus) bool {
	if s.ServingStatus == st {
		return false
	}
	s.ServingStatus = st
	s.Transitions++
	s.Since = time.Now()
	return true
}

// Health is the read-only view of a dynamically changing serving status
// of a named set of services plus their aggregate.
// All methods are safe for concurrent use.
type Health interface {
	// Check returns the current status of service
	// (Overall for the aggregate).
	// Unknown names yield ServiceUnknown.
	Check(ctx context.Context, service ServiceName) ServingStatus

	// List returns the aggregate (under Overall)
	// and every member with its Status.
	List(ctx context.Context) map[ServiceName]Status

	// Watch delivers the current status immediately,
	// then every change, conflated.
	// The returned channel is closed when ctx is done — and only then.
	//
	// Watching an unknown name delivers ServiceUnknown once
	// and the channel then stays open until ctx is done.
	Watch(ctx context.Context, service ServiceName) <-chan Status
}

// HealthProvider is the embeddable handle exposing an object's health.
// Default style: embed it and assign a concrete implementation
// in the constructor:
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
type HealthProvider interface{ Health() Health }

// transitionNotifier registers a callback on the Overall status.
// The callback fires immediately with the current status (seeding)
// and on every subsequent effective transition,
// synchronously under the writer's mutex.
// Registration is permanent.
type transitionNotifier interface {
	onTransition(f func(ServingStatus))
}
