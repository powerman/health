package health

import (
	"context"
)

// AlwaysServing implements HealthProvider with a permanent Serving status.
// Embed the bare type where readiness is unconditional
// (the grpc UnimplementedXxxServer analogue).
// It is read-only by construction,
// so exposing the concrete type is harmless.
type AlwaysServing struct{}

// Health returns a Health that always reports Serving for Overall
// and ServiceUnknown for other names.
func (AlwaysServing) Health() Health {
	return alwaysServingHealth{}
}

// alwaysServingHealth is the read-only view for AlwaysServing.
type alwaysServingHealth struct{}

// onTransition implements transitionNotifier:
// the status never changes,
// so f is seeded with Serving and never fired again.
func (alwaysServingHealth) onTransition(f func(ServingStatus)) {
	f(Serving)
}

func (alwaysServingHealth) Check(_ context.Context, service ServiceName) ServingStatus {
	if service == Overall {
		return Serving
	}
	return ServiceUnknown
}

func (alwaysServingHealth) List(_ context.Context) map[ServiceName]Status {
	return map[ServiceName]Status{
		Overall: {ServingStatus: Serving},
	}
}

func (alwaysServingHealth) Watch(ctx context.Context, service ServiceName) <-chan Status {
	ch := make(chan Status, 1)
	if service == Overall {
		ch <- Status{ServingStatus: Serving}
	} else {
		ch <- Status{ServingStatus: ServiceUnknown}
	}
	context.AfterFunc(ctx, func() { close(ch) })
	return ch
}
