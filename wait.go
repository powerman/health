package health

import (
	"context"
	"errors"
)

// ErrWatchClosed is returned by WaitSettled
// when the events channel is closed before the status settles
// (i.e. the Watch ctx was done first).
var ErrWatchClosed = errors.New("health: watch closed")

// WaitSettled discards Unknown statuses received from ch
// and returns the first status that differs from Unknown.
// It returns ctx.Err() if ctx is done first,
// or ErrWatchClosed if ch is closed first.
// ServiceUnknown settles too:
// with static wiring it is a bug,
// and callers must fail fast instead of hanging until a timeout.
func WaitSettled(ctx context.Context, ch <-chan Status) (ServingStatus, error) {
	for {
		select {
		case <-ctx.Done():
			return Unknown, ctx.Err()
		case st, ok := <-ch:
			if !ok {
				err := ctx.Err()
				if err != nil {
					return Unknown, err
				}
				return Unknown, ErrWatchClosed
			}
			if st.ServingStatus != Unknown {
				return st.ServingStatus, nil
			}
		}
	}
}
