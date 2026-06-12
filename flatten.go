package health

import (
	"context"
)

// Flatten walks h recursively via Status.Sub and returns a single flat map:
// nested member names are joined with "/" (e.g. "auth/repo"),
// aggregates of inner nodes appear under their node's name (e.g. "auth"),
// h's own aggregate under Overall.
// Sub is cleared in the result.
func Flatten(ctx context.Context, h Health) map[ServiceName]Status {
	result := make(map[ServiceName]Status)
	flatten(ctx, h, "", result)
	return result
}

func flatten(ctx context.Context, h Health, prefix string, result map[ServiceName]Status) {
	statuses := h.List(ctx)
	for name, st := range statuses {
		fullName := prefix + string(name)
		if st.Sub != nil {
			childPrefix := fullName + "/"
			flatten(ctx, st.Sub, childPrefix, result)
		}
		// Skip child's Overall — it is already represented
		// by the parent member entry (e.g. "db" covers "db/").
		if name == Overall && prefix != "" {
			continue
		}
		st.Sub = nil
		result[ServiceName(fullName)] = st
	}
}
