package health

// notifyWatchers sends st to every watcher using the conflated pattern:
// drain one pending event (to make room) then send.
// Must be called under the writer's mutex.
func notifyWatchers(watchers []chan Status, st Status) {
	for _, ch := range watchers {
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- st:
		default:
		}
	}
}

// fireCallbacks calls every registered callback for service.
// Must be called under the writer's mutex.
func fireCallbacks(callbacks []func(ServingStatus), st ServingStatus) {
	for _, f := range callbacks {
		f(st)
	}
}
