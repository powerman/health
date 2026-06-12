package health

// noCopy signals to vet that a struct must not be copied after first use.
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
