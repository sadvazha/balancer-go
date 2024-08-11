package service

import (
	"context"
	"math/rand"
	"time"
)

// TheExpensiveFragileService is a service that we need to utilise as much as we can, since it's expensive to run,
// but on the other hand is very fragile, so we can't just flood it with thousands of requests per second.
// It implements balancer.Server interface.
type TheExpensiveFragileService struct{}

// Process a single work chunk and return error if occurred.
func (TheExpensiveFragileService) Process(_ context.Context, _ int) error {
	// do not implement me, just imagine there's huge, complex, almost extra-terrestrial logic here which takes
	// arbitrary number of time to complete.
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return nil
}
