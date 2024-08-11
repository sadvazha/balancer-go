package client

import (
	"context"
)

// Client implements balancer.Client interface.
type Client struct {
	workload int
	weight   int
}

// New client.
func New(workload, weight int) *Client {
	return &Client{
		workload: workload,
		weight:   weight,
	}
}

// Weight of this client, i.e. how "important" they are. This might affect how much computation capacity
// the client should be allocated by Balancer.
func (c *Client) Weight() int {
	return c.weight
}

// Workload feeds work chunks through returned channel until there is no more work to be fed or ctx was Done.
func (c *Client) Workload(ctx context.Context) chan int {
	workload := make(chan int)
	go func() {
		defer close(workload)

		load := c.workload
		for {
			select {
			case <-ctx.Done():
				return
			case workload <- load:
				if load -= 1; load == 0 {
					return
				}
			}
		}
	}()
	return workload
}
