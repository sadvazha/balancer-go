package balancer

import (
	"context"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
)

type Client interface {
	// Weight is unit-less number that determines how much processing capacity can a client be allocated
	// when running in parallel with other clients. The higher the weight, the more capacity the client receives.
	Weight() int
	// Workload returns a channel of work chunks that are meant to be processed through the Server.
	// Client's channel is always filled with work chunks.
	Workload(ctx context.Context) chan int
}

// Server defines methods required to process client's work chunks (requests).
type Server interface {
	// Process takes one work chunk (request) and does something with it. The error can be ignored.
	Process(ctx context.Context, workChunk int) error
}

type ClientInfo struct {
	id                  uint32
	ctx                 context.Context
	client              Client
	workload            chan int
	currentlyProcessing int32
	capacity            int32 // Capacity can go over maxCapacity, but it's up to the Balancer to limit it
	exhausted           bool  // New field to track workload exhaustion
}
type ClientPool struct {
	clients     []*ClientInfo
	mu          sync.Mutex
	maxCapacity int32
}

// Add a client to the pool and recalculate capacities.
func (p *ClientPool) AddClient(ctx context.Context, client Client) {
	ci := &ClientInfo{
		id:                  rand.Uint32(), // used only for debugging purposes
		ctx:                 ctx,
		client:              client,
		workload:            client.Workload(ctx),
		currentlyProcessing: 0,
		capacity:            0,
		exhausted:           false,
	}
	log.Printf("Registering new client with weight %d, id: %d", client.Weight(), ci.id)
	p.mu.Lock()
	p.clients = append(p.clients, ci)
	p.recalculateCapacities()
	p.mu.Unlock()
}

// Private method to calculate available capacity based on weight.
func (p *ClientPool) calculateCapacity(client Client) int32 {
	totalWeight := p.totalWeight()
	if totalWeight == 0 {
		return p.maxCapacity
	}
	return int32((client.Weight() * int(p.maxCapacity)) / totalWeight)
}

func (p *ClientPool) totalWeight() int {
	total := 0
	for _, ci := range p.clients {
		// Sum of available capacity is based on weight only for clients that are not exhausted
		// Control over maxCapacity is done by the Balancer
		if ci.exhausted {
			continue
		}
		total += ci.client.Weight()
	}
	return total
}

func (p *ClientPool) recalculateCapacities() {
	totalWeight := p.totalWeight()
	for _, ci := range p.clients {
		if ci.exhausted {
			continue
		}
		ci.capacity = p.calculateCapacity(ci.client)
		log.Printf("Recalculated capacity: %+v, current: %d, totalWeight: %d, client id: %d", ci.capacity, ci.currentlyProcessing, totalWeight, ci.id)
	}
}

// Remove clients with no remaining workload and no ongoing processes.
func (p *ClientPool) RemoveInactiveClients() {
	p.mu.Lock()
	defer p.mu.Unlock()

	activeClients := []*ClientInfo{}
	for _, ci := range p.clients {
		if !(ci.exhausted && atomic.LoadInt32(&ci.currentlyProcessing) == 0) {
			activeClients = append(activeClients, ci)
			continue
		}
		log.Printf("Removed inactive clients")
	}
	p.clients = activeClients
}

// Mark client as exhausted if it has no more workload.
func (p *ClientPool) markClientExhausted(ci *ClientInfo) {
	log.Printf("Marking client as exhausted")
	ci.exhausted = true
	p.recalculateCapacities()
}

// Get the next client with available capacity.
func (p *ClientPool) GetNextWorkload() (ctx context.Context, chunk *int, done func()) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Shuffle clients to distribute workload randomly.
	rand.Shuffle(len(p.clients), func(i, j int) {
		p.clients[i], p.clients[j] = p.clients[j], p.clients[i]
	})

	// Check if there is a client with available capacity
	for _, ci := range p.clients {
		if ci.currentlyProcessing < ci.capacity && !ci.exhausted {
			ctx, chunk, done = p.getWorkload(ci)
			if ctx != nil {
				return ctx, chunk, done
			}
		}
	}

	// If there is no client with available capacity, check if there is a client with ongoing processes
	for _, ci := range p.clients {
		if !ci.exhausted {
			ctx, chunk, done = p.getWorkload(ci)
			if ctx != nil {
				return ctx, chunk, done
			}
		}
	}

	return nil, nil, nil
}

func (p *ClientPool) getWorkload(ci *ClientInfo) (context.Context, *int, func()) {
	result, ok := <-ci.workload
	if !ok {
		p.markClientExhausted(ci)
		return nil, nil, nil
	}

	done := func() {
		p.mu.Lock()
		ci.currentlyProcessing--
		p.mu.Unlock()
	}

	ci.currentlyProcessing++

	log.Printf("Sending workload %d, currentlyProcessing: %d, clientId: %d", result, ci.currentlyProcessing, ci.id)
	return ci.ctx, &result, done
}

// Balancer makes sure the Server is not smashed with incoming requests (work chunks) by only enabling certain number
// of parallel requests processed by the Server. Imagine there's a SLO defined, and we don't want to make the expensive
// service people angry.
//
// If implementing more advanced balancer, ake sure to correctly assign processing capacity to a client based on other
// clients currently in process.
// To give an example of this, imagine there's a maximum number of work chunks set to 100 and there are two clients
// registered, both with the same priority. When they are both served in parallel, each of them gets to send
// 50 chunks at the same time.
// In the same scenario, if there were two clients with priority 1 and one client with priority 2, the first
// two would be allowed to send 25 requests and the other one would send 50. It's likely that the one sending 50 would
// be served faster, finishing the work early, meaning that it would no longer be necessary that those first two
// clients only send 25 each but can and should use the remaining capacity and send 50 again.
type Balancer struct {
	server            Server
	maxCapacity       int32
	clientPool        ClientPool
	available         chan struct{}
	newUserRegistered *sync.Cond
}

// New creates a new Balancer instance. It needs the server that it's going to balance for and a maximum number of work
// chunks that can the processor process at a time. THIS IS A HARD REQUIREMENT - THE SERVICE CANNOT PROCESS MORE THAN
// <PROVIDED NUMBER> OF WORK CHUNKS IN PARALLEL.
func New(server Server, maxCapacity int32) *Balancer {
	log.Printf("Creating balancer with max capacity %d", maxCapacity)
	b := &Balancer{
		server:            server,
		maxCapacity:       maxCapacity,
		available:         make(chan struct{}, maxCapacity),
		newUserRegistered: sync.NewCond(&sync.Mutex{}),
		clientPool: ClientPool{
			clients:     []*ClientInfo{},
			mu:          sync.Mutex{},
			maxCapacity: maxCapacity,
		},
	}
	for i := int32(0); i < maxCapacity; i++ {
		b.available <- struct{}{} // Fill the channel to represent available capacity
	}
	// There should be a start method, but since there is none, i am starting the processing here
	go b.processClients()
	return b
}

// Register a client to the balancer and start processing its work chunks through provided processor (server).
// For the sake of simplicity, assume that the client has no identifier, meaning the same client can register themselves
// multiple times.
func (b *Balancer) Register(ctx context.Context, client Client) {
	b.clientPool.AddClient(ctx, client)
	b.newUserRegistered.Signal()
}

func (b *Balancer) processClients() {
	for {
		b.clientPool.RemoveInactiveClients()

		ctx, chunk, done := b.clientPool.GetNextWorkload()
		if chunk == nil {
			log.Printf("No workload available, waiting for new registration")
			// No clients available, wait for new registration
			b.newUserRegistered.L.Lock()
			b.newUserRegistered.Wait()
			b.newUserRegistered.L.Unlock()
			log.Printf("Client available")
			continue
		}

		<-b.available
		log.Printf("Processing chunk %d, available: %d", *chunk, len(b.available))
		go b.processChunk(ctx, *chunk, done)
	}
}

// Process a single chunk of work.
func (b *Balancer) processChunk(ctx context.Context, chunk int, done func()) {
	defer func() {
		done()
		b.available <- struct{}{} // Release capacity after processing
	}()

	b.server.Process(ctx, chunk)
}
