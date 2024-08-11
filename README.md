# Task description

Imagine a standard client-server relationship, only in our case, the server is very fragile (and very expensive).
We can't let an arbitrary number of clients smashing the server with arbitrary number of requests, but as it's so expensive
to run, we'd like to utilize it as much as possible. And that's why we need a balancer.

A balancer is a gateway between the clients and the server. A client registers themselves in balancer and the balancer
is responsible for distributing the service capacity between registered clients.

Based on your skill, you can choose to implement a very simple balancer or production-ready balancer.

The implementation of a simple balancer can, for example, be one of:

* A balancer that serves one client at a time, enqueuing others.
* A balancer that serves registered clients in a round-robin fashion.
* A balancer that is processing batches of registered clients and distributing the capacity among them, while enqueuing
  incoming clients and once one batch is done, process the next batch.

The ultimate, production balancer would be one that immediately starts processing requests from an incoming client,
in regard to its weight (priority). Once client's requests has finished processing, the balancer re-allocates
the computation capacity between other clients currently in process.

In any way, **the balancer must ensure that the number of requests in process at any given time equals the provided
limit of the server**, aka we never can over-utilise the server, but we also don't want to under-utilise it.