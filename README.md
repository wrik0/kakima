# kakima

[![Go Reference](https://pkg.go.dev/badge/github.com/wrik0/kakima.svg)](https://pkg.go.dev/github.com/wrik0/kakima)
[![Go CI](https://github.com/wrik0/kakima/actions/workflows/ci.yml/badge.svg)](https://github.com/wrik0/kakima/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A thread-safe, high-performance consistent hashing ring in Go. `kakima` uses virtual nodes (replicas) to ensure uniform load distribution and relies on $O(\log N)$ binary search for fast request routing with strictly zero heap allocations on the hot path.

## Mechanics

1. **The Ring Space**: A 64-bit integer space utilizing the deterministic `FNV-1a` hashing algorithm.
2. **Virtual Nodes**: Each physical node is hashed multiple times to ensure uniform distribution, preventing load hotspots. 
3. **Routing**: A key's hash is compared against the sorted virtual node hashes. The first node hash >= key hash is selected. If no node hash is found, it wraps around to the first node hash (index `0`).
4. **Complexity**:
    * **Lookup**: $O(\log N)$ where $N$ is the number of virtual nodes.
    * **Add/Remove Node**: $O(N)$ due to slice insertion/deletion shift.

## Performance

Tested on an AMD Ryzen 5 5500U. A hash ring loaded with 1,000 physical servers (100,000 virtual nodes total) yields sub-microsecond routing with zero garbage collection pressure.

```text
BenchmarkGetServer_1000Servers-12    13794426      86.98 ns/op       0 B/op       0 allocs/op
```

## Installation

```bash
go get github.com/wrik0/kakima
```

## Usage

```go
package main

import (
	"fmt"
	"github.com/wrik0/kakima"
)

func main() {
	// Initialize a ring with 100 virtual replicas per physical server
	ring, err := kakima.NewHashRing(kakima.RingConfig{
		VirtualReplicaCount: 100,
		DeploymentName:      "cache-tier",
	})
	if err != nil {
		panic(err)
	}

	// Add servers to the ring
	_ = ring.AddServer("redis-node-a")
	_ = ring.AddServer("redis-node-b")

	// Route a key to its assigned server
	server, _ := ring.GetServer("user:12345")
	fmt.Println("Routed to:", server) 
}
```

## License

MIT
