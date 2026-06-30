// Copyright 2026 Ishanu Chakraborty. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package kakima

import (
	"fmt"
	"testing"
)

func TestHashRing_AddAndRemove(t *testing.T) {
	ring, err := NewHashRing(RingConfig{VirtualReplicaCount: 3, DeploymentName: "test-cluster"})
	if err != nil {
		t.Fatalf("NewHashRing failed: %v", err)
	}

	if err := ring.AddServer("server-1"); err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}

	if err := ring.AddServer("server-1"); err == nil {
		t.Fatal("expected error adding duplicate server")
	}

	if err := ring.RemoveServer("server-1"); err != nil {
		t.Fatalf("RemoveServer failed: %v", err)
	}

	if err := ring.RemoveServer("server-1"); err == nil {
		t.Fatal("expected error removing non-existent server")
	}
}

func TestHashRing_GetServer(t *testing.T) {
	ring, _ := NewHashRing(RingConfig{VirtualReplicaCount: 100, DeploymentName: "test-cluster"})

	if _, err := ring.GetServer("key1"); err == nil {
		t.Fatal("expected error getting server from empty ring")
	}

	_ = ring.AddServer("redis-1")
	_ = ring.AddServer("redis-2")

	s1, _ := ring.GetServer("user:100")
	s2, _ := ring.GetServer("user:100")
	if s1 != s2 {
		t.Fatalf("non-deterministic routing: got %s and %s", s1, s2)
	}
}

func TestHashRing_ScaleVReplicas(t *testing.T) {
	ring, _ := NewHashRing(RingConfig{VirtualReplicaCount: 10, DeploymentName: "test-cluster"})
	_ = ring.AddServer("redis-1")

	if ring.config.VirtualReplicaCount != 10 {
		t.Fatalf("expected 10 replicas, got %d", ring.config.VirtualReplicaCount)
	}

	if err := ring.ScaleVReplicas(20); err != nil {
		t.Fatalf("scale up failed: %v", err)
	}
	if ring.config.VirtualReplicaCount != 20 {
		t.Fatalf("expected 20 replicas, got %d", ring.config.VirtualReplicaCount)
	}

	if err := ring.ScaleVReplicas(5); err != nil {
		t.Fatalf("scale down failed: %v", err)
	}
	if ring.config.VirtualReplicaCount != 5 {
		t.Fatalf("expected 5 replicas, got %d", ring.config.VirtualReplicaCount)
	}
}

func ExampleHashRing_GetServer() {
	ring, _ := NewHashRing(RingConfig{
		VirtualReplicaCount: 100,
		DeploymentName:      "cache-tier",
	})

	_ = ring.AddServer("redis-node-a")

	server, _ := ring.GetServer("user:123")
	fmt.Println(server)
	// Output: redis-node-a
}

func BenchmarkGetServer_1000Servers(b *testing.B) {
	ring, _ := NewHashRing(RingConfig{VirtualReplicaCount: 100, DeploymentName: "bench-cluster"})
	for i := 0; i < 1000; i++ {
		_ = ring.AddServer(ServerName(fmt.Sprintf("redis-%d", i)))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = ring.GetServer("user:12345")
	}
}
