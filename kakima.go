// Copyright 2026 Ishanu Chakraborty. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

// Package kakima provides a thread-safe, deterministic consistent hash ring.
// It is designed to act as a scalable routing layer for distributed systems,
// ensuring minimal key migration when nodes are added or removed.
package kakima

import (
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"
)

var (
	// DefaultServerPrefix is the prefix used for virtual node names if no server name is provided.
	DefaultServerPrefix string = "SERVER"
	// MaxCollisionCount limits the number of linear probes when handling hash collisions.
	MaxCollisionCount int = 100

	// ErrInvalidConfig is returned when the provided RingConfig is invalid.
	ErrInvalidConfig error = errors.New("ring configuration error")
	// ErrAlreadyPresent is returned when attempting to add a server that already exists on the ring.
	ErrAlreadyPresent error = errors.New("resource already present")
	// ErrNotPresent is returned when attempting to access or remove a server that does not exist.
	ErrNotPresent error = errors.New("resource not present")
)

// ServerName represents the unique identifier of a physical server or node.
type ServerName string

// String returns the string representation of the ServerName.
func (s ServerName) String() string {
	return string(s)
}

// DeploymentName represents the identifier for this specific hash ring deployment.
type DeploymentName string

// RingConfig defines the initial parameters for instantiating a new HashRing.
type RingConfig struct {
	// VirtualReplicaCount sets the number of virtual nodes per physical server.
	// Starts from 1 by default as a minimum of 1 node will be there in the ring.
	VirtualReplicaCount int
	// ServerCount denotes the initial number of servers expected in the deployment.
	// Defaults to 0 as the value can be mutated later.
	ServerCount int
	// DeploymentName denotes a name for the deployment which will be its
	// identifier in the HashRing. Recommended to give a human-readable name.
	DeploymentName DeploymentName
}

// ServerHash represents the deterministic 64-bit hash of a virtual node on the ring.
type ServerHash uint64

// HashRing is a thread-safe consistent hash ring that routes keys to servers.
type HashRing struct {
	sync.RWMutex
	config             RingConfig
	serverHashSpaceMap map[ServerName][]ServerHash
	hashSpaceServerMap map[ServerHash]ServerName
	sortedServerHashes []ServerHash
}

// NewHashRing initializes and returns a new consistent HashRing based on the provided config.
func NewHashRing(conf RingConfig) (*HashRing, error) {
	if conf.VirtualReplicaCount == 0 {
		conf.VirtualReplicaCount = 1
	}
	if conf.DeploymentName == "" {
		return nil, fmt.Errorf("%w, cannot have empty \"DeploymentName\"", ErrInvalidConfig)
	}
	return &HashRing{
		config:             conf,
		serverHashSpaceMap: map[ServerName][]ServerHash{},
		hashSpaceServerMap: map[ServerHash]ServerName{},
		sortedServerHashes: make(
			[]ServerHash, 0, conf.ServerCount*conf.VirtualReplicaCount,
		),
	}, nil
}

// AddServer adds a new server and its virtual replicas to the hash ring.
// Returns ErrAlreadyPresent if the server is already on the ring.
func (hr *HashRing) AddServer(sn ServerName) error {
	hr.Lock()
	defer hr.Unlock()
	if _, exists := hr.serverHashSpaceMap[sn]; exists {
		return fmt.Errorf("%w: server %s exists in hashRing", ErrAlreadyPresent, sn)
	}
	newHashRingLen := len(hr.sortedServerHashes) + hr.config.VirtualReplicaCount
	newHashes := make([]ServerHash, 0, hr.config.VirtualReplicaCount)
	hr.serverHashSpaceMap[sn] = []ServerHash{}
	for i := 0; i < hr.config.VirtualReplicaCount; i++ {
		var hash ServerHash
		var j int
		for j = 0; j <= MaxCollisionCount+1; j++ {
			hash = ServerHash(hashFNV1a(getServerString(sn, i, j)))
			if _, exists := hr.hashSpaceServerMap[hash]; !exists {
				break
			}
		}
		if j == MaxCollisionCount+1 {
			panic("collision bounds exceeded")
		}
		newHashes = append(newHashes, hash)
		hr.serverHashSpaceMap[sn] = append(hr.serverHashSpaceMap[sn], hash)
		hr.hashSpaceServerMap[hash] = sn
	}
	newSortedServerHashes := make([]ServerHash, 0, newHashRingLen)
	newSortedServerHashes = append(newSortedServerHashes, hr.sortedServerHashes...)
	newSortedServerHashes = append(newSortedServerHashes, newHashes...)
	slices.Sort(newSortedServerHashes)
	hr.sortedServerHashes = newSortedServerHashes
	hr.config.ServerCount++
	return nil
}

// RemoveServer safely removes a server and all its associated virtual replicas from the ring.
func (hr *HashRing) RemoveServer(sn ServerName) error {
	hr.Lock()
	defer hr.Unlock()
	if _, ok := hr.serverHashSpaceMap[sn]; !ok {
		return fmt.Errorf(
			"%w: server with name \"%s\" is not present",
			ErrNotPresent, sn,
		)
	}
	for _, v := range hr.serverHashSpaceMap[sn] {
		delete(hr.hashSpaceServerMap, v)
	}
	hr.sortedServerHashes = slices.DeleteFunc(
		hr.sortedServerHashes,
		func(s ServerHash) bool {
			_, ok := hr.hashSpaceServerMap[s]
			return !ok
		})
	delete(hr.serverHashSpaceMap, sn)
	hr.config.ServerCount--
	return nil
}

// GetServer routes a given key (as a string) to the closest server on the hash ring.
func (hr *HashRing) GetServer(s string) (ServerName, error) {
	hr.RLock()
	defer hr.RUnlock()
	if len(hr.sortedServerHashes) == 0 {
		return "", fmt.Errorf("%w hashRing for %s is empty", ErrNotPresent, hr.config.DeploymentName)
	}
	hash := ServerHash(hashFNV1a(s))
	idx, _ := slices.BinarySearch(hr.sortedServerHashes, hash)
	if idx == len(hr.sortedServerHashes) {
		return hr.hashSpaceServerMap[hr.sortedServerHashes[0]], nil
	}
	return hr.hashSpaceServerMap[hr.sortedServerHashes[idx]], nil
}

func (hr *HashRing) addReplica() error {
	for sn := range hr.serverHashSpaceMap {
		var hash ServerHash
		var j int
		for j = 0; j <= MaxCollisionCount+1; j++ {
			hash = ServerHash(hashFNV1a(getServerString(sn, len(hr.serverHashSpaceMap[sn]), j)))
			if _, exists := hr.hashSpaceServerMap[hash]; !exists {
				break
			}
		}
		if j == MaxCollisionCount+1 {
			panic("collision bounds exceeded")
		}
		hr.serverHashSpaceMap[sn] = append(hr.serverHashSpaceMap[sn], hash)
		hr.hashSpaceServerMap[hash] = sn
		idx, _ := slices.BinarySearch(hr.sortedServerHashes, hash)
		hr.sortedServerHashes = slices.Insert(hr.sortedServerHashes, idx, hash)
	}
	hr.config.VirtualReplicaCount++
	return nil
}

func (hr *HashRing) removeReplica() error {
	if hr.config.VirtualReplicaCount == 1 {
		return fmt.Errorf("%w: replicacount cannot go lower than 1", ErrInvalidConfig)
	}
	for sn := range hr.serverHashSpaceMap {
		hash := hr.serverHashSpaceMap[sn][len(hr.serverHashSpaceMap[sn])-1]
		delete(hr.hashSpaceServerMap, hr.serverHashSpaceMap[sn][len(hr.serverHashSpaceMap[sn])-1])
		hr.serverHashSpaceMap[sn] = hr.serverHashSpaceMap[sn][:len(hr.serverHashSpaceMap[sn])-1]
		idx, _ := slices.BinarySearch(hr.sortedServerHashes, hash)
		hr.sortedServerHashes = slices.Delete(hr.sortedServerHashes, idx, idx+1)
	}
	hr.config.VirtualReplicaCount--
	return nil
}

// ScaleVReplicas dynamically scales the number of virtual replicas per server.
// It acquires a write lock and safely iterates through add/remove operations.
func (hr *HashRing) ScaleVReplicas(targetCount int) error {
	hr.Lock()
	defer hr.Unlock()

	if targetCount < 1 {
		return fmt.Errorf("%w: target count must be at least 1", ErrInvalidConfig)
	}

	for hr.config.VirtualReplicaCount < targetCount {
		if err := hr.addReplica(); err != nil {
			return err
		}
	}

	for hr.config.VirtualReplicaCount > targetCount {
		if err := hr.removeReplica(); err != nil {
			return err
		}
	}

	return nil
}

// hashFNV1a uses the fnv-1a algorithm to deterministically hash a string.
func hashFNV1a(s string) uint64 {
	hasher := fnv.New64a()
	hasher.Write([]byte(s))
	return hasher.Sum64()
}

// excelStyleAlphaFromInt returns excel style notation from an integer.
func excelStyleAlphaFromInt(i int) string {
	var s string
	for i > 0 {
		i--
		rem := i % 26
		s = fmt.Sprintf("%c%s", rune('A'+rem), s)
		i /= 26
	}
	return s
}

func getServerString(serverName ServerName, virtualCount int, collisionCount int) string {
	var s ServerName = ServerName(DefaultServerPrefix)
	if serverName != "" {
		s = serverName
	}
	s = ServerName(fmt.Sprintf("%s#%03d", s, virtualCount))
	if collisionCount == 0 {
		return string(s)
	}
	return fmt.Sprintf(
		"%s-%s",
		s, excelStyleAlphaFromInt(collisionCount),
	)
}
