package kakima

import (
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"
)

var (
	DefaultServerPrefix string = "SERVER"
	MaxCollisionCount   int    = 100

	ErrInvalidConfig  error = errors.New("ring configuration error")
	ErrAlreadyPresent error = errors.New("resource already present")
)

type ServerName string

func (s ServerName) String() string {
	return string(s)
}

type DeploymentName string

type RingConfig struct {
	// Starts from 1 by default as a minimum of 1 node will be there in the ring
	VirtualReplicaCount int
	// Denotes the number of servers in the deployment.
	//
	// Defaults to 0 as the value can be mutated later
	ServerCount int
	// Denotes a name for the deployment which will be its
	// identifier in the HashingRing
	//
	// Recommended to give a human-readable name
	DeploymentName DeploymentName
}

type ServerHash uint64
type HashRing struct {
	sync.RWMutex
	config             RingConfig
	serverHashSpaceMap map[ServerName][]ServerHash
	hashSpaceServerMap map[ServerHash]ServerName
	sortedServerHashes []ServerHash
}

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
		sortedServerHashes: make([]ServerHash, 0, conf.ServerCount*conf.VirtualReplicaCount),
	}, nil
}

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
	return nil
}

// hashFNV1a uses the fnv-1a algorithm to
// deterministically hash a string
func hashFNV1a(s string) uint64 {
	hasher := fnv.New64a()
	hasher.Write([]byte(s))
	return hasher.Sum64()
}

// Returns excel stryle notation from integer, i.e.
// 26 will be denoted by "Z" and 31 will be denoted
// by "AE"
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

func main() {
	fmt.Println(getServerString("redis-cache", 23, 31))
}
