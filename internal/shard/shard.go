// Package shard splits a fleet of RDS instances across several rdstail
// processes without overlap: process i of n owns the instances whose
// rendezvous (highest-random-weight) hash picks i. Every process computes the
// same assignment independently — no coordination — and changing n moves only
// the instances whose winner changes (about 1/n of them).
package shard

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// Shard is this process's slice of the fleet: Index in 1..Count.
type Shard struct {
	Index, Count int
}

// All is the zero-config shard that owns everything.
var All = Shard{Index: 1, Count: 1}

// Parse reads "i/n" (1 <= i <= n <= 1024). "" means All.
func Parse(s string) (Shard, error) {
	if strings.TrimSpace(s) == "" {
		return All, nil
	}
	a, b, ok := strings.Cut(strings.TrimSpace(s), "/")
	i, err1 := strconv.Atoi(a)
	n, err2 := strconv.Atoi(b)
	if !ok || err1 != nil || err2 != nil {
		return Shard{}, fmt.Errorf("shard %q: want i/n, e.g. 2/4", s)
	}
	if n < 1 || n > 1024 || i < 1 || i > n {
		return Shard{}, errors.New("shard: need 1 <= i <= n <= 1024")
	}
	return Shard{Index: i, Count: n}, nil
}

// Owns reports whether this shard owns key (e.g. "us-east-1|db-1").
func (s Shard) Owns(key string) bool {
	if s.Count <= 1 {
		return true
	}
	best, owner := uint64(0), 0
	for j := 1; j <= s.Count; j++ {
		h := fnv.New64a()
		_, _ = h.Write([]byte(key))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.Itoa(j)))
		if v := mix(h.Sum64()); j == 1 || v > best {
			best, owner = v, j
		}
	}
	return owner == s.Index
}

// mix is a 64-bit finalizer (splitmix64) to spread FNV's low-entropy bits.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ x>>31
}

func (s Shard) String() string { return fmt.Sprintf("%d/%d", s.Index, s.Count) }
