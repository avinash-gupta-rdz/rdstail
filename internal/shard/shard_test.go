package shard

import (
	"fmt"
	"testing"
)

func TestOwns_PartitionsWithoutOverlapAndEvenly(t *testing.T) {
	const n, keys = 4, 4000
	counts := make([]int, n)
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("us-east-1|db-%d", k)
		owners := 0
		for i := 1; i <= n; i++ {
			if (Shard{i, n}).Owns(key) {
				owners++
				counts[i-1]++
			}
		}
		if owners != 1 {
			t.Fatalf("%s owned by %d shards", key, owners)
		}
	}
	for i, c := range counts {
		if c < keys/n*8/10 || c > keys/n*12/10 {
			t.Fatalf("shard %d owns %d of %d: uneven %v", i+1, c, keys, counts)
		}
	}
}

func TestOwns_ResizeMovesFewInstances(t *testing.T) {
	moved, keys := 0, 4000
	owner := func(key string, n int) int {
		for i := 1; i <= n; i++ {
			if (Shard{i, n}).Owns(key) {
				return i
			}
		}
		return 0
	}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("db-%d", k)
		if owner(key, 4) != owner(key, 5) {
			moved++
		}
	}
	// Growing 4 → 5 should move ~1/5 of the fleet, not reshuffle it.
	if moved > keys*3/10 {
		t.Fatalf("resize 4→5 moved %d of %d instances", moved, keys)
	}
}

func TestParse(t *testing.T) {
	for _, bad := range []string{"0/4", "5/4", "2", "a/b", "1/0", "1/2000"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("Parse(%q) accepted", bad)
		}
	}
	if s, err := Parse("2/4"); err != nil || s != (Shard{2, 4}) {
		t.Fatalf("Parse(2/4) = %v, %v", s, err)
	}
	if s, _ := Parse(""); s != All || !s.Owns("x") {
		t.Fatal("empty shard must own everything")
	}
}
