package store

import (
	"strconv"
	"testing"
)

// BEGIN AI SECTION
func TestSkiplist_BasicGetSet(t *testing.T) {
	sl := NewSkiplist()

	// Test Set and Get for a single item
	sl.Set("key1", "value1")
	val, ok := sl.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("Expected to get 'value1', got '%s' (ok: %v)", val, ok)
	}

	// Test Get for a non-existent item
	val, ok = sl.Get("key2")
	if ok || val != "" {
		t.Errorf("Expected to not find 'key2', got '%s' (ok: %v)", val, ok)
	}

	// Test Overwrite
	sl.Set("key1", "value1_updated")
	val, ok = sl.Get("key1")
	if !ok || val != "value1_updated" {
		t.Errorf("Expected to get 'value1_updated', got '%s' (ok: %v)", val, ok)
	}
}

func TestSkiplist_Ordering(t *testing.T) {
	sl := NewSkiplist()

	// Insert keys out of alphabetical order
	sl.Set("c", "3")
	sl.Set("a", "1")
	sl.Set("d", "4")
	sl.Set("b", "2")

	// We must verify they are actually sorted by traversing Level 0
	expectedKeys := []string{"a", "b", "c", "d"}
	expectedVals := []string{"1", "2", "3", "4"}

	current := sl.BeginNode.Next[0]
	count := 0

	for current != nil {
		if count >= len(expectedKeys) {
			t.Fatalf("Found more nodes than expected. Extra key: %s", current.Key)
		}

		if current.Key != expectedKeys[count] {
			t.Errorf("Expected key %s at position %d, got %s", expectedKeys[count], count, current.Key)
		}
		if current.Value != expectedVals[count] {
			t.Errorf("Expected value %s at position %d, got %s", expectedVals[count], count, current.Value)
		}

		current = current.Next[0]
		count++
	}

	if count != len(expectedKeys) {
		t.Errorf("Expected traversal of %d nodes, but only found %d", len(expectedKeys), count)
	}
}

// Note: Skiplist is documented as not thread-safe; the Store wraps every
// skiplist access in its own sync.RWMutex. Concurrent-access testing
// therefore lives at the Store boundary in TestStore_ConcurrentSetGet, not
// here.

func TestSkiplist_TombstoneStoredAsRegularValue(t *testing.T) {
	// Skiplist is a dumb sorted-map: it stores tombstones like any other
	// string value and does not interpret them. Tombstone semantics (masking
	// older copies of a key across LSM levels) live in the Store layer.
	sl := NewSkiplist()

	sl.Set("key", "val1")
	val, ok := sl.Get("key")
	if !ok || val != "val1" {
		t.Fatalf("Expected key to exist with val1, got %q (ok: %v)", val, ok)
	}

	// Overwrite with the tombstone sentinel — the skiplist must return it
	// verbatim, NOT hide it.
	sl.Set("key", tombstone)
	val, ok = sl.Get("key")
	if !ok || val != tombstone {
		t.Errorf("Expected Get to return the tombstone sentinel verbatim, got %q (ok: %v)", val, ok)
	}

	// Overwriting with a real value resurrects the key
	sl.Set("key", "val2")
	val, ok = sl.Get("key")
	if !ok || val != "val2" {
		t.Errorf("Expected resurrected key to return val2, got %q (ok: %v)", val, ok)
	}
}

func BenchmarkSkiplist_Set(b *testing.B) {
	sl := NewSkiplist()

	for i := 0; b.Loop(); i++ {
		sl.Set(strconv.Itoa(i), "value")
	}
}

func BenchmarkSkiplist_Get(b *testing.B) {
	sl := NewSkiplist()

	// Pre-generate the exact 10,000 keys we will use, this will prevent noises
	// like garbage collection and strconv for our benchmark
	const numItems = 10000
	keys := make([]string, numItems)

	for i := range numItems {
		keys[i] = strconv.Itoa(i)
		sl.Set(keys[i], "value")
	}

	// Loop resets the benchmark timer the first time it is called in a benchmark
	for i := 0; b.Loop(); i++ {
		sl.Get(keys[i%10000])
	}
}

// END AI SECTION
