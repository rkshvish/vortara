package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLRU_BasicSetGet(t *testing.T) {
	c := New[string, int](2)
	c.Set("a", 1, 0)

	got, ok := c.Get("a")
	if !ok || got != 1 {
		t.Fatalf("Get() = (%v, %v), want (1, true)", got, ok)
	}
}

func TestLRU_Eviction(t *testing.T) {
	c := New[string, int](3)
	c.Set("a", 1, 0)
	c.Set("b", 2, 0)
	c.Set("c", 3, 0)
	c.Set("d", 4, 0)

	if _, ok := c.Get("a"); ok {
		t.Fatal("expected oldest entry to be evicted")
	}
	if got, ok := c.Get("d"); !ok || got != 4 {
		t.Fatalf("expected newest entry to remain, got (%v, %v)", got, ok)
	}
}

func TestLRU_TTLExpiry(t *testing.T) {
	c := New[string, int](2)
	c.Set("a", 1, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	if _, ok := c.Get("a"); ok {
		t.Fatal("expected expired entry to miss")
	}
}

func TestLRU_MoveOnAccess(t *testing.T) {
	c := New[string, int](2)
	c.Set("a", 1, 0)
	c.Set("b", 2, 0)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected key a to be present")
	}
	c.Set("c", 3, 0)

	if _, ok := c.Get("b"); ok {
		t.Fatal("expected b to be evicted after access moved a to front")
	}
	if got, ok := c.Get("a"); !ok || got != 1 {
		t.Fatalf("expected a to remain, got (%v, %v)", got, ok)
	}
}

func TestLRU_Delete(t *testing.T) {
	c := New[string, int](2)
	c.Set("a", 1, 0)
	c.Delete("a")

	if _, ok := c.Get("a"); ok {
		t.Fatal("expected deleted key to miss")
	}
}

func TestLRU_Concurrent(t *testing.T) {
	c := New[string, int](128)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", i)
			c.Set(key, i, 0)
			if got, ok := c.Get(key); !ok || got != i {
				t.Errorf("Get(%q) = (%v, %v), want (%d, true)", key, got, ok, i)
			}
		}(i)
	}
	wg.Wait()
}
