package registry

import (
	"strings"
	"testing"
)

type testConnector struct{ id string }

func TestRegisterBatchSource_Success(t *testing.T) {
	resetForTest()
	t.Cleanup(resetForTest)

	RegisterBatchSource("batch-a", func() any { return &testConnector{id: "a"} })

	got1, err := GetBatchSource("batch-a")
	if err != nil {
		t.Fatalf("GetBatchSource() error = %v", err)
	}
	got2, err := GetBatchSource("batch-a")
	if err != nil {
		t.Fatalf("GetBatchSource() error = %v", err)
	}

	if got1 == got2 {
		t.Fatal("expected factory to return new instances")
	}
}

func TestRegisterBatchSource_Duplicate(t *testing.T) {
	resetForTest()
	t.Cleanup(resetForTest)

	RegisterBatchSource("batch-b", func() any { return &testConnector{id: "b"} })
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	RegisterBatchSource("batch-b", func() any { return &testConnector{id: "b2"} })
}

func TestGetBatchSource_Unknown(t *testing.T) {
	resetForTest()
	t.Cleanup(resetForTest)

	RegisterBatchSource("batch-z", func() any { return &testConnector{id: "z"} })
	_, err := GetBatchSource("nonexistent")
	if err == nil || !strings.Contains(err.Error(), "nonexistent") || !strings.Contains(err.Error(), "batch-z") {
		t.Fatalf("expected unknown type error, got %v", err)
	}
}

func TestListBatchSources(t *testing.T) {
	resetForTest()
	t.Cleanup(resetForTest)

	RegisterBatchSource("batch-c", func() any { return &testConnector{id: "c"} })
	RegisterBatchSource("batch-d", func() any { return &testConnector{id: "d"} })

	got := ListBatchSources()
	if len(got) != 2 || got[0] != "batch-c" || got[1] != "batch-d" {
		t.Fatalf("ListBatchSources() = %v", got)
	}
}

func TestRegisterStreamingSource_Success(t *testing.T) {
	resetForTest()
	t.Cleanup(resetForTest)

	RegisterStreamingSource("stream-a", func() any { return &testConnector{id: "s1"} })

	got1, err := GetStreamingSource("stream-a")
	if err != nil {
		t.Fatalf("GetStreamingSource() error = %v", err)
	}
	got2, err := GetStreamingSource("stream-a")
	if err != nil {
		t.Fatalf("GetStreamingSource() error = %v", err)
	}
	if got1 == got2 {
		t.Fatal("expected factory to return new instances")
	}
}

func TestRegisterDestination_Success(t *testing.T) {
	resetForTest()
	t.Cleanup(resetForTest)

	RegisterDestination("dest-a", func() any { return &testConnector{id: "d1"} })

	got1, err := GetDestination("dest-a")
	if err != nil {
		t.Fatalf("GetDestination() error = %v", err)
	}
	got2, err := GetDestination("dest-a")
	if err != nil {
		t.Fatalf("GetDestination() error = %v", err)
	}
	if got1 == got2 {
		t.Fatal("expected factory to return new instances")
	}
}

func TestRegistry_FactoryCalledEachTime(t *testing.T) {
	resetForTest()
	t.Cleanup(resetForTest)

	calls := 0
	RegisterBatchSource("batch-e", func() any {
		calls++
		return &testConnector{id: "e"}
	})

	_, err := GetBatchSource("batch-e")
	if err != nil {
		t.Fatalf("GetBatchSource() error = %v", err)
	}
	_, err = GetBatchSource("batch-e")
	if err != nil {
		t.Fatalf("GetBatchSource() error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected factory to be called twice, got %d", calls)
	}
}
