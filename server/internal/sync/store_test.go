package sync

import (
	"testing"
	"time"
)

func TestFileStorePersistsRoomHistory(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Append("notes", []byte{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append("notes", []byte{3}); err != nil {
		t.Fatal(err)
	}

	got, err := store.Load("notes")
	if err != nil {
		t.Fatal(err)
	}

	assertUpdates(t, got, [][]byte{{1, 2}, {3}})
}

func TestHubLoadsPersistedHistoryForRoom(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append("notes", []byte{9}); err != nil {
		t.Fatal(err)
	}

	hub, err := NewHub(HubOptions{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	room := hub.room("notes")

	late := NewClient(room, nil)
	room.Join(late)

	select {
	case got := <-late.send:
		if string(got) != string([]byte{9}) {
			t.Fatalf("late joiner got %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("late joiner did not get persisted history")
	}
}

func assertUpdates(t *testing.T, got, want [][]byte) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d updates, want %d", len(got), len(want))
	}
	for index := range got {
		if string(got[index]) != string(want[index]) {
			t.Fatalf("update %d got %v, want %v", index, got[index], want[index])
		}
	}
}
