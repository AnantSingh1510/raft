package sync

import (
	"testing"
	"time"
)

func TestRoomBroadcastSkipsSender(t *testing.T) {
	room := NewRoom("test")
	go room.Run()

	sender := NewClient(room, nil)
	receiver := NewClient(room, nil)

	room.Join(sender)
	room.Join(receiver)
	room.Broadcast(Message{Sender: sender, Data: []byte{1, 2, 3}})

	select {
	case got := <-receiver.send:
		if string(got) != string([]byte{1, 2, 3}) {
			t.Fatalf("receiver got %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("receiver did not get broadcast")
	}

	select {
	case got := <-sender.send:
		t.Fatalf("sender should not receive own message, got %v", got)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestRoomReplaysHistoryToLateJoiner(t *testing.T) {
	room := NewRoom("test")
	go room.Run()

	sender := NewClient(room, nil)
	room.Join(sender)
	room.Broadcast(Message{Sender: sender, Data: []byte{1}})
	room.Broadcast(Message{Sender: sender, Data: []byte{2}})
	time.Sleep(25 * time.Millisecond)

	late := NewClient(room, nil)
	room.Join(late)

	for _, want := range [][]byte{{1}, {2}} {
		select {
		case got := <-late.send:
			if string(got) != string(want) {
				t.Fatalf("late joiner got %v, want %v", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("late joiner did not get history")
		}
	}
}
