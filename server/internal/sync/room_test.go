package sync

import (
	"testing"
	"time"
)

func TestRoomBroadcastSkipsSender(t *testing.T) {
	room := NewRoom("test", nil, nil)
	go room.Run()

	sender := NewClient(room, nil)
	receiver := NewClient(room, nil)

	room.Join(sender)
	room.Join(receiver)
	room.Broadcast(Message{Sender: sender, Data: []byte{1, 2, 3}})

	select {
	case got := <-receiver.send:
		wire, ok, err := DecodeWireMessage(got)
		if err != nil || !ok {
			t.Fatalf("receiver got invalid wire message: ok=%v err=%v", ok, err)
		}
		if wire.Type != WireUpdate || string(wire.Payload) != string([]byte{1, 2, 3}) {
			t.Fatalf("receiver got %v", wire)
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
	room := NewRoom("test", nil, nil)
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
			wire, ok, err := DecodeWireMessage(got)
			if err != nil || !ok {
				t.Fatalf("late joiner got invalid wire message: ok=%v err=%v", ok, err)
			}
			if wire.Type != WireUpdate || string(wire.Payload) != string(want) {
				t.Fatalf("late joiner got %v, want %v", wire, want)
			}
		case <-time.After(time.Second):
			t.Fatal("late joiner did not get history")
		}
	}
}

func TestRoomRespondsToSyncWithMissingUpdates(t *testing.T) {
	room := NewRoom("test", nil, [][]byte{testUpdate(1, 1), testUpdate(1, 2)})
	go room.Run()

	client := NewClient(room, nil)
	room.Join(client)
	<-client.send
	<-client.send

	room.Handle(Message{
		Sender: client,
		Data:   EncodeWireMessage(WireMessage{Type: WireSync, Payload: testStateVector(1, 1)}),
	})

	select {
	case got := <-client.send:
		wire, ok, err := DecodeWireMessage(got)
		if err != nil || !ok {
			t.Fatalf("got invalid wire message: ok=%v err=%v", ok, err)
		}
		if wire.Type != WireUpdate || string(wire.Payload) != string(testUpdate(1, 2)) {
			t.Fatalf("got %v", wire)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not get sync diff")
	}
}

func TestRoomBroadcastsPresenceWithoutPersisting(t *testing.T) {
	room := NewRoom("test", nil, nil)
	go room.Run()

	sender := NewClient(room, nil)
	receiver := NewClient(room, nil)
	room.Join(sender)
	room.Join(receiver)

	room.Handle(Message{
		Sender: sender,
		Data:   EncodeWireMessage(WireMessage{Type: WirePresence, Payload: []byte(`{"user":"Ada"}`)}),
	})

	select {
	case got := <-receiver.send:
		wire, ok, err := DecodeWireMessage(got)
		if err != nil || !ok {
			t.Fatalf("got invalid wire message: ok=%v err=%v", ok, err)
		}
		if wire.Type != WirePresence || string(wire.Payload) != `{"user":"Ada"}` {
			t.Fatalf("got %v", wire)
		}
	case <-time.After(time.Second):
		t.Fatal("receiver did not get presence")
	}

	if len(room.history) != 0 {
		t.Fatalf("presence should not be persisted in history")
	}
}

func testStateVector(client, clock uint64) []byte {
	data := make([]byte, 20)
	data[3] = 1
	for shift := 0; shift < 8; shift++ {
		data[4+shift] = byte(client >> (56 - shift*8))
		data[12+shift] = byte(clock >> (56 - shift*8))
	}
	return data
}

func testUpdate(client, clock uint64) []byte {
	data := make([]byte, 58)
	data[3] = 1
	for shift := 0; shift < 8; shift++ {
		data[4+shift] = byte(client >> (56 - shift*8))
		data[12+shift] = byte(clock >> (56 - shift*8))
	}
	data[53] = 3
	return data
}
