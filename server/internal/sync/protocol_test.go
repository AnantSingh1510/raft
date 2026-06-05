package sync

import "testing"

func TestWireMessagesRoundTrip(t *testing.T) {
	want := WireMessage{Type: WirePresence, Payload: []byte(`{"ok":true}`)}

	got, ok, err := DecodeWireMessage(EncodeWireMessage(want))
	if err != nil || !ok {
		t.Fatalf("decode failed: ok=%v err=%v", ok, err)
	}
	if got.Type != want.Type || string(got.Payload) != string(want.Payload) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestUpdateHasMissing(t *testing.T) {
	update := testUpdate(1, 2)

	if !UpdateHasMissing(update, StateVector{1: 1}) {
		t.Fatal("update should be missing")
	}
	if UpdateHasMissing(update, StateVector{1: 2}) {
		t.Fatal("update should not be missing")
	}
}
