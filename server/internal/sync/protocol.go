package sync

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	wireMagic0  byte = 0x52
	wireMagic1  byte = 0x46
	wireVersion byte = 1

	WireSync        byte = 1
	WireStateVector byte = 2
	WireUpdate      byte = 3
	WirePresence    byte = 4
	WireError       byte = 5
)

type WireMessage struct {
	Type    byte
	Payload []byte
}

type StateVector map[uint64]uint64

func EncodeWireMessage(message WireMessage) []byte {
	data := make([]byte, 8+len(message.Payload))
	data[0] = wireMagic0
	data[1] = wireMagic1
	data[2] = wireVersion
	data[3] = message.Type
	binary.BigEndian.PutUint32(data[4:8], uint32(len(message.Payload)))
	copy(data[8:], message.Payload)
	return data
}

func DecodeWireMessage(data []byte) (WireMessage, bool, error) {
	if len(data) < 2 || data[0] != wireMagic0 || data[1] != wireMagic1 {
		return WireMessage{}, false, nil
	}
	if len(data) < 8 {
		return WireMessage{}, true, errors.New("wire message header is incomplete")
	}
	if data[2] != wireVersion {
		return WireMessage{}, true, fmt.Errorf("unsupported wire version: %d", data[2])
	}
	if data[3] < WireSync || data[3] > WireError {
		return WireMessage{}, true, fmt.Errorf("invalid wire message type: %d", data[3])
	}
	length := int(binary.BigEndian.Uint32(data[4:8]))
	if len(data) != 8+length {
		return WireMessage{}, true, errors.New("wire message length does not match payload")
	}
	return WireMessage{Type: data[3], Payload: append([]byte(nil), data[8:]...)}, true, nil
}

func DecodeStateVector(data []byte) (StateVector, error) {
	if len(data) < 4 {
		return nil, errors.New("state vector is incomplete")
	}
	count := int(binary.BigEndian.Uint32(data[:4]))
	if len(data) != 4+count*16 {
		return nil, errors.New("state vector length is invalid")
	}
	vector := StateVector{}
	offset := 4
	for i := 0; i < count; i++ {
		client := binary.BigEndian.Uint64(data[offset : offset+8])
		clock := binary.BigEndian.Uint64(data[offset+8 : offset+16])
		offset += 16
		if client == 0 {
			return nil, errors.New("state vector client id must be non-zero")
		}
		vector[client] = clock
	}
	return vector, nil
}

func UpdateHasMissing(update []byte, vector StateVector) bool {
	if len(update) < 4 {
		return true
	}
	count := int(binary.BigEndian.Uint32(update[:4]))
	offset := 4
	for i := 0; i < count; i++ {
		if offset+54 > len(update) {
			return true
		}
		client := binary.BigEndian.Uint64(update[offset : offset+8])
		clock := binary.BigEndian.Uint64(update[offset+8 : offset+16])
		offset += 16 + 16 + 16 + 1
		offset += 1
		contentLen := int(binary.BigEndian.Uint32(update[offset : offset+4]))
		offset += 4
		if offset+contentLen > len(update) {
			return true
		}
		offset += contentLen
		if clock > vector[client] {
			return true
		}
	}
	return offset != len(update)
}
