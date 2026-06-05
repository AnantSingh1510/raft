package sync

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Store interface {
	Load(roomID string) ([][]byte, error)
	Append(roomID string, update []byte) error
}

type FileStore struct {
	dir string
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) Load(roomID string) ([][]byte, error) {
	file, err := os.Open(s.path(roomID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var updates [][]byte
	for {
		var length uint32
		if err := binary.Read(file, binary.BigEndian, &length); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}

		update := make([]byte, length)
		if _, err := io.ReadFull(file, update); err != nil {
			return nil, err
		}
		updates = append(updates, update)
	}

	return updates, nil
}

func (s *FileStore) Append(roomID string, update []byte) error {
	file, err := os.OpenFile(s.path(roomID), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := binary.Write(file, binary.BigEndian, uint32(len(update))); err != nil {
		return err
	}
	_, err = file.Write(update)
	return err
}

func (s *FileStore) path(roomID string) string {
	name := strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(roomID)
	return filepath.Join(s.dir, name+".raftlog")
}
