package remux

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"golang.org/x/sys/unix"
)

// seekableFile exposes a path FFmpeg/ffprobe can open without writing to disk
// (Linux memfd, else tmpfs under /dev/shm — same strategy as Python mp4_buffer.py).
type seekableFile struct {
	path    string
	fd      int
	shmPath string // set when backed by /dev/shm; removed on cleanup
}

func (s *seekableFile) cleanup() {
	if s.fd >= 0 {
		_ = unix.Close(s.fd)
		s.fd = -1
	}
	if s.shmPath != "" {
		_ = os.Remove(s.shmPath)
		s.shmPath = ""
	}
}

func newSeekableBytes(data []byte, suffix string) (*seekableFile, error) {
	if runtime.GOOS == "linux" {
		if sf, err := newMemfdFile(data, suffix); err == nil {
			return sf, nil
		}
	}
	return newShmFile(data, suffix)
}

func newMemfdFile(data []byte, suffix string) (*seekableFile, error) {
	name := "tgtv" + strings.TrimPrefix(suffix, ".")
	fd, err := unix.MemfdCreate(name, 0)
	if err != nil {
		return nil, err
	}
	if _, err := unix.Write(fd, data); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	if _, err := unix.Seek(fd, 0, unix.SEEK_SET); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return &seekableFile{
		path: procFDPath(fd),
		fd:   fd,
	}, nil
}

func newShmFile(data []byte, suffix string) (*seekableFile, error) {
	dir := "/dev/shm"
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("memfd unavailable and %s missing: %w", dir, err)
	}
	f, err := os.CreateTemp(dir, "tgtv-*"+suffix)
	if err != nil {
		return nil, err
	}
	path := f.Name()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return &seekableFile{path: path, fd: -1, shmPath: path}, nil
}

func procFDPath(fd int) string {
	return fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), fd)
}
