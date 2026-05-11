package flat

import "os"

type flatReader struct {
	file *os.File
	size int64
}

func (r *flatReader) ReadAt(p []byte, off int64) (int, error) {
	return r.file.ReadAt(p, off)
}

func (r *flatReader) Close() error { return r.file.Close() }

func (r *flatReader) Size() int64 { return r.size }
