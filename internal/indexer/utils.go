package indexer

import (
	"crypto/sha512"
	"fmt"
	"io"
	"slices"

	"github.com/gabriel-vasile/mimetype"
)

func splitReader(r io.Reader, n int) []io.ReadCloser {
	prs := make([]*io.PipeReader, n)
	pws := make([]*io.PipeWriter, n)
	readers := make([]io.ReadCloser, n)

	for i := 0; i < n; i++ {
		pr, pw := io.Pipe()
		prs[i] = pr
		pws[i] = pw
		readers[i] = pr
	}

	go func() {
		defer func() {
			for _, pw := range pws {
				pw.Close()
			}
		}()

		closedReaders := make([]int, 0)
		buf := make([]byte, 1024*32)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				for i := 0; i < len(pws); i++ {
					if slices.Contains(closedReaders, i) {
						continue
					}

					_, wrErr := pws[i].Write(buf[:n])
					if wrErr != nil {
						closedReaders = append(closedReaders, i)
					}
				}
			}
			if err != nil {
				break
			}
		}
	}()

	return readers
}

func calculateSHA512(reader io.ReadCloser) (string, error) {
	defer reader.Close()

	hash := sha512.New()
	_, err := io.Copy(hash, reader)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func detectContentType(reader io.ReadCloser) (string, error) {
	defer reader.Close()

	objMimetype, err := mimetype.DetectReader(reader)
	if err != nil {
		return "", err
	}

	return objMimetype.String(), nil
}
