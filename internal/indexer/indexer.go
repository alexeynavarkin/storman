package indexer

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/alexeynavarkin/storman/internal/connector"
	"github.com/alexeynavarkin/storman/internal/repository/index_repo"
)

const (
	traverseChanSize = 4096
	indexWorkerCount = 5
)

type Indexer struct {
	index index_repo.Index
	con   connector.Connector
	lg    *zap.Logger
}

func NewIndexer(
	idx index_repo.Index,
	con connector.Connector,
	lg *zap.Logger,
) *Indexer {
	return &Indexer{
		index: idx,
		con:   con,
		lg:    lg,
	}
}

func (idxr *Indexer) Index(ctx context.Context) error {
	objCh := make(chan connector.Object, traverseChanSize)

	go func() {
		err := idxr.con.Traverse(ctx, objCh)
		if err != nil {
			idxr.lg.Error("traverse failed", zap.Error(err))
		}
	}()

	wg := sync.WaitGroup{}
	for range indexWorkerCount {
		wg.Add(1)
		go idxr.worker(ctx, objCh)
	}

	wg.Wait()

	return nil
}

func (idxr *Indexer) worker(ctx context.Context, objCh chan connector.Object) {
	for obj := range objCh {
		lg := idxr.lg.With(zap.Any("object", obj))

		storedObj, err := idxr.index.GetByPath(obj.Path)
		if err != nil {
			lg.Error("failed to get obj from db", zap.Error(err))
			continue
		}
		if storedObj != nil {
			lg.Info("file exists in index, skip")
			continue
		}

		lg.Info("scanning file")
		objReader, err := idxr.con.Get(ctx, obj)
		if err != nil {
			lg.Error("failed to get file reader", zap.Error(err))
			continue
		}
		defer objReader.Close()

		repoObj := index_repo.Object{
			Path:              obj.Path,
			Name:              obj.Name,
			SizeBytes:         obj.SizeBytes,
			ModifiedTimestamp: obj.ModifiedTimestamp,
			CreatedTimestamp:  obj.CreatedTimestamp,
		}

		wg := sync.WaitGroup{}
		readers := splitReader(objReader, 2)

		// Detect content type.
		wg.Add(1)
		go func() {
			defer wg.Done()
			objContentType, err := detectContentType(readers[0])
			if err != nil {
				lg.Error("failed to guess content type")
				return
			}
			repoObj.ContentType = objContentType
			lg.Info("content type done")
		}()

		// Calculate SHA512 checksum.
		wg.Add(1)
		go func() {
			defer wg.Done()
			objSHA512, err := calculateSHA512(readers[1])
			if err != nil {
				lg.Error("failed to calculate sha512 checksum", zap.Error(err))
				return
			}
			repoObj.HashSumSHA512 = objSHA512
			lg.Info("sha512 done")
		}()

		wg.Wait()

		err = idxr.index.Store(repoObj)
		if err != nil {
			lg.Error("failed to store in index")
		}
	}
}
