package indexer

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/alexeynavarkin/storman/internal/connector"
	"github.com/alexeynavarkin/storman/internal/repository/index_repo"
)

const (
	traverseChanSize = 1024
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

func (i *Indexer) Index(ctx context.Context) error {
	objCh := make(chan connector.Object, traverseChanSize)

	go func() {
		err := i.con.Traverse(ctx, objCh)
		if err != nil {
			i.lg.Error("traverse failed", zap.Error(err))
		}
	}()

	for obj := range objCh {
		lg := i.lg.With(zap.Any("object", obj))
		lg.Info("scanning file")
		objReader, err := i.con.Get(ctx, obj)
		if err != nil {
			return err
		}
		defer objReader.Close()

		repoObj := index_repo.Object{
			Path:              obj.Path,
			Name:              obj.Name,
			Size:              obj.Size,
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
		err = i.index.Store(repoObj)
		if err != nil {
			return err
		}
	}

	return nil
}
