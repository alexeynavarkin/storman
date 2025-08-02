package connector

import (
	"context"
	"io"

	"github.com/studio-b12/gowebdav"

	"github.com/alexeynavarkin/storman/pkg/utils"
)

type WebdavConnectorConfig struct {
	BaseURL  string
	BasePath string
	Username string
	Password string
}

type WebdavConnector struct {
	baseURL  string
	basePath string

	webdavClient *gowebdav.Client
}

func NewWebdavConnector(cfg WebdavConnectorConfig) Connector {
	return &WebdavConnector{
		baseURL:  cfg.BaseURL,
		basePath: cfg.BasePath,
		webdavClient: gowebdav.NewClient(
			cfg.BaseURL,
			cfg.Username,
			cfg.Password,
		),
	}
}

func (wi *WebdavConnector) Traverse(ctx context.Context, objCh chan Object) error {
	queue := make([]string, 0)
	queue = append(queue, wi.basePath)

	for len(queue) > 0 {
		path := queue[0]
		queue = queue[1:]

		objects, err := wi.webdavClient.ReadDir(path)
		if err != nil {
			return err
		}

		for _, obj := range objects {
			objPath := gowebdav.Join(path, obj.Name())
			if obj.IsDir() {
				queue = append(queue, objPath)
				continue
			}

			objCh <- Object{
				Name:              obj.Name(),
				Path:              objPath,
				SizeBytes:         uint64(obj.Size()),
				ModifiedTimestamp: utils.Ptr(obj.ModTime()),
				CreatedTimestamp:  nil,
			}
		}
	}

	return nil
}

func (c *WebdavConnector) Get(ctx context.Context, obj Object) (io.ReadCloser, error) {
	return c.webdavClient.ReadStream(obj.Path)
}
