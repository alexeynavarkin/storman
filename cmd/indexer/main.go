package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"

	config "github.com/ThomasObenaus/go-conf"
	"go.uber.org/zap"

	"github.com/alexeynavarkin/storman/internal/connector"
	"github.com/alexeynavarkin/storman/internal/indexer"
	"github.com/alexeynavarkin/storman/internal/repository/index_repo"
)

type Config struct {
	Target struct {
		URL string `cfg:"{'name': 'url'}"`
	} `cfg:"{'name': 'target'}"`

	IndexStorage struct {
		URL string `cfg:"{'name': 'url'}"`
	} `cfg:"{'name': 'index_storage'}"`
}

func main() {
	lg := zap.Must(zap.NewProduction())

	cfg := Config{}

	cfgProvider, err := config.NewConfigProvider(
		&cfg,
		"STORMAN_INDEXER",
		"STORMAN_INDEXER",
	)
	if err != nil {
		lg.Fatal("failed to build config provider", zap.Error(err))
	}

	err = cfgProvider.ReadConfig(os.Args)
	if err != nil {
		fmt.Println(cfgProvider.Usage())
		os.Exit(-1)
	}

	targetURL, err := url.Parse(cfg.Target.URL)
	if err != nil {
		lg.Fatal("failed to parse target url", zap.Error(err))
	}
	conCfg := connector.WebdavConnectorConfig{
		BaseURL:  targetURL.Scheme + "://" + targetURL.Host + targetURL.Path,
		BasePath: "",
		Username: targetURL.User.Username(),
	}
	targetPassword, targetHasPassword := targetURL.User.Password()
	if targetHasPassword {
		conCfg.Password = targetPassword
	}
	con := connector.NewWebdavConnector(conCfg)

	db, err := sql.Open("postgres", cfg.IndexStorage.URL)
	if err != nil {
		lg.Fatal("failed to open db", zap.Error(err))
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		lg.Fatal("failed to ping db", zap.Error(err))
	}

	idx := index_repo.NewPostgresIndex(db)
	indexBuilder := indexer.NewIndexer(
		idx,
		con,
		lg,
	)
	err = indexBuilder.Index(context.Background())
	if err != nil {
		lg.Fatal("failed to build index", zap.Error(err))
	}
}
