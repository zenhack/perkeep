package fulltext

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/blevesearch/bleve"
	"go4.org/jsonconfig"
	"perkeep.org/pkg/blobserver"

	_ "github.com/mattn/go-sqlite3"
)

func init() {
	blobserver.RegisterStorageConstructor("fulltext-index", newStoreFromConfig)
	blobserver.RegisterHandlerConstructor("fulltext-search", newHandlerFromConfig)
}

func newStoreFromConfig(loader blobserver.Loader, cfg jsonconfig.Obj) (bs blobserver.Storage, err error) {
	blobSrcPrefix := cfg.RequiredString("blobSource")

	dbCfg := cfg.RequiredObject("sql")
	dbDriver := dbCfg.RequiredString("driver")
	dbLoc := dbCfg.RequiredString("location")

	bleveCfg := cfg.RequiredObject("bleve")
	blevePath := bleveCfg.RequiredString("path")

	if err = cfg.Validate(); err != nil {
		return
	}

	blobSrc, err := loader.GetStorage(blobSrcPrefix)
	if err != nil {
		return
	}

	db, err := sql.Open(dbDriver, dbLoc)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			db.Close()
		}
	}()

	bleveIdx, err := bleve.Open(blevePath)
	if err == bleve.ErrorIndexPathDoesNotExist {
		bleveIdx, err = bleve.New(blevePath, bleve.NewIndexMapping())
	}
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			bleveIdx.Close()
		}
	}()
	return New(db, bleveIdx, blobSrc)
}

func newHandlerFromConfig(loader blobserver.Loader, cfg jsonconfig.Obj) (http.Handler, error) {
	indexPath := cfg.RequiredString("index")

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	index, err := loader.GetStorage(indexPath)
	if err != nil {
		return nil, err
	}

	fullTextIndex, ok := index.(*Index)
	if !ok {
		return nil, fmt.Errorf("Not a full text index")
	}

	return &fullTextSearch{index: fullTextIndex}, nil
}

type fullTextSearch struct {
	index *Index
}

func (fullTextSearch) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(500)
	w.Write([]byte("Not implemented"))
}
