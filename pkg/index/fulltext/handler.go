package fulltext

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/blevesearch/bleve"
	bleveQuery "github.com/blevesearch/bleve/search/query"
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

type search struct {
	MatchText json.RawMessage `json:"matchText"`
}

func (s *fullTextSearch) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	search := &search{}
	err := json.NewDecoder(req.Body).Decode(search)
	if err != nil {
		log.Print("Failed to decode query: ", err)
		w.WriteHeader(400)
		return
	}
	q, err := bleveQuery.ParseQuery([]byte(search.MatchText))
	if err != nil {
		log.Print("Failed to parse bleve query: ", err)
		w.WriteHeader(400)
		return
	}
	searchReq := bleve.NewSearchRequest(q)
	searchRes, err := s.index.bleveSearch(searchReq)
	if err != nil {
		log.Print("Bleve search failed: ", err)
		w.WriteHeader(500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	err = enc.Encode(searchRes)
	if err != nil {
		log.Print("Failed to encode search result: ", err)
	}
}
