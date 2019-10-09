package fulltext

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"

	"go4.org/jsonconfig"

	"perkeep.org/internal/httputil"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/search"
	"perkeep.org/pkg/types/camtypes"

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

func (s *fullTextSearch) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	suffix := httputil.PathSuffix(req)
	path := strings.TrimPrefix(suffix, "camli/search/")
	switch {
	case req.Method == "POST":
		s.searchPost(path, w, req)
	case httputil.IsGet(req):
	default:
		httputil.ReturnJSON(w, &camtypes.SearchErrorResponse{
			Error:     "Unsupported search method",
			ErrorType: "Input",
		})
		return
	}
}

func (s *fullTextSearch) searchPost(path string, w http.ResponseWriter, req *http.Request) {
	switch path {
	case "query":
		s.searchQuery(w, req)
	case "describe":
		// We'll want to implement this at some point, but for now send it
		// to the catchall.
		fallthrough
	default:
		httputil.ReturnJSON(w, &camtypes.SearchErrorResponse{
			Error:     "Unsupported search path",
			ErrorType: "Input",
		})
	}
}

func (s *fullTextSearch) searchQuery(w http.ResponseWriter, req *http.Request) {
	rawq := &search.SearchQuery{}
	err := json.NewDecoder(req.Body).Decode(rawq)
	if err != nil {
		log.Print("Failed to decode query: ", err)
		w.WriteHeader(400)
		return
	}
	res, err := s.Query(req.Context(), rawq)
	if err != nil {
		log.Print("Query failed: ", err)
		w.WriteHeader(500)
		return
	}
	httputil.ReturnJSON(w, res)
}

func (s *fullTextSearch) Query(ctx context.Context, rawq *search.SearchQuery) (*search.SearchResult, error) {
	if rawq.Constraint == nil || rawq.Constraint.Text == nil {
		return nil, fmt.Errorf("Unsupported")
	}
	q, err := query.ParseQuery([]byte(rawq.Constraint.Text))
	if err != nil {
		return nil, fmt.Errorf("Error parsing bleve query: %q", err)
	}

	searchReq := bleve.NewSearchRequest(q)
	searchRes, err := s.index.bleveSearch(searchReq)
	if err != nil {
		return nil, err
	}

	res := &search.SearchResult{}

	for _, hit := range searchRes.Hits {
		ref, ok := blob.Parse(hit.ID)
		if !ok {
			return nil, fmt.Errorf("Warning: Invalid blobref in bleve index")
		}
		res.Blobs = append(res.Blobs, &search.SearchResultBlob{
			Blob: ref,
		})
	}

	return res, nil
}
