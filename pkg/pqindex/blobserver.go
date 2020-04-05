package pqindex

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/schema"

	_ "github.com/lib/pq"
)

var (
	_ blobserver.Storage = handleStorage{}
	_ blobserver.Storage = &Server{}
)

type sqlHandle interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

type RoStorage interface {
	blob.Fetcher
	blobserver.BlobStatter
	blobserver.BlobEnumerator
}

type Server struct {
	handleStorage
	db *sql.DB
}

func withTx(ctx context.Context, opts *sql.TxOptions, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	err = fn(tx)
	if err == nil {
		return tx.Commit()
	}
	return err
}

func (s *Server) RwTx(ctx context.Context, fn func(blobserver.Storage) error) error {
	return withTx(ctx, nil, s.db, func(tx *sql.Tx) error {
		return fn(handleStorage{h: tx})
	})
}

var roTxOpts = &sql.TxOptions{ReadOnly: true}

func (s *Server) RoTx(ctx context.Context, fn func(RoStorage) error) error {
	return withTx(ctx, roTxOpts, s.db, func(tx *sql.Tx) error {
		return fn(handleStorage{h: tx})
	})
}

type handleStorage struct {
	RoStorage
	h sqlHandle
}

func FromDB(db *sql.DB) *Server {
	return &Server{
		handleStorage: handleStorage{h: db},
		db:            db,
	}
}

func (s handleStorage) Init() error {
	_, err := s.h.Exec(`CREATE TABLE IF NOT EXISTS schema_blobs (
		blobref VARCHAR PRIMARY KEY,
		parsed_json JSON NOT NULL
	)`)
	return err
}

func placeHolders(start int, count int) string {
	buf := &bytes.Buffer{}
	buf.WriteRune('(')
	if count > 0 {
		fmt.Fprintf(buf, "$%d", start)
	}
	for i := start + 1; i <= count; i++ {
		fmt.Fprintf(buf, ",$%d", i)
	}
	buf.WriteRune(')')
	return buf.String()
}

func blobRefsSql(start int, brs ...blob.Ref) (query string, args []interface{}) {
	args = make([]interface{}, len(brs))
	for i, v := range brs {
		args[i] = v.String()
	}
	return placeHolders(start, len(args)), args
}

func (s handleStorage) ReceiveBlob(ctx context.Context, br blob.Ref, source io.Reader) (blob.SizedRef, error) {
	data, err := ioutil.ReadAll(source)
	if err != nil {
		return blob.SizedRef{}, err
	}
	sr := blob.SizedRef{
		Ref:  br,
		Size: uint32(len(data)),
	}
	if !schema.LikelySchemaBlob(data) {
		return sr, nil
	}
	_, err = s.h.Exec(`
		INSERT INTO schema_blobs (blobref, parsed_json)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING`,
		br.String(),
		data,
	)
	return sr, err
}

func (s handleStorage) RemoveBlobs(ctx context.Context, blobs []blob.Ref) error {
	query, args := blobRefsSql(1, blobs...)
	query = "DELETE FROM schema_blobs WHERE blobref IN " + query
	_, err := s.h.Exec(query, args...)
	return err
}
