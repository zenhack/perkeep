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
	h sqlHandle
}

func FromDB(db *sql.DB) *Server {
	return &Server{
		handleStorage: handleStorage{h: db},
		db:            db,
	}
}

func (s handleStorage) Init() error {
	_, err := s.h.Exec(`CREATE TABLE IF NOT EXISTS blobs (
		blobref VARCHAR PRIMARY KEY,
		size INTEGER NOT NULL,
		content BYTEA NOT NULL
	)`)
	return err
}

func (s handleStorage) Fetch(ctx context.Context, br blob.Ref) (blob io.ReadCloser, size uint32, err error) {
	data := []byte{}
	row := s.h.QueryRow(
		`SELECT (size, blob)
		FROM blobs
		WHERE blobref = $1`,
		br.String(),
	)
	err = row.Scan(&size, &data)
	blob = ioutil.NopCloser(bytes.NewBuffer(data))
	return
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
	_, err = s.h.Exec(`
		INSERT INTO blobs (blobref, size, content)
		VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`,
		br.String(),
		len(data),
		data,
	)
	return blob.SizedRef{
		Ref:  br,
		Size: uint32(len(data)),
	}, err
}

func (s handleStorage) StatBlobs(ctx context.Context, blobs []blob.Ref, fn func(blob.SizedRef) error) error {
	query, args := blobRefsSql(1, blobs...)
	query = `SELECT (blobref, size) FROM blobs WHERE blobref IN ` + query
	rows, err := s.h.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for ctx.Err() == nil && rows.Next() {
		refStr := ""
		sr := blob.SizedRef{}
		err := rows.Scan(&refStr, &sr.Size)
		if err != nil {
			return err
		}
		// If this fails, the db is corrupt:
		sr.Ref = blob.MustParse(refStr)

		err = fn(sr)
		if err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (s handleStorage) EnumerateBlobs(
	ctx context.Context,
	dest chan<- blob.SizedRef,
	after string,
	limit int,
) error {
	defer close(dest)
	rows, err := s.h.Query(`
		SELECT (blobref, size) FROM blobs
		WHERE ref > $1
		ORDER BY blobref ASCENDING
		LIMIT $2
	`, after, limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		sr := blob.SizedRef{}
		refStr := ""
		err = rows.Scan(&refStr, &sr.Size)
		if err != nil {
			return err
		}
		sr.Ref = blob.MustParse(refStr)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case dest <- sr:
		}
	}
	return rows.Err()
}

func (s handleStorage) RemoveBlobs(ctx context.Context, blobs []blob.Ref) error {
	query, args := blobRefsSql(1, blobs...)
	query = "DELETE FROM blobs WHERE blobref IN " + query
	_, err := s.h.Exec(query, args...)
	return err
}
