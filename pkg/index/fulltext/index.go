package fulltext

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"time"

	"github.com/blevesearch/bleve"

	"perkeep.org/internal/magic"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/index"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/types/camtypes"
)

var mimeIndexers = map[string]func(blob.Ref, io.Reader) (interface{}, error){
	"text/plain":      indexText,
	"application/pdf": indexPdf,
}

type Index struct {
	blobSrc blobserver.Storage
	db      *sql.DB

	// Protects everything below:
	mu chan struct{}

	bleveIndex bleve.Index
}

type Common struct {
	Type string

	// The "version" of the indexer that indexed this item. This allows
	// future versions of perkeep to work out which blobs should be
	// re-indexed For example, if we add support for a new mime type
	// "application/example" in version N, then we can search for blobs
	// where the mime type is application/example and IndexByVersion is
	// less than N, and re-index those blobs.
	IndexedByVersion int
}

// The current "version" of the indexer. See the comments in
// Common.IndexedByVersion; we set that field to this value when indexing
// blobs.
const currentIndexerVersion = 1

type File struct {
	Filename, MIMEType string
	Size               int64
	ModTime            *time.Time
	Content            interface{}
	Common
}

func New(db *sql.DB, bleveIndex bleve.Index, blobSrc blobserver.Storage) (*Index, error) {
	err := initDB(db)
	if err != nil {
		return nil, err
	}
	ret := &Index{
		db:         db,
		blobSrc:    blobSrc,
		mu:         make(chan struct{}, 1),
		bleveIndex: bleveIndex,
	}
	ret.mu <- struct{}{}
	return ret, nil
}

func (ix *Index) EnumerateBlobs(
	ctx context.Context,
	dest chan<- blob.SizedRef,
	after string,
	limit int,
) error {
	defer close(dest)
	rows, err := ix.db.QueryContext(
		ctx,
		`
		SELECT blob_ref, size
		FROM indexed_blobs
		WHERE blob_ref > ?
		LIMIT ?
		`,
		after,
		limit,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			refStr string
			size   uint32
		)
		err = rows.Scan(&refStr, &size)
		if err != nil {
			return err
		}
		br, ok := blob.Parse(refStr)
		if !ok {
			// TODO: maybe fail more loudly? This shouldn't happen.
			return fmt.Errorf("Failed to parse blob ref: %q", br)
		}
		sr := blob.SizedRef{
			Ref:  br,
			Size: size,
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case dest <- sr:
		}
	}
	return rows.Err()
}

func (ix *Index) Fetch(ctx context.Context, ref blob.Ref) (b io.ReadCloser, size uint32, err error) {
	return ix.blobSrc.Fetch(ctx, ref)
}

func (ix *Index) ReceiveBlob(
	ctx context.Context,
	br blob.Ref,
	src io.Reader,
) (sr blob.SizedRef, err error) {
	data, err := ioutil.ReadAll(src)
	if err != nil {
		return blob.SizedRef{}, err
	}
	sr = blob.SizedRef{
		Ref:  br,
		Size: uint32(len(data)),
	}
	isSchemaBlob := schema.LikelySchemaBlob(data)
	defer func() {
		var status string
		if !isSchemaBlob {
			status = "skipped"
		} else if err == nil {
			status = "indexed"
		} else {
			log.Printf("Error indexing %v: %v", br, err)
			status = "errored"
		}

		_, sqlErr := ix.db.Exec(`
			INSERT INTO indexed_blobs
			(blob_ref, size, status)
			VALUES(?, ?, ?)
			`,
			br.String(),
			sr.Size,
			status,
		)

		if err == nil && sqlErr != nil {
			err = sqlErr
		}
	}()
	if !isSchemaBlob {
		return
	}
	sniffer := index.NewBlobSniffer(br)
	_, err = sniffer.Write(data)
	if err != nil {
		return
	}

	sniffer.Parse()
	b, ok := sniffer.SchemaBlob()
	isSchemaBlob = ok
	if !ok {
		return
	}
	err = ix.indexBlob(sniffer, b)
	return
}

func (ix *Index) RemoveBlobs(ctx context.Context, blobs []blob.Ref) error {
	return blobserver.ErrNotImplemented
}

func (ix *Index) StatBlobs(ctx context.Context, blobs []blob.Ref, fn func(blob.SizedRef) error) error {
	for _, v := range blobs {
		err := ctx.Err()
		if err != nil {
			return err
		}

		row := ix.db.QueryRow(`
			SELECT size
			FROM indexed_blobs
			WHERE blob_ref = ?
			LIMIT 1
			`,
			v.String(),
		)
		param := blob.SizedRef{
			Ref: v,
		}
		err = row.Scan(&param.Size)
		if err != nil {
			return err
		}
		err = fn(param)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ix *Index) indexBlob(sniffer *index.BlobSniffer, b *schema.Blob) (err error) {
	switch b.Type() {
	case "claim":
		claim, _ := b.AsClaim()
		err = ix.indexClaimBlob(&claim)
	case "file":
		var r *schema.FileReader
		r, err = b.NewFileReader(ix.blobSrc)
		if err != nil {
			return
		}
		defer r.Close()
		err = ix.indexFile(b, r)
	default:
		log.Printf("Unknown blob type %q; not indexing.", b.Type())
	}
	return
}

func (ix *Index) indexClaimBlob(b *schema.Claim) (err error) {
	claim := &camtypes.Claim{}

	dateStr := b.ClaimDateString()

	claim.Date, err = time.Parse(time.RFC3339Nano, dateStr)
	if err != nil {
		claim.Date, err = time.Parse(time.RFC3339, dateStr)
	}
	if err != nil {
		return
	}

	claim.BlobRef = b.Blob().BlobRef()
	claim.Type = b.ClaimType()
	claim.Attr = b.Attribute()
	claim.Value = b.Value()
	claim.Permanode = b.ModifiedPermanode()
	claim.Signer = b.Signer()
	claim.Target = b.Target()

	tx, err := ix.db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	err = handleClaim(tx, claim)
	if err == nil {
		err = tx.Commit()
	}
	return
}

func (ix *Index) indexFile(b *schema.Blob, r *schema.FileReader) (err error) {
	blobRef := b.BlobRef()

	meta := &File{
		Common: Common{
			Type:             "file",
			IndexedByVersion: currentIndexerVersion,
		},
		Filename: r.FileName(),
		Size:     r.Size(),
		MIMEType: magic.MIMETypeFromReaderAt(r),
	}
	modTime := r.ModTime()
	if !modTime.IsZero() {
		meta.ModTime = &modTime
	}

	tooBig := meta.Size > 100*1024*1024

	f, ok := mimeIndexers[meta.MIMEType]
	if ok && !tooBig {
		// If we know about the mime type, index the contents. Otherwise,
		// we still index the metadata, but leave the contents nil.
		var data interface{}
		data, err = f(blobRef, r)
		if err != nil {
			return
		}
		meta.Content = data
	}
	err = ix.indexBleve(blobRef.String(), meta)
	return
}

func (ix *Index) lockBleve()   { <-ix.mu }
func (ix *Index) unlockBleve() { ix.mu <- struct{}{} }

func (ix *Index) indexBleve(id string, data interface{}) error {
	ix.lockBleve()
	defer ix.unlockBleve()
	return ix.bleveIndex.Index(id, data)
}

func (ix *Index) bleveSearch(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	ix.lockBleve()
	defer ix.unlockBleve()
	return ix.bleveIndex.Search(req)
}
