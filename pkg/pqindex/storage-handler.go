package pqindex

import (
	"database/sql"

	"go4.org/jsonconfig"
	"perkeep.org/pkg/blobserver"
)

func init() {
	blobserver.RegisterStorageConstructor("postgres-storage", newStorageFromConfig)
}

func newStorageFromConfig(loader blobserver.Loader, cfg jsonconfig.Obj) (blobserver.Storage, error) {
	pqLoc := cfg.RequiredString("postgres-location")

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", pqLoc)
	if err != nil {
		return nil, err
	}
	return FromDB(db), nil
}
