package fulltext

import (
	"database/sql"
	"fmt"
	"math"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/types/camtypes"

	"github.com/elgris/sqrl"
)

func initDB(db *sql.DB) error {
	tables := []string{
		`
		CREATE TABLE IF NOT EXISTS indexed_blobs (
			blob_ref VARCHAR NOT NULL UNIQUE,
			size INTEGER NOT NULL,

			-- The status of the blob. Legal values:
			--
			-- * "indexed"
			-- * "errored"
			-- * "pending"
			-- * "skipped"
			status VARCHAR NOT NULL
		)
		`,
		`
		CREATE TABLE IF NOT EXISTS attr_claims (
			claim_ref VARCHAR NOT NULL UNIQUE,
			signer_ref VARCHAR NOT NULL,

			date INTEGER NOT NULL,
			type VARCHAR NOT NULL,

			attr VARCHAR,
			value VARCHAR,

			permanode_ref VARCHAR NOT NULL
		)
		`,
		`
		CREATE TABLE IF NOT EXISTS deleted (
			permanode_ref VARCHAR NOT NULL,
			signer_ref VARCHAR NOT NULL,
			date INTEGER NOT NULL
		)
		`,
		`
		CREATE TABLE IF NOT EXISTS attrs (
			permanode_ref VARCHAR NOT NULL,
			signer_ref VARCHAR NOT NULL,
			attr VARCHAR NOT NULL,
			value VARCHAR NOT NULL,
			valid_since INTEGER NOT NULL,
			valid_until INTEGER NOT NULL
		)
		`,
		`CREATE TABLE IF NOT EXISTS edges (
			from_ VARCHAR NOT NULL,
			to_ VARCHAR NOT NULL,
			UNIQUE(from_, to_)
		)`,
	}
	for _, v := range tables {
		if _, err := db.Exec(v); err != nil {
			return err
		}
	}
	return nil
}

func handleClaim(tx *sql.Tx, claim *camtypes.Claim) error {
	err := saveClaim(tx, claim)
	if err != nil {
		return err
	}

	switch schema.ClaimType(claim.Type) {
	case schema.DeleteClaim:
		return handleDelete(tx, claim)
	case schema.DelAttributeClaim:
		return handleDelAttribute(tx, claim)
	case schema.AddAttributeClaim:
		return handleAddAttribute(tx, claim)
	case schema.SetAttributeClaim:
		// delete then add.
		delClaim := *claim
		delClaim.Type = string(schema.DelAttributeClaim)
		delClaim.Value = ""
		err = handleDelAttribute(tx, &delClaim)
		if err != nil {
			return err
		}

		addClaim := *claim
		addClaim.Type = string(schema.AddAttributeClaim)
		return handleAddAttribute(tx, &addClaim)
	default:
		return fmt.Errorf("Unknown claim type: %q", claim.Type)
	}
}

func handleDelete(tx *sql.Tx, claim *camtypes.Claim) error {
	_, err := sqrl.Insert("deleted").SetMap(map[string]interface{}{
		"permanode_ref": claim.Target.String(),
		"signer_ref":    claim.Signer.String(),
		"date":          claim.Date.Unix(),
	}).RunWith(tx).Exec()
	return err
}

func handleAddAttribute(tx *sql.Tx, claim *camtypes.Claim) error {
	permanode := claim.Permanode.String()
	signer := claim.Signer.String()
	unixDate := claim.Date.Unix()

	// Search for a claim which overrides this one. If we find one, its date
	// will be the 'valid_until' field for this attribute entry.
	row := sqrl.Select("date").From("attr_claims").Where(sqrl.And{
		sqrl.Gt{"date": unixDate},

		sqrl.Eq{"attr": claim.Attr},
		sqrl.Eq{"permanode_ref": permanode},
		sqrl.Eq{"signer_ref": signer},

		sqrl.Or{
			sqrl.Eq{"type": "set-attribute"},
			sqrl.And{
				sqrl.Eq{"type": "del-attribute"},
				sqrl.Or{
					sqrl.Eq{"value": claim.Value},
					sqrl.Eq{"value": ""},
				},
			},
		},
	}).
		OrderBy("date ASC").
		Limit(1).
		RunWith(tx).
		QueryRow()

	var validUntil int64
	err := row.Scan(&validUntil)
	switch err {
	case sql.ErrNoRows:
		validUntil = math.MaxInt64
	case nil:
	default:
		return err
	}

	_, err = sqrl.Insert("attrs").SetMap(map[string]interface{}{
		"permanode_ref": permanode,
		"signer_ref":    signer,
		"attr":          claim.Attr,
		"value":         claim.Value,
		"valid_since":   unixDate,
		"valid_until":   validUntil,
	}).RunWith(tx).Exec()
	return err
}

func handleDelAttribute(tx *sql.Tx, claim *camtypes.Claim) error {
	unixDate := claim.Date.Unix()
	q := sqrl.Update("attrs").
		Set("valid_until", unixDate).
		Where(sqrl.And{
			sqrl.Lt{"valid_since": unixDate},
			sqrl.Gt{"valid_until": unixDate},
			sqrl.Eq{"attr": claim.Attr},
			sqrl.Eq{"permanode_ref": claim.Permanode.String()},
			sqrl.Eq{"signer_ref": claim.Signer.String()},
		})

	if claim.Value != "" {
		q = q.Where(sqrl.Eq{"value": claim.Value})
	}
	_, err := q.RunWith(tx).Exec()
	return err
}

func saveClaim(tx *sql.Tx, claim *camtypes.Claim) error {
	switch claim.Type {
	case "delete":
		_, err := sqrl.Insert("deleted").Options("OR IGNORE").SetMap(map[string]interface{}{
			"permanode_ref": claim.Target.String(),
			"signer_ref":    claim.Signer.String(),
			"date":          claim.Date.Unix(),
		}).RunWith(tx).Exec()
		return err
	case "add-attribute", "set-attribute", "del-attribute":
		_, err := sqrl.Insert("attr_claims").Options("OR IGNORE").SetMap(map[string]interface{}{
			"type":          claim.Type,
			"claim_ref":     claim.BlobRef.String(),
			"signer_ref":    claim.Signer.String(),
			"date":          claim.Date.Unix(),
			"attr":          claim.Attr,
			"value":         claim.Value,
			"permanode_ref": claim.Permanode.String(),
		}).RunWith(tx).Exec()
		return err
	default:
		return fmt.Errorf("Unknown claim type: %q", claim.Type)
	}
}

func attrAsOf(db *sql.DB, signer string, permanode blob.Ref, when time.Time, attr string) (string, error) {
	// TODO: mutli-value attrs.
	row := sqrl.Select("value").From("attrs").Where(sqrl.And{
		sqrl.LtOrEq{"valid_since": when},
		sqrl.Gt{"valid_until": when},
		sqrl.Eq{"attr": attr},
		sqrl.Eq{"permanode_ref": permanode.String()},
		sqrl.Eq{"signer": signer},
	}).Limit(1).RunWith(db).QueryRow()
	ret := ""
	err := row.Scan(&ret)
	return ret, err
}
