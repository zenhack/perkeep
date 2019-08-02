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
	_, err := tx.Exec(`
		INSERT INTO deleted
		(permandode_ref, signer_ref, date)
		VALUES(?, ?, ?)
		`,
		claim.Target.String(),
		claim.Signer.String(),
		claim.Date.Unix(),
	)
	return err
}

func handleAddAttribute(tx *sql.Tx, claim *camtypes.Claim) error {
	permanode := claim.Permanode.String()
	signer := claim.Signer.String()
	unixDate := claim.Date.Unix()

	row := tx.QueryRow(`
		SELECT date
		FROM attr_claims
		WHERE
			attr = ?
			AND date > ?
			AND (
				type = 'set-attribute'
				OR (
					type = 'del-attribute'
					AND (value = ? || value = '')
				)
			)
			AND permanode_ref = ?
			AND signer_ref = ?
		`,
		unixDate,
		claim.Attr,
		claim.Value,
		permanode,
		signer,
	)
	var validUntil int64
	err := row.Scan(&validUntil)
	switch err {
	case sql.ErrNoRows:
		validUntil = math.MaxInt64
	case nil:
	default:
		return err
	}

	_, err = tx.Exec(`
		INSERT INTO attrs
		(permanode_ref, signer_ref, attr, value, valid_since, valid_until)
		VALUES (?, ?, ?, ?, ?, ?)`,
		permanode,
		signer,
		claim.Attr,
		claim.Value,
		unixDate,
		validUntil,
	)
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
		_, err := tx.Exec(`
			INSERT OR IGNORE INTO deleted
			(permanode_ref, signer_ref, date)
			VALUES (?, ?, ?)
			`,
			claim.Target.String(),
			claim.Signer.String(),
			claim.Date.Unix(),
		)
		return err
	case "add-attribute", "set-attribute", "del-attribute":
		_, err := tx.Exec(`
			INSERT OR IGNORE INTO attr_claims
			(type, claim_ref, signer_ref, date, attr, value, permanode_ref)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			`,
			claim.Type,
			claim.BlobRef.String(),
			claim.Signer.String(),
			claim.Date.Unix(),
			claim.Attr,
			claim.Value,
			claim.Permanode.String(),
		)
		return err
	default:
		return fmt.Errorf("Unknown claim type: %q", claim.Type)
	}
}

func attrAsOf(db *sql.DB, signer string, permanode blob.Ref, when time.Time, attr string) (string, error) {
	// TODO: mutli-value attrs.
	row := db.QueryRow(`
		SELECT value
		FROM attrs
		WHERE
			valid_since <= ?
			&& valid_until > ?
			&& attr = ?
			&& permanode_ref = ?
			&& signer = ?
		LIMIT 1
		`,
		when,
		when,
		attr,
		permanode.String(),
		signer,
	)
	ret := ""
	err := row.Scan(&ret)
	return ret, err
}
