package index_repo

import (
	"database/sql"

	_ "github.com/lib/pq"
)

func NewPostgresIndex(db *sql.DB) *PostgresIndex {
	return &PostgresIndex{db: db}
}

type PostgresIndex struct {
	db *sql.DB
}

func (i *PostgresIndex) Store(obj Object) error {
	_, err := i.db.Exec(
		`

		INSERT INTO objects (
			path,
			name,
			extension,
			contentType,
			hashSumSHA512,
			modifiedTimestamp,
			createdTimestamp
		) VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7
		)
		ON CONFLICT (path) DO UPDATE SET
			name = EXCLUDED.name,
			extension = EXCLUDED.extension,
			contentType = EXCLUDED.contentType,
			hashSumSHA512 = EXCLUDED.hashSumSHA512,
			modifiedTimestamp = EXCLUDED.modifiedTimestamp,
			createdTimestamp = EXCLUDED.createdTimestamp;
		`,
		obj.Path,
		obj.Name,
		obj.Extension,
		obj.ContentType,
		obj.HashSumSHA512,
		obj.ModifiedTimestamp,
		obj.CreatedTimestamp,
	)
	if err != nil {
		return err
	}

	return nil
}
