package index_repo

import (
	"database/sql"
	"errors"

	_ "github.com/lib/pq"
)

func NewPostgresIndex(db *sql.DB) *PostgresIndex {
	return &PostgresIndex{db: db}
}

type PostgresIndex struct {
	db *sql.DB
}

func (i *PostgresIndex) GetByPath(path string) (*Object, error) {
	var obj Object

	err := i.db.QueryRow(
		"SELECT path, name, extension, contentType, hashSumSHA512, modifiedTimestamp, createdTimestamp FROM objects WHERE path = $1",
		path,
	).Scan(
		&obj.Path,
		&obj.Name,
		&obj.SizeBytes,
		&obj.Extension,
		&obj.ContentType,
		&obj.HashSumSHA512,
		&obj.ModifiedTimestamp,
		&obj.CreatedTimestamp,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return &obj, nil
}

func (i *PostgresIndex) Store(obj Object) error {
	_, err := i.db.Exec(
		`

		INSERT INTO objects (
			path,
			name,
			sizeBytes,
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
			$7,
			$8
		)
		ON CONFLICT (path) DO UPDATE SET
			name = EXCLUDED.name,
			sizeBytes = EXCLUDED.sizeBytes,
			extension = EXCLUDED.extension,
			contentType = EXCLUDED.contentType,
			hashSumSHA512 = EXCLUDED.hashSumSHA512,
			modifiedTimestamp = EXCLUDED.modifiedTimestamp,
			createdTimestamp = EXCLUDED.createdTimestamp;
		`,
		obj.Path,
		obj.Name,
		obj.SizeBytes,
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
