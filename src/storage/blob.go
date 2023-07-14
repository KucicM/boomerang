package storage

import (
	"database/sql"
	"log"

    "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"

    _ "github.com/golang-migrate/migrate/v4/source/file"
)

type BlobStorageRequest struct {
    Id string
    Payload string
}

type BlobStorageCfg struct {
    DbURL string
}

type BlobStorage struct {
    db *sql.DB
}

func NewBlobStorage(cfg BlobStorageCfg) (*BlobStorage, error) {
    log.Println("connecting to blob database")
    db, err := sql.Open("sqlite3", cfg.DbURL)
    if err != nil {
        log.Printf("Cannot open queue database %v\n", err)
        return nil, err
    }

    log.Println("connected to queue database")


    driver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
    if err != nil {
        log.Printf("Error getting a driver %v\n", err)
        return nil, err
    }

    m, err := migrate.NewWithDatabaseInstance("file://resources/sql/blobs", "sqlite3", driver)
    if err != nil {
        log.Printf("Error getting a migration %v\n", err)
        return nil, err
    }

    log.Println("Running blobs db migration script")
    if err := m.Up(); err != nil && err.Error() != "no change" {
        log.Printf("Error running migration %v\n", err)
        return nil, err
    }

    return &BlobStorage{db: db}, nil
}

func (s *BlobStorage) Save(req BlobStorageRequest) error {
    stmt, err := s.db.Prepare("INSERT INTO Blobs (id, payload) VALUES (?, ?)")
    if err != nil {
        return err
    }
    defer stmt.Close()

    if _, err = stmt.Exec(req.Id, req.Payload); err != nil {
        return err
    }

    return nil
}

func (s *BlobStorage) Load(id string) (string, error) {
    row := s.db.QueryRow("SELECT payload FROM Blobs WHERE id = ?", id)

    var ret string
    err := row.Scan(&ret)
    return ret, err
}
