package storage

import (
	"database/sql"
	"fmt"
	"log"
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

    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS Blobs (
        id TEXT PRIMARY KEY, 
        payload TEXT
    );`)
    if err != nil {
        return nil, fmt.Errorf("error creating table %v\n", err)
    }

    return &BlobStorage{db: db}, nil
}

func (s *BlobStorage) Save(req BlobStorageRequest) error {
    log.Printf("%+v\n", req)
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
    log.Printf("load %+v\n", id)
    row := s.db.QueryRow("SELECT payload FROM Blobs WHERE id = ?", id)

    var ret string
    err := row.Scan(&ret)
    return ret, err
}
