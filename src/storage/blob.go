package storage

import (
	"database/sql"
	"log"
	"strings"
	"sync"

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
    lock *sync.Mutex
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

    return &BlobStorage{db: db, lock: &sync.Mutex{}}, nil
}

func (s *BlobStorage) BulkSave(reqs []BlobStorageRequest) error {
    if len(reqs) == 0 {
        return nil 
    }

    s.lock.Lock()
    defer s.lock.Unlock()

    tx, err := s.db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()

    stmt, err := tx.Prepare("INSERT INTO Blobs (id, payload) VALUES (?, ?)")
    if err != nil {
        return err
    }
    defer stmt.Close()

    for _, req := range reqs {
        if _, err = stmt.Exec(req.Id, req.Payload); err != nil {
            return err
        }
    }
    return tx.Commit()
}

func (s *BlobStorage) Load(ids []string) ([]string, error) {
    if len(ids) == 0 {
        return []string{}, nil
    }
    s.lock.Lock()
    defer s.lock.Unlock()

    query := "SELECT payload FROM Blobs WHERE id in (?" + strings.Repeat(",?", len(ids)-1) +");"
    args := make([]interface{}, len(ids))
    for i := 0; i < len(ids); i++ {
        args[i] = ids[i]
    }
    row, err := s.db.Query(query, args...)
    if err != nil {
        return nil, err
    }

    var ret []string
    for row.Next() {
        var r string
        err = row.Scan(&r)
        if err != nil {
            return nil, err
        }
        ret = append(ret, r)
    }


    return ret, nil
}

func (s *BlobStorage) DeleteMany(ids []string) error {
    if len(ids) == 0 {
        return nil
    }

    s.lock.Lock()
    defer s.lock.Unlock()

    tx, err := s.db.Begin()
    if err != nil {
        log.Printf("failed to begin blobs delete transaction %s\n", err)
        return err
    }
    defer tx.Rollback() 

    stmt, err := tx.Prepare("DELETE FROM Blobs WHERE id = ?;")
    if err != nil {
        log.Printf("failed to create blobs delete statement %s\n", err)
        return err
    }
    defer stmt.Close()

    for _, id := range ids {
        if _, err := stmt.Exec(id); err != nil {
            log.Printf("error executing blobs delete %s\n", err)
            return err
        }
    }

    if err = tx.Commit(); err != nil {
        log.Printf("error commiting blobs delete %s\n", err)
        return err
    }

    return nil
}
