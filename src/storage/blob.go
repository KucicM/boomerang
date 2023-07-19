package storage

import (
	"database/sql"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/prometheus/client_golang/prometheus"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var blobHistogram = prometheus.NewHistogramVec(
    prometheus.HistogramOpts{Name: "db_blob_ops", Help: "Operations on db bobs"},
    []string{"op", "success"},
)
 var blobCounter = prometheus.NewCounterVec(
     prometheus.CounterOpts{Name: "db_blob", Help: "Counts of operations on db blob"},
     []string{"op", "success"},
 )

 func init() {
     prometheus.MustRegister(
         blobHistogram,
         blobCounter,
     )
 }

type BlobStorageCfg struct {
    DbURL string
}

type blobStorage struct {
    db *sql.DB
    lock *sync.Mutex
}

func newBlobStorage(cfg BlobStorageCfg) (*blobStorage, error) {
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

    return &blobStorage{db: db, lock: &sync.Mutex{}}, nil
}

func (s *blobStorage) save(items []blobItem) error {
    if len(items) == 0 {
        return nil 
    }

    var success = false
    defer func(start time.Time) {
        blobHistogram.WithLabelValues("save", strconv.FormatBool(success)).Observe(float64(time.Since(start)))
        blobCounter.WithLabelValues("save", strconv.FormatBool(success)).Add(float64(len(items)))
    }(time.Now())

    s.lock.Lock()
    defer s.lock.Unlock()

    tx, err := s.db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()

    stmt, err := tx.Prepare("INSERT INTO Blobs (id, payload, headers) VALUES (?, ?, ?)")
    if err != nil {
        return err
    }
    defer stmt.Close()

    for _, item := range items {
        if _, err = stmt.Exec(item.id, item.payload, item.headers); err != nil {
            return err
        }
    }
    if err := tx.Commit(); err != nil {
        return err
    }
    success = true
    return nil
}

func (s *blobStorage) load(ids []string) ([]blobItem, error) {
    if len(ids) == 0 {
        return []blobItem{}, nil
    }

    var success = false
    defer func(start time.Time) {
        blobHistogram.WithLabelValues("load", strconv.FormatBool(success)).Observe(float64(time.Since(start)))
        blobCounter.WithLabelValues("load", strconv.FormatBool(success)).Add(float64(len(ids)))
    }(time.Now())

    s.lock.Lock()
    defer s.lock.Unlock()

    query := "SELECT id, payload, headers FROM Blobs WHERE id in (?" + strings.Repeat(",?", len(ids)-1) +");"
    args := make([]interface{}, len(ids))
    for i := 0; i < len(ids); i++ {
        args[i] = ids[i]
    }
    row, err := s.db.Query(query, args...)
    if err != nil {
        return nil, err
    }

    var ret []blobItem
    for row.Next() {
        var r blobItem
        err = row.Scan(&r.id, &r.payload, &r.headers)
        if err != nil {
            return nil, err
        }
        ret = append(ret, r)
    }
    success = true
    return ret, nil
}

func (s *blobStorage) delete(ids []string) error {
    if len(ids) == 0 {
        return nil
    }

    var success = false
    defer func(start time.Time) {
        blobHistogram.WithLabelValues("delete", strconv.FormatBool(success)).Observe(float64(time.Since(start)))
        blobCounter.WithLabelValues("delete", strconv.FormatBool(success)).Add(float64(len(ids)))
    }(time.Now())

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

    success = true

    return nil
}

func (b *blobStorage) Shutdown() error {
    log.Println("blob shutdown")
    b.lock.Lock()
    return b.db.Close()
}
