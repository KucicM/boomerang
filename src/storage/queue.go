package storage

import (
	"database/sql"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/prometheus/client_golang/prometheus"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var queueHistogram = prometheus.NewHistogramVec(
    prometheus.HistogramOpts{Name: "db_queue_ops", Help: "Operations on db queue"},
    []string{"op", "success"},
)
 var queueCounter = prometheus.NewCounterVec(
     prometheus.CounterOpts{Name: "db_queue", Help: "Counts of operations on db queue"},
     []string{"op", "success"},
 )

 func init() {
     prometheus.MustRegister(
         queueHistogram,
         queueCounter,
     )
 }

type PersistentQueueCfg struct {
    DbURL string
}

type persistentQueue struct {
    db *sql.DB
    lock *sync.Mutex
}

func newQueue(cfg PersistentQueueCfg) (*persistentQueue, error) {
    log.Println("connecting to queue database")

    db, err := sql.Open("sqlite3", cfg.DbURL)
    if err != nil {
        log.Printf("Cannot open queue database %s %v\n", cfg.DbURL, err)
        return nil, err
    }
    log.Println("connected to queue database")

    driver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
    if err != nil {
        log.Printf("Error getting a driver %v\n", err)
        return nil, err
    }

    m, err := migrate.NewWithDatabaseInstance("file://resources/sql/queue", "sqlite3", driver)
    if err != nil {
        log.Printf("Error getting a migration %v\n", err)
        return nil, err
    }

    log.Println("Running queue db migration script")
    if err := m.Up(); err != nil && err.Error() != "no change" {
        log.Printf("Error running migration %v\n", err)
        return nil, err
    }
    
    return &persistentQueue{db: db, lock: &sync.Mutex{}}, nil
}

func (q *persistentQueue) save(items []queueItem) error {
    if len(items) == 0 {
        return nil
    }

    var success = false
    defer func(start time.Time) {
        queueHistogram.WithLabelValues("save", strconv.FormatBool(success)).Observe(float64(time.Since(start)))
        queueCounter.WithLabelValues("save", strconv.FormatBool(success)).Add(float64(len(items)))
    }(time.Now())

    q.lock.Lock()
    defer q.lock.Unlock()

    tx, err := q.db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()

    query := "REPLACE INTO PriorityQueue (id, endpoint, sendafter, leftattempts, backoffms, status) VALUES (?, ?, ?, ?, ?, ?)"
    stmt, err := tx.Prepare(query)
    if err != nil {
        return err
    }
    defer stmt.Close()

    for _, item := range items {
        _, err := stmt.Exec(
            item.id, 
            item.endpoint, 
            item.sendAfter, 
            item.leftAttempts, 
            item.backOffMs,
            item.statusId,
        )
        if err != nil {
            return err
        }
    }
    if err = tx.Commit(); err != nil {
        return err
    }
    success = true
    return nil
}

func (q *persistentQueue) load(maxSize int) ([]queueItem, error) {
    var success = false
    var size = 0
    defer func(start time.Time) {
        queueHistogram.WithLabelValues("load", strconv.FormatBool(success)).Observe(float64(time.Since(start)))
        queueCounter.WithLabelValues("load", strconv.FormatBool(success)).Add(float64(size))
    }(time.Now())

    q.lock.Lock()
    defer q.lock.Unlock()

    currentTimeMs := time.Now().UnixMilli()
    rows, err := q.db.Query(`
    WITH cte AS (
        SELECT * 
        FROM PriorityQueue
        WHERE sendAfter < ?
            AND (status = 0 OR status = 2)
        LIMIT ?
    )
    UPDATE PriorityQueue
    SET status = 1
    WHERE Id IN (SELECT id FROM cte)
    RETURNING id, endpoint, sendafter, leftattempts, backoffms, status;`,
        currentTimeMs,
        maxSize,
    )
    if err != nil && err != sql.ErrNoRows {
        return nil, err
    }

    var items []queueItem
    for rows.Next() {
        var item queueItem
        err := rows.Scan(
            &item.id, 
            &item.endpoint, 
            &item.sendAfter, 
            &item.leftAttempts, 
            &item.backOffMs,
            &item.statusId,
        )
        if err != nil {
            return nil, err
        }
        items = append(items, item)
    }

    if err = rows.Err(); err != nil {
        return nil, err
    }

    size = len(items)
    success = true
    return items, nil
}

func (q *persistentQueue) delete(ids []string) error {
    if len(ids) == 0 {
        return nil
    }

    var success = false
    defer func(start time.Time) {
        queueHistogram.WithLabelValues("load", strconv.FormatBool(success)).Observe(float64(time.Since(start)))
        queueCounter.WithLabelValues("load", strconv.FormatBool(success)).Add(float64(len(ids)))
    }(time.Now())

    q.lock.Lock()
    defer q.lock.Unlock()

    tx, err := q.db.Begin()
    if err != nil {
        log.Printf("failed to begin queue delete transaction %s\n", err)
        return err
    }
    defer tx.Rollback()

    stmt, err := tx.Prepare("DELETE FROM PriorityQueue WHERE Id = ?;")
    if err != nil {
        log.Printf("failed to create queue delete statement %s\n", err)
        return err
    }
    defer stmt.Close()

    for _, id := range ids {
        if _, err = stmt.Exec(id); err != nil {
            log.Printf("error executing queue delete %s\n", err)
            return err
        }
    }

    if err = tx.Commit(); err != nil {
        log.Printf("error commiting queue delete %s\n", err)
        return err
    }

    return nil
}

func (q *persistentQueue) update(items []queueItem) error {
    if len(items) == 0 {
        return nil 
    }

    // TODO fix double counting save metrics
    var success = false
    defer func(start time.Time) {
        queueHistogram.WithLabelValues("update", strconv.FormatBool(success)).Observe(float64(time.Since(start)))
        queueCounter.WithLabelValues("update", strconv.FormatBool(success)).Add(float64(len(items)))
    }(time.Now())

    if err := q.save(items); err != nil {
        return err
    }
    success = true
    return nil
}

func (q *persistentQueue) Shutdown() error {
    log.Println("queue shutdown")
    q.lock.Lock()
    return q.db.Close()
}
