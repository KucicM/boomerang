package storage

import (
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"

	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type QueueRequest struct {
    Id string
    Endpoint string
    SendAfter uint64
}

type PersistentQueueCfg struct {
    DbURL string
}

type PersistentQueue struct {
    db *sql.DB
    lock *sync.Mutex
    fn *BulkProcessor[string]
}

func NewQueue(cfg PersistentQueueCfg) (*PersistentQueue, error) {
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
    
    q := &PersistentQueue{db: db, lock: &sync.Mutex{}}
    q.fn = NewBulkProcessor(
        1000,
        50,
        time.Microsecond,
        q.delete,
    )
    return q, nil
}

func (q *PersistentQueue) Push(req QueueRequest) error {
    q.lock.Lock()
    defer q.lock.Unlock()

    stmt, err := q.db.Prepare("INSERT INTO PriorityQueue (id, endpoint, sendAfter) VALUES (?, ?, ?)")
    if err != nil {
        return err
    }
    defer stmt.Close()

    if _, err = stmt.Exec(req.Id, req.Endpoint, req.SendAfter); err != nil {
        return err
    }

    return nil
}

func (q *PersistentQueue) Pop(maxSize int) ([]QueueRequest, error) {
    q.lock.Lock()
    defer q.lock.Unlock()

    currentTimeMs := time.Now().UnixMilli()
    rows, err := q.db.Query(`
    WITH cte AS (
        SELECT * 
        FROM PriorityQueue
        WHERE sendAfter < ?
            AND status = 0
        LIMIT ?
    )
    UPDATE PriorityQueue
    SET status = 1
    WHERE Id IN (SELECT id FROM cte)
    RETURNING id, endpoint, sendAfter;`,
        currentTimeMs,
        maxSize,
    )
    if err != nil && err != sql.ErrNoRows {
        return nil, err
    }

    var reqs []QueueRequest
    for rows.Next() {
        var req QueueRequest
        if rows.Scan(&req.Id, &req.Endpoint, &req.SendAfter); err != nil {
            return nil, err
        }
        reqs = append(reqs, req)
    }

    if err = rows.Err(); err != nil {
        return nil, err
    }

    return reqs, nil
}

func (q *PersistentQueue) Delete(id string) error {
    return q.fn.Add(id)
}

func (q *PersistentQueue) delete(ids []string) error {
    if len(ids) == 0 {
        return nil
    }

    query := "DELETE FROM PriorityQueue WHERE id in (?" + strings.Repeat(",?", len(ids)-1) +");"
    args := make([]interface{}, len(ids))
    for i := 0; i < len(ids); i++ {
        args[i] = ids[i]
    }

    q.lock.Lock()
    defer q.lock.Unlock()

    stmt, err := q.db.Prepare(query)
    if err != nil {
        return err
    }
    defer stmt.Close()

    _, err = stmt.Exec(args...)
    return err
}
