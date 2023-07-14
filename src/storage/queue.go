package storage

import (
	"database/sql"
	"log"

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

    return &PersistentQueue{db: db}, nil
}

func (q *PersistentQueue) Close() {
    q.db.Close()
}

func (q *PersistentQueue) Push(req QueueRequest) error {
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

func (q *PersistentQueue) Pop() ([]QueueRequest, error) {
    rows, err := q.db.Query("SELECT id, endpoint, sendAfter FROM PriorityQueue")
    if err != nil {
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
    stmt, err := q.db.Prepare("DELETE FROM PriorityQueue WHERE id = ?")
    if err != nil {
        return err
    }
    defer stmt.Close()

    _, err = stmt.Exec(id)
    return err
}
