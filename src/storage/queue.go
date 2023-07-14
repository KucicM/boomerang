package storage

import (
	"database/sql"
	"fmt"
	"log"
)

type QueueRequest struct {
    Endpoint string
    Payload string
    SendAfter uint64
}

type QueueResponse struct {

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
        log.Printf("Cannot open queue database %v\n", err)
        return nil, err
    }

    log.Println("connected to queue database")

    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS PriorityQueue (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        endpoint TEXT,
        payload TEXT,
        sendAfter BIGINT
    );`)
    if err != nil {
        return nil, fmt.Errorf("error creating table %v\n", err)
    }

    return &PersistentQueue{db: db}, nil
}

func (q *PersistentQueue) Close() {
    q.db.Close()
}

func (q *PersistentQueue) Push(req QueueRequest) (QueueResponse, error) {
    stmt, err := q.db.Prepare("INSERT INTO PriorityQueue (endpoint, payload, sendAfter) VALUES (?, ?, ?)")
    if err != nil {
        return QueueResponse{}, err
    }
    defer stmt.Close()

    if _, err = stmt.Exec(req.Endpoint, req.Payload, req.SendAfter); err != nil {
        return QueueResponse{}, err
    }

    return QueueResponse{}, nil
}

func (q *PersistentQueue) Pop() ([]QueueRequest, error) {
    rows, err := q.db.Query("SELECT endpoint, payload, sendAfter FROM PriorityQueue")
    if err != nil {
        return nil, err
    }

    var reqs []QueueRequest
    for rows.Next() {
        var req QueueRequest
        if rows.Scan(&req.Endpoint, &req.Payload, &req.SendAfter); err != nil {
            return nil, err
        }
        reqs = append(reqs, req)
    }

    if err = rows.Err(); err != nil {
        return nil, err
    }


    return reqs, nil
}

