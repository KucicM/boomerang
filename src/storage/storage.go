package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx"
	"github.com/jackc/pgx/v5/pgxpool"
	srv "github.com/kucicm/boomerang/src/server"
)

type StorageServiceCfg struct {
    saveQueueSize int
    saveBatchSize int
    maxWaitMs int
    migrationPath string
}

type StorageService struct {
    dbClient *pgxpool.Pool
}

func NewStorageService(cfg StorageServiceCfg) (*StorageService, error) {
    ctx := context.Background()
    dbpool, err := pgxpool.New(context.Background(), os.Getenv("DB_URL"))
    if err != nil {
        return nil, fmt.Errorf("cannot open database %v", err)
    }

    if err = dbpool.Ping(ctx); err != nil {
        return nil, fmt.Errorf("cannot ping database %v", err)
    }

    log.Println("Connected to dabase")

    if err := runDatabaseMigration(cfg.migrationPath); err != nil {
        return nil, err
    }

    s := &StorageService{
        dbClient: dbpool,
    }
    return s, nil
}

func (s *StorageService) Save(r srv.ScheduleRequest) error {
    query := `INSERT INTO schedule.primary_queue
        (endpoint, headers, payload, send_after, max_retry, back_off_ms, time_to_live)
        VALUES ($1, $2, $3, $4, $5, $6, $7)`

    headers, err := json.Marshal(r.Headers)
    if err != nil {
        return fmt.Errorf("failed to convert headers to string %s", err)
    }

    _, err = s.dbClient.Exec(context.Background(), query, 
        r.Endpoint, headers, r.Payload, r.SendAfter, r.MaxRetry, r.BackOffMs, r.TimeToLive)
    if err != nil {
        log.Printf("Error saving to primary queue %s\n", err)
        return err
    }
    return nil
}

func (s *StorageService) Load(bs uint) []srv.ScheduleRequest {
    // todo fair queue
    query := `
    WITH ready AS (
        SELECT *
        FROM schedule.primary_queue
        WHERE
            STATUS = 0
            AND send_after <= EXTRACT(epoch FROM CURRENT_TIMESTAMP) * 1000
            AND time_to_live >= EXTRACT(epoch FROM CURRENT_TIMESTAMP) * 1000
        LIMIT $1
    )
    UPDATE schedule.primary_queue
    SET status = 1
    FROM ready
    WHERE schedule.primary_queue.id = ready.id
    RETURNING ready.id
            , ready.endpoint
            , ready.headers
            , ready.payload
            , ready.send_after
            , ready.max_retry
            , ready.back_off_ms
            , ready.time_to_live;
    `

    rows, err := s.dbClient.Query(context.Background(), query, bs)
    if err != nil {
        log.Printf("Error loading schedule requests from dababase %s\n", err)
        return []srv.ScheduleRequest{}
    }

    out := make([]srv.ScheduleRequest, 0, bs)
    for rows.Next() {
        var it srv.ScheduleRequest
        var headers string
        err := rows.Scan(&it.Id, &it.Endpoint, &headers, &it.Payload, &it.SendAfter, &it.MaxRetry, &it.BackOffMs, &it.TimeToLive)
        if err != nil {
            log.Printf("Error converting database row to struct %s\n", err)
            continue
        }
        if err = json.Unmarshal([]byte(headers), &it.Headers); err != nil {
            log.Printf("Error converting database headers to struct %s\n", err)
            continue
        }
        out = append(out, it)
    }
    return out
}

func (s *StorageService) Update(task srv.ScheduleRequest) {
    query := `UPDATE schedule.primary_queue
        SET 
            send_after = $2
            , max_retry = $3
        WHERE Id = $1
    `
    tag, err := s.dbClient.Exec(context.Background(), query, task.Id, task.SendAfter, task.MaxRetry)
    if err != nil {
        log.Printf("error on update of task with id %d, err: %s\n", task.Id, err)
        return
    }

    if tag.RowsAffected() != 1 {
        log.Printf("update of id %d caused %d updates\n", task.Id, tag.RowsAffected())
    }
}

func (s *StorageService) Delete(task srv.ScheduleRequest) {
    query := `DELETE FROM schedule.primary_queue WHERE id = $1;`
    if _, err := s.dbClient.Exec(context.Background(), query, task.Id); err != nil {
        log.Printf("failed to delete task with id %d error: %s\n", task.Id, err)
    }
}

func (s *StorageService) Shutdown() error {
    s.dbClient.Close()
    return nil
}

func runDatabaseMigration(migrationPath string) error {
    log.Println("running migration")
    db, err := sql.Open("pgx", os.Getenv("DB_URL"))
    if err != nil {
        return fmt.Errorf("error openning connection to dababase %s", err)
    }
    defer db.Close()

    driver, err := pgx.WithInstance(db, &pgx.Config{})
    if err != nil {
        return fmt.Errorf("error getting a driver %v", err)
    }

    m, err := migrate.NewWithDatabaseInstance(migrationPath, "pgx", driver)
    if err != nil {
        return fmt.Errorf("error getting a migration %v", err)
    }

    if err := m.Up(); err != nil && err.Error() != "no change" {
        return fmt.Errorf("error running migration %v", err)
    }
    log.Println("migration complited")
    return nil
}
