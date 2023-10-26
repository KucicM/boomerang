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
    return nil
}

func (s *StorageService) Update(task srv.ScheduleRequest) {
    // find task with id and update it
}

func (s *StorageService) Delete(task srv.ScheduleRequest) {
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
