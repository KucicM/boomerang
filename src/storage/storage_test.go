package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/kucicm/boomerang/src/server"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func init() {
    if err := StartTestDatabase(); err != nil {
        panic(err)
    }
}

func StartTestDatabase() error {
    ctx := context.Background()
    cont, err := postgres.RunContainer(ctx,
        testcontainers.WithImage("docker.io/postgres:16.0-alpine"),
        postgres.WithDatabase("dbName"),
        postgres.WithUsername("user"),
        postgres.WithPassword("pass"),
        testcontainers.WithWaitStrategy(
            wait.ForLog("database system is ready to accept connections").
            WithOccurrence(2).
            WithStartupTimeout(5*time.Second)),
    )
    if err != nil {
        return err
    }

    log.Println("container started")
    url, _ := cont.ConnectionString(ctx, "sslmode=disable")
    log.Println(url)
    os.Setenv("DB_URL", url)
    return nil
}

var onceStartDb sync.Once
var testDb *sql.DB
var testDbErr error
func GetTestDatabase() (*sql.DB, error) {
    onceStartDb.Do(func() {
        testDb, testDbErr = sql.Open("pgx", os.Getenv("DB_URL"))
    })
    return testDb, testDbErr
}

func TruncateTables() error {
    db, err := GetTestDatabase()
    if err != nil {
        return err
    }
    query := `DO $$ 
    BEGIN 
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'primary_queue' AND table_schema = 'schedule') THEN
            EXECUTE 'TRUNCATE TABLE schedule.primary_queue';
        END IF;
    END $$;`
    if _, err = db.Exec(query); err != nil {
        return err
    }
    return nil
}

func TestSave(t *testing.T) {
    if err := TruncateTables(); err != nil {
        t.Error(err)
    }

    storage, err := NewStorageService(StorageServiceCfg{migrationPath: "file://../../resources/sql"})
    if err != nil {
        t.Error(err)
    }

    req := server.ScheduleRequest{
    	Endpoint:   "Test",
        Headers:    map[string]string{"Ha": "Ha", "He": "He"},
    	Payload:    "slkdjf",
    	SendAfter:  328389,
    	MaxRetry:   23,
    	BackOffMs:  12,
    	TimeToLive: 91909129,
    }

    err = storage.Save(req)
    if err != nil {
        t.Error(err)
    }

    db, err := GetTestDatabase()
    if err != nil {
        t.Error(err)
    }

    var headers string
    var got server.ScheduleRequest
    var status int
    err = db.QueryRow("SELECT * FROM schedule.primary_queue LIMIT 1;").Scan(
        &got.Id,
        &got.Endpoint,
        &headers,
        &got.Payload,
        &got.SendAfter,
        &got.MaxRetry,
        &got.BackOffMs,
        &got.TimeToLive,
        &status,
    )
    if err != nil {
        t.Error(err)
    }

    if got.Id == 0 {
        t.Error("id was not added")
    }

    if err := json.Unmarshal([]byte(headers), &got.Headers); err != nil {
        t.Errorf("got unmarshable headers form db %v", err)
    }

    got.Id = 0
    if !reflect.DeepEqual(got, req) {
        t.Errorf("expected %+v got %+v", req, got)
    }

    if status != 0 {
        t.Errorf("expected status 0 got %d", status)
    }
}

func TestSaveLoad(t *testing.T) {
    if err := TruncateTables(); err != nil {
        t.Error(err)
    }

    storage, err := NewStorageService(StorageServiceCfg{migrationPath: "file://../../resources/sql"})
    if err != nil {
        t.Error(err)
    }

    req := server.ScheduleRequest{
    	Endpoint:   "Test",
        Headers:    map[string]string{"Ha": "Ha", "He": "He"},
    	Payload:    "slkdjf",
    	SendAfter:  328389,
    	MaxRetry:   23,
    	BackOffMs:  12,
    	TimeToLive: uint64(time.Now().UnixMilli()) + 5_000,
    }

    if err = storage.Save(req); err != nil {
        t.Error(err)
    }

    loaded := storage.Load(10)
    if len(loaded) != 1 {
        t.Errorf("expected loaded size of 1 got %d", len(loaded))
    }

    it := loaded[0]
    if it.Id == 0 {
        t.Errorf("Id was not set")
    }
    it.Id = 0

    if !reflect.DeepEqual(it, req) {
        t.Errorf("expected %+v got %+v", req, it)
    }
}

func TestLoadNItems(t *testing.T) {
    if err := TruncateTables(); err != nil {
        t.Error(err)
    }

    storage, err := NewStorageService(StorageServiceCfg{migrationPath: "file://../../resources/sql"})
    if err != nil {
        t.Error(err)
    }

    for i := 0; i < 20; i++ {
        req := server.ScheduleRequest{
            Endpoint:   "Test",
            Headers:    map[string]string{"Ha": "Ha", "He": "He"},
            Payload:    "slkdjf",
            SendAfter:  328389,
            MaxRetry:   23,
            BackOffMs:  12,
            TimeToLive: uint64(time.Now().UnixMilli()) + 5_000,
        }

        if err = storage.Save(req); err != nil {
            t.Error(err)
        }
    }

    loaded := storage.Load(10)
    if len(loaded) != 10 {
        t.Errorf("expected loaded size of 10 got %d", len(loaded))
    }
}

func TestDeleteTask(t *testing.T) {
    if err := TruncateTables(); err != nil {
        t.Error(err)
    }

    db, err := GetTestDatabase()
    if err != nil {
        t.Error(err)
    }

    storage, err := NewStorageService(StorageServiceCfg{migrationPath: "file://../../resources/sql"})
    if err != nil {
        t.Error(err)
    }

    req := server.ScheduleRequest{
        Endpoint:   "Test",
        Headers:    map[string]string{"Ha": "Ha", "He": "He"},
        Payload:    "slkdjf",
        SendAfter:  328389,
        MaxRetry:   23,
        BackOffMs:  12,
        TimeToLive: uint64(time.Now().UnixMilli()) + 5_000,
    }

    if err = storage.Save(req); err != nil {
        t.Error(err)
    }

    loaded := storage.Load(10)
    if len(loaded) != 1 {
        t.Errorf("expected loaded size of 1 got %d", len(loaded))
    }

    it := loaded[0]
    storage.Delete(it)


    var count int
    err = db.QueryRow("SELECT COUNT(1) FROM schedule.primary_queue WHERE id = $1;", it.Id).Scan(&count)
    if err != nil {
        t.Error(err)
    }

    if count != 0 {
        t.Errorf("expected no rows to be found with id %d\n", it.Id)
    }
}

func TestUpdate(t *testing.T) {
    if err := TruncateTables(); err != nil {
        t.Error(err)
    }
    db, err := GetTestDatabase()
    if err != nil {
        t.Error(err)
    }

    storage, err := NewStorageService(StorageServiceCfg{migrationPath: "file://../../resources/sql"})
    if err != nil {
        t.Error(err)
    }

    req := server.ScheduleRequest{
        Endpoint:   "Test",
        Headers:    map[string]string{"Ha": "Ha", "He": "He"},
        Payload:    "slkdjf",
        SendAfter:  328389,
        MaxRetry:   23,
        BackOffMs:  12,
        TimeToLive: uint64(time.Now().UnixMilli()) + 5_000,
    }

    if err = storage.Save(req); err != nil {
        t.Error(err)
    }

    it := storage.Load(1)[0]
    it.MaxRetry = 1

    storage.Update(it)


    var retry int
    err = db.QueryRow("SELECT max_retry FROM schedule.primary_queue WHERE id = $1;", it.Id).Scan(&retry)
    if err != nil {
        t.Error(err)
    }

    if retry != 1 {
        t.Errorf("expected max_retry to be 1 got %d\n", retry)
    }

}
