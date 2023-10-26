package storage

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/kucicm/boomerang/src/server"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func StartTestDatabase() error {
    ctx := context.Background()
    
    dbName := "test-name"
    dbUser := "test-user"
    dbPassword := "test-password"


    cont, err := postgres.RunContainer(ctx,
        testcontainers.WithImage("docker.io/postgres:16.0-alpine"),
        postgres.WithDatabase(dbName),
        postgres.WithUsername(dbUser),
        postgres.WithPassword(dbPassword),
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
    return err
}

func TestSave(t *testing.T) {
    if err := StartTestDatabase(); err != nil {
        t.Error(err)
    }

    storage, err := NewStorageService(StorageServiceCfg{migrationPath: "file://../../resources/sql"})
    if err != nil {
        t.Error(err)
    }

    err = storage.Save(server.ScheduleRequest{})
    if err != nil {
        t.Error(err)
    }

}
