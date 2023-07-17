package dispatcher

import (
	"log"
	"time"

	"github.com/kucicm/boomerang/src/storage"
)

type Retrier struct {
    store *storage.StorageManager
}

func NewRetrier(store *storage.StorageManager) *Retrier {
    return &Retrier{store}
}

func (r *Retrier) Retry(item storage.StorageItem) {
    if item.MaxRetry == 0 {
        return
    }

    item.StatusId = 2
    item.MaxRetry -= 1
    item.SendAfter = uint64(time.Now().UnixMilli()) + uint64(item.BackOffMs)

    log.Printf("retry %+v\n", item)
    r.store.Update(item)
}

