package dispatcher

import (
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

    item.Status = storage.Retry
    item.MaxRetry -= 1
    item.SendAfter = uint64(time.Now().UnixMilli()) + uint64(item.BackOffMs)

    for {
        if err := r.store.Update(item); err == nil {
            return
        }
    }
}

