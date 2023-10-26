package main

type QueueItem struct {

}

type DispatcherQueue struct {
    stats *StatsTracker

}

// save single item into database
func (q *DispatcherQueue) Add(item *QueueItem) {

}

// load batch of items from database
func (q *DispatcherQueue) Poll(batchSize int) []*QueueItem {
    return nil
}
