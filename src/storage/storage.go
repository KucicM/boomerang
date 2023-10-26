package storage

import "time"

type Storage interface {
    Save(ScheduledTask)
    Load(bs uint) []ScheduledTask
    Update(ScheduledTask)
    Delete(ScheduledTask)
    Shutdown()
}

// this should not be here?
type ScheduledTask struct {

}

type StorageServiceCfg struct {
    saveQueueSize int
    saveBatchSize int
    maxWaitMs int
}

type StorageService struct {
    bulkSave *BulkProcessor[ScheduledTask]
}

func NewStorageService(cfg StorageServiceCfg) *StorageService {
    s := &StorageService{}
    s.bulkSave = NewBulkProcessor[ScheduledTask](cfg.saveQueueSize, cfg.saveBatchSize, time.Millisecond, s.save)
    return s
}

func (s *StorageService) Save(task ScheduledTask) error {
    return s.bulkSave.Add(task)
}

func (s *StorageService) save(task []ScheduledTask) error {
    return nil
}

func (s *StorageService) Load(bs uint) []ScheduledTask {
    return nil
}

func (s *StorageService) Update(task ScheduledTask) {
    // find task with id and update it
}

func (s *StorageService) Delete(task ScheduledTask) {
}

func (s *StorageService) Shutdown() error {
    return s.bulkSave.Shutdown()
}
