package storage

import (
    "time"
    srv "github.com/kucicm/boomerang/src/server"
)

type StorageServiceCfg struct {
    saveQueueSize int
    saveBatchSize int
    maxWaitMs int
}

type StorageService struct {
    bulkSave *BulkProcessor[srv.ScheduleRequest]
}

func NewStorageService(cfg StorageServiceCfg) *StorageService {
    s := &StorageService{}
    s.bulkSave = NewBulkProcessor[srv.ScheduleRequest](cfg.saveQueueSize, cfg.saveBatchSize, time.Millisecond, s.save)
    return s
}

func (s *StorageService) Save(initReq srv.ScheduleRequest) error {
    return s.bulkSave.Add(initReq)
}

func (s *StorageService) save(reqs []srv.ScheduleRequest) error {
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
    return s.bulkSave.Shutdown()
}
