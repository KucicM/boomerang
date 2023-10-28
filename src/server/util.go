package server

import "time"


func isValidForRetry(req ScheduleRequest) bool {
    if req.MaxRetry == 1 {
        return false
    }

    if time.Now().UnixMilli() >= int64(req.TimeToLive) {
        return false
    }

    if req.SendAfter + req.BackOffMs > req.TimeToLive {
        return false
    }

    return true
}
