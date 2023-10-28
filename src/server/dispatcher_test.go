package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

var testDispatcher *dispatcher
var ms *mockStorage

type mockStorage struct {
    ret []ScheduleRequest
}

func (s *mockStorage) Load(bs uint) []ScheduleRequest {
    ret := s.ret
    s.ret = []ScheduleRequest{}
    return ret
}

func (s *mockStorage) Update(ScheduleRequest) {
}

func (s *mockStorage) Delete(ScheduleRequest) {
}

func init() {
    cfg := DispatcherCfg{
    	LoadBatchSize:  10,
    	MaxConcurrency: 1,
    }

    ms = &mockStorage{}
    testDispatcher = NewDispatcher(cfg, ms)
    testDispatcher.Start()
}

func TestCallHttpWithoutHeaders(t *testing.T) {
    var recivedPayload string
    headers := make(map[string]string)
    done := make(chan struct{})
    srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
        body, err := io.ReadAll(r.Body)
        if err != nil {
            t.Error(err)
        } else {
            recivedPayload = string(body)
        }

        for k, v := range r.Header {
            if k == "Accept-Encoding" || k == "Content-Length" || k == "User-Agent" {
                continue
            }
            headers[k] = v[0]
        }
        done <- struct{}{}
    }))
    defer srv.Close()

    req := ScheduleRequest{
    	Id:         0,
    	Endpoint:   srv.URL,
    	Headers:    map[string]string{},
    	Payload:    "sadf",
    	SendAfter:  0,
    	MaxRetry:   0,
    	BackOffMs:  0,
    	TimeToLive: 0,
    }

    ms.ret = []ScheduleRequest{req}


    select {
    case <- done:
        break
    case <-time.After(2 * time.Second):
        t.Error("timoute, endpoint not called in 2 seconds")
    }

    if recivedPayload != req.Payload {
        t.Errorf("expected payload %s got %s\n", req.Payload, recivedPayload)
    }

    if len(headers) != 0 {
        t.Errorf("expected no headers got %+v\n", headers)
    }
}

func TestCallHttpWithHeaders(t *testing.T) {
    var recivedPayload string
    headers := make(map[string]string)
    done := make(chan struct{})
    srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
        body, err := io.ReadAll(r.Body)
        if err != nil {
            t.Error(err)
        } else {
            recivedPayload = string(body)
        }

        for k, v := range r.Header {
            if k == "Accept-Encoding" || k == "Content-Length" || k == "User-Agent" {
                continue
            }
            headers[k] = v[0]
        }
        done <- struct{}{}
    }))
    defer srv.Close()

    req := ScheduleRequest{
    	Id:         0,
    	Endpoint:   srv.URL,
        Headers:    map[string]string{"Test": "test-1243"},
    	Payload:    "sadf",
    	SendAfter:  0,
    	MaxRetry:   0,
    	BackOffMs:  0,
    	TimeToLive: 0,
    }

    ms.ret = []ScheduleRequest{req}

    select {
    case <- done:
        break
    case <-time.After(2 * time.Second):
        t.Error("timoute, endpoint not called in 2 seconds")
    }

    if recivedPayload != req.Payload {
        t.Errorf("expected payload %s got %s\n", req.Payload, recivedPayload)
    }

    if len(headers) != 1 {
        t.Errorf("expected some headers got %+v\n", headers)
    }

    if r, ok := headers["Test"]; !ok || r != "test-1243" {
        t.Errorf("unexpected headers got %+v\n", headers)
    }
}

