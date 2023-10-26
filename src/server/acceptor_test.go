package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)


type mockStore struct {
    returnErr error
    called bool
    item *ScheduleRequest
}

func (s *mockStore) Save(r ScheduleRequest) error {
    s.called = true
    s.item = &r
    return s.returnErr
}

func TestHappyPath(t *testing.T) {
    expectedReq := ScheduleRequest{
        Endpoint: "example.com/test",
        Headers: map[string]string{"test1": "123"},
        Payload: "example",
        SendAfter: 200032,
        BackOffMs: 20,
        TimeToLive: 200050,
    }

    store := &mockStore{
    	returnErr: nil,
    	called:    false,
    	item:      &ScheduleRequest{},
    }
    srv := NewAccepter(store)

    bs, err := json.Marshal(expectedReq)
    if err != nil {
        t.Errorf("failed to marshal request %+v error: %s", expectedReq, err)
    }
    requestBody := strings.NewReader(string(bs))
    req, err := http.NewRequest(http.MethodPost, "/submit", requestBody)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

    srv.SubmitHandler(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

    if !store.called {
        t.Error("store save never called")
    }

    if reflect.DeepEqual(expectedReq, store.item) {
        t.Errorf("failed to save expected item, got %+v expected: %+v", store.item, expectedReq)
    }
}

func TestFaileSave(t *testing.T) {
    expectedReq := ScheduleRequest{
        Endpoint: "example.com/test",
        Headers: map[string]string{"test1": "123"},
        Payload: "example",
        SendAfter: 200032,
        BackOffMs: 20,
        TimeToLive: 200050,
    }

    store := &mockStore{
    	returnErr: errors.New("ups"),
    	called:    false,
    	item:      &ScheduleRequest{},
    }
    srv := NewAccepter(store)

    bs, err := json.Marshal(expectedReq)
    if err != nil {
        t.Errorf("failed to marshal request %+v error: %s", expectedReq, err)
    }

    requestBody := strings.NewReader(string(bs))
    req, err := http.NewRequest(http.MethodPost, "/submit", requestBody)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

    srv.SubmitHandler(rr, req)

	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusInternalServerError)
	}

    if !store.called {
        t.Error("store save never called")
    }

    if reflect.DeepEqual(expectedReq, store.item) {
        t.Errorf("failed to save expected item, got %+v expected: %+v", store.item, expectedReq)
    }

}
