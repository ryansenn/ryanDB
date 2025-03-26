package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

type Store struct {
	data map[string]string
	mu   sync.RWMutex
}

func (s *Store) Get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	s.mu.RLock()
	value, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"value": value})
}

func (s *Store) Put(w http.ResponseWriter, r *http.Request) {
	var req map[string]string
	json.NewDecoder(r.Body).Decode(&req)
	key, value := req["key"], req["value"]
	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (s *Store) Delete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func main() {
	store := &Store{data: make(map[string]string)}
	http.HandleFunc("/get", store.Get)
	http.HandleFunc("/put", store.Put)
	http.HandleFunc("/delete", store.Delete)

	log.Println("Server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}