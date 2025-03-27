package main

import (
	"log"
	"net/http"
	"ryanDB/storage"
)

var engine = storage.NewEngine()

func get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	log.Println("get key=", key)
	value := engine.Get(key)
	w.Write([]byte(value))
}

func put(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	log.Println("put key=", key, "value=", value)
	engine.Put(key, value)
}

func main() {
	http.HandleFunc("/get", get)
	http.HandleFunc("/put", put)

	log.Println("Server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
