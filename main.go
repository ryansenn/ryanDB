package main

import (
	"log"
	"net/http"
)

func get(w http.ResponseWriter, r *http.Request) {
	log.Println("get key=", r.URL.Query().Get("key"))
}

func put(w http.ResponseWriter, r *http.Request) {
	log.Println("put key=", r.URL.Query().Get("key"), "value=", r.URL.Query().Get("value"))
}

func main() {
	http.HandleFunc("/get", get)
	http.HandleFunc("/put", put)

	log.Println("Server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
