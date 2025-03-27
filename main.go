package main

import (
	"log"
	"net/http"
)

func get(w http.ResponseWriter, r *http.Request) {

}

func put(w http.ResponseWriter, r *http.Request) {

}

func main() {
	http.HandleFunc("GET", get)
	http.HandleFunc("PUT", put)

	log.Println("Sever on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
