package core

import "sync"

type Engine struct {
	store   map[string]string
	muStore sync.Mutex
}

func NewEngine() *Engine {
	return &Engine{store: make(map[string]string)}
}

func (e *Engine) Get(key string) string {
	e.muStore.Lock()
	defer e.muStore.Unlock()
	return e.store[key]
}

func (e *Engine) Put(key string, value string) {
	e.muStore.Lock()
	defer e.muStore.Unlock()
	e.store[key] = value
}
