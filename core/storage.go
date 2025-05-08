package core

type Engine struct {
	store map[string]string
}

func NewEngine() *Engine {
	return &Engine{store: make(map[string]string)}
}

func (e *Engine) Get(key string) string {
	return e.store[key]
}

func (e *Engine) Put(key string, value string) {
	e.store[key] = value
}
