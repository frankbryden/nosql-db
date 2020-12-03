package util

import (
	"time"
)

type callback func()

//Worker will run at regular intervals calling the passed function
type Worker struct {
	callback callback
	interval time.Duration
	running  bool
}

//NewWorker creates a Worker instance with the associated callback func and interval
func NewWorker(callback callback, interval time.Duration) *Worker {
	return &Worker{callback: callback, interval: interval, running: false}
}

//Start the worker
func (w *Worker) Start() {
	w.running = true
	go w.doTick()
}

//Stop the worker. Will not run the next iter if between two ticks.
func (w *Worker) Stop() {
	w.running = false
}

func (w *Worker) doTick() {
	<-time.After(w.interval)
	if w.running {
		w.callback()
		go w.doTick()
	}
}
