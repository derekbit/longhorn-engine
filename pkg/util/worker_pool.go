package util

import "sync"

const (
	workerPoolWorkerMax = 5
	workerPoolQueueMax  = 1024
)

type Task interface {
	Execute()
}

type WorkerPool struct {
	sync.Mutex
	size      int
	workQueue chan Task
	kill      chan struct{}
	wg        sync.WaitGroup
}

func NewWorkerPool() *WorkerPool {
	pool := &WorkerPool{
		workQueue: make(chan Task, workerPoolQueueMax),
		kill:      make(chan struct{}),
	}
	pool.Resize(workerPoolWorkerMax)
	return pool
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case task, ok := <-p.workQueue:
			if !ok {
				return
			}
			task.Execute()
		case <-p.kill:
			return
		}
	}
}

func (p *WorkerPool) Resize(n int) {
	p.Lock()
	defer p.Unlock()
	for p.size < n {
		p.size++
		p.wg.Add(1)
		go p.worker()
	}
	for p.size > n {
		p.size--
		p.kill <- struct{}{}
	}
}

func (p *WorkerPool) GetSize() int {
	return p.size
}

func (p *WorkerPool) Close() {
	close(p.workQueue)
}

func (p *WorkerPool) Wait() {
	p.wg.Wait()
}

func (p *WorkerPool) Exec(task Task) {
	p.workQueue <- task
}
