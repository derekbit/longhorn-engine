package util

import (
	"sync"
)

type Perf struct {
	sync.Mutex
	Count       int64
	TimeElapsed int64
}
