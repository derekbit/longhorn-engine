package controller

import (
	"io"
	"strings"
	"sync"
)

type MultiWriterAt struct {
	writers []io.WriterAt
}

type MultiWriterError struct {
	Writers []io.WriterAt
	Errors  []error
}

func (m *MultiWriterError) Error() string {
	errors := []string{}
	for _, err := range m.Errors {
		if err != nil {
			errors = append(errors, err.Error())
		}
	}

	switch len(errors) {
	case 0:
		return "Unknown"
	case 1:
		return errors[0]
	default:
		return strings.Join(errors, "; ")
	}
}

//var totalTime int64
//var totalCount int64

func (m *MultiWriterAt) WriteAt(p []byte, off int64) (int, error) {
	errs := make([]error, len(m.writers))
	wg := sync.WaitGroup{}

	for i, w := range m.writers {
		wg.Add(1)
		go func(index int, w io.WriterAt) {
			//start := time.Now()
			_, err := w.WriteAt(p, off)
			//elapsed := time.Since(start)
			//logrus.Infof("Debug ---> MultiWriterAt elapsed=%v  size=%v", elapsed.Nanoseconds(), len(p))
			//totalTime = totalTime + int64(elapsed)
			//totalCount = totalCount + 1
			//avg := totalTime / totalCount
			//ogrus.Infof("Debug ---> MultiWriterAt elapsed=%v  size=%v avg=%v", elapsed.Nanoseconds(), len(p), avg)
			if err != nil {
				errs[index] = err
			}

			wg.Done()
		}(i, w)
	}

	wg.Wait()

	n := 0
	var err error = nil

	for i := range errs {
		if errs[i] != nil {
			err = &MultiWriterError{
				Writers: m.writers,
				Errors:  errs,
			}
		} else {
			n = len(p)
		}
	}

	return n, err
}
