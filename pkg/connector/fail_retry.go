package connector

import (
	"time"

	"github.com/pkg/errors"
)

// Retry 重试 3次,每次等 500 ms
func Retry(f func() error) error {
	var err error
	for i := 0; i < 3; i++ {
		err = f()
		if err == nil {
			return nil
		}
		time.Sleep(time.Millisecond * 500)
	}
	return errors.WithStack(err)
}
