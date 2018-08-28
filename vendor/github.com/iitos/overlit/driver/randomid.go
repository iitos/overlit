package driver

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

func generateID(l int) string {
	const (
		maxretries = 9
		backoff    = time.Millisecond * 10
	)

	var (
		totalBackoff time.Duration
		count        int
		retries      int
		size         = (l*5 + 7) / 8
		u            = make([]byte, size)
	)

	for {
		b := time.Duration(retries) * backoff
		time.Sleep(b)
		totalBackoff += b

		n, err := io.ReadFull(rand.Reader, u[count:])
		if err != nil {
			if retryOnError(err) && retries < maxretries {
				count += n
				retries++
				logrus.Errorf("error generating version 4 uuid, retrying: %v", err)
				continue
			}

			panic(fmt.Errorf("error reading random number generator, retried for %v: %v", totalBackoff.String(), err))
		}

		break
	}

	s := base32.StdEncoding.EncodeToString(u)

	return s[:l]
}

func retryOnError(err error) bool {
	switch err := err.(type) {
	case *os.PathError:
		return retryOnError(err.Err)
	case syscall.Errno:
		if err == unix.EPERM {
			return true
		}
	}

	return false
}
