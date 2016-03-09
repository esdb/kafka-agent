package dchannel

import (
	"testing"
	"os"
	"reflect"
)

func Test_empty(t *testing.T) {
	ensureFileNotExist("/tmp/q")
	q, err := Open("/tmp/q", 1)
	if err != nil {
		t.Error(reflect.TypeOf(err))
	}
	if q != nil {
		defer q.Close()
	}
}

func ensureFileNotExist(filePath string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// file does not exist
		return nil
	} else if err != nil {
		return err
	} else {
		err = os.Remove(filePath)
		if err != nil {
			return err
		} else {
			return nil
		}
	}
}

