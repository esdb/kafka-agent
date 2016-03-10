package dchannel

import (
	"testing"
	"os"
	"log"
	"reflect"
)

func Test_empty(t *testing.T) {
	q := openTestQueue()
	defer q.Close()
}

func Test_pop_from_empty(t *testing.T) {
	q := openTestQueue()
	defer q.Close()
	packets, _ := q.Pop()
	assertEq(0, len(packets))
}

func Test_push_then_pop(t *testing.T) {
	q := openTestQueue()
	defer q.Close()
	q.Push([][]byte{
		[]byte("A"),
	})
	packets, _ := q.Pop()
	assertEq(1, len(packets))
	assertEq("A", string(packets[0]))
}

func Test_push_multiple_then_pop(t *testing.T) {
	q := openTestQueue()
	defer q.Close()
	q.Push([][]byte{
		[]byte("A"),
		[]byte("B"),
		[]byte("C"),
	})
	packets, _ := q.Pop()
	assertEq(3, len(packets))
	assertEq("A", string(packets[0]))
	assertEq("B", string(packets[1]))
	assertEq("C", string(packets[2]))
}

func Test_push_wrap_around_when_body_overflow(t *testing.T) {
	q := openTestQueue()
	defer q.Close()
	assertEq(1011, len(q.body))
	// A wrapped
	q.header.setNextWriteAt(1008)
	err := q.Push([][]byte{
		[]byte("A"),
	})
	assertNil(err)
	assertEq(uint32(1011), q.header.getNextWriteAt())
	q.header.setNextReadAt(1008)
	packets, _ := q.Pop()
	assertEq(1, len(packets))
	assertEq("A", string(packets[0]))
	// second byte of packet size wrapped
	q.header.setNextWriteAt(1009)
	q.Push([][]byte{
		[]byte("A"),
	})
	assertEq(uint32(3), q.header.getNextWriteAt())
	q.header.setNextReadAt(1009)
	packets, _ = q.Pop()
	assertEq(1, len(packets))
	assertEq("A", string(packets[0]))
	// first byte of packet size wrapped
	q.header.setNextWriteAt(1010)
	q.Push([][]byte{
		[]byte("A"),
	})
	assertEq(uint32(3), q.header.getNextWriteAt())
	q.header.setNextReadAt(1010)
	packets, _ = q.Pop()
	assertEq(1, len(packets))
	assertEq("A", string(packets[0]))
}

func Test_push_wrap_around_when_view_overflow(t *testing.T) {
	q := openTestQueue()
	defer q.Close()
	assertEq(1011, len(q.body))
	q.view = q.body[:10]
	q.header.setNextWriteAt(10)
	err := q.Push([][]byte{
		[]byte("A"),
	})
	assertNil(err)
	assertEq(uint32(13), q.header.getNextWriteAt())
	q.header.setNextReadAt(10)
	packets, _ := q.Pop()
	assertEq(1, len(packets))
	assertEq("A", string(packets[0]))
}

const ASSERT_FAILED = "ASERT_FAILED"

func assertNotNil(obj interface{}) {
	if obj == nil {
		log.Println(obj, " should not be nil")
		panic(ASSERT_FAILED)
	}
}

func assertNil(obj interface{}) {
	if obj != nil {
		log.Println(obj, " should be nil")
		panic(ASSERT_FAILED)
	}
}

func assertEq(expected interface{}, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		log.Println(actual, " should be ", expected)
		panic(ASSERT_FAILED)
	}
}

func openTestQueue() *DQueue {
	ensureFileNotExist("/tmp/q")
	q, _ := Open("/tmp/q", 1)
	assertNotNil(q)
	return q
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

