package dchannel

import (
	"testing"
	"os"
	"log"
	"reflect"
	"strconv"
"math/rand"
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
	q.nextReadAt = 1008
	packets, _ := q.Pop()
	assertEq(1, len(packets))
	assertEq("A", string(packets[0]))
	// second byte of packet size wrapped
	q.header.setNextWriteAt(1009)
	q.Push([][]byte{
		[]byte("A"),
	})
	assertEq(uint32(3), q.header.getNextWriteAt())
	q.nextReadAt = 1009
	packets, _ = q.Pop()
	assertEq(1, len(packets))
	assertEq("A", string(packets[0]))
	// first byte of packet size wrapped
	q.header.setNextWriteAt(1010)
	q.Push([][]byte{
		[]byte("A"),
	})
	assertEq(uint32(3), q.header.getNextWriteAt())
	q.nextReadAt = 1010
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
	q.nextReadAt = 10
	packets, _ := q.Pop()
	assertEq(1, len(packets))
	assertEq("A", string(packets[0]))
}

func Test_next_read_at_will_be_updated_at_next_round(t *testing.T) {
	q := openTestQueue()
	defer q.Close()
	q.Push([][]byte{
		[]byte("A"),
	})
	q.Pop()
	assertEq(uint32(0), q.header.getNextReadAt())
	packets, err := q.Pop()
	assertNil(err)
	assertEq(0, len(packets))
	assertEq(uint32(3), q.header.getNextReadAt())
}

func Test_write_overflow_read(t *testing.T) {
	q := openTestQueue()
	defer q.Close()
	q.view = q.body[:]
	q.nextReadAt = 1
	q.Push([][]byte{
		[]byte("A"),
	})
	assertEq(uint32(3), q.header.getNextWriteAt())
	assertEq(uint32(3), q.nextReadAt)
	assertEq(uint32(3), q.header.getNextReadAt())
}

func Test_write_overflow_read_real_case(t *testing.T) {
	q := openTestQueue()
	defer q.Close()
	assertEq(1011, len(q.body))
	for i := 0; i < 1024; i++ {
		q.Push([][]byte{
			[]byte(strconv.Itoa(i)),
		})
	}
	packets, err := q.Pop()
	assertNil(err)
	assertNotNil(packets)
	assertEq(194, len(packets))
	assertEq("830", string(packets[0]))
}

func Test_random_push_pop(t *testing.T) {
	q := openTestQueue()
	defer q.Close()
	for j := 0; j < 1024; j++ {
		for i := 0; i < 1+int(rand.Int31n(10)); i++ {
			q.Push([][]byte{
				make([]byte, rand.Int31n(10)),
			})
		}
		for i := 0; i < int(rand.Int31n(10)); i++ {
			packets, err := q.Pop()
			assertNil(err)
			if i == 0 {
				if len(packets) == 0 {
					log.Println("next write at", q.header.getNextWriteAt())
					log.Println("next read at", q.header.getNextReadAt())
					log.Println("next read at (uncommitted)", q.nextReadAt)
					log.Println("view size", len(q.view))
					log.Println("body size", len(q.body))
					return
				}
			}
		}
	}
}

const ASSERT_FAILED = "ASERT_FAILED"

func assertNotNil(obj interface{}) {
	if obj == nil {
		log.Println(obj, "should not be nil")
		panic(ASSERT_FAILED)
	}
}

func assertNil(obj interface{}) {
	if obj != nil {
		log.Println(obj, "should be nil")
		panic(ASSERT_FAILED)
	}
}

func assertEq(expected interface{}, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		log.Println(actual, "should be", expected)
		panic(ASSERT_FAILED)
	}
}

type Comparable interface {
	compare(that Comparable) int
}

type comp_int int

//func (this comp_int) compare(thatObj Comparable) int {
//	that, err := thatObj.(comp_int)
//	if err != nil {
//		return false
//	}
//	if this > that {
//		return 1
//	} else if this == that {
//		return 0
//	} else {
//		return -1
//	}
//}

func assertGt(left interface{}, right interface{}) {
	if !_assertGt(left, right) {
		log.Println(left, "should >", right)
		panic(ASSERT_FAILED)
	}
}

func _assertGt(left interface{}, right interface{}) bool {
	leftAsInt, ok := left.(int)
	if ok {
		rightAsInt, ok := right.(int)
		if (ok) {
			return leftAsInt > rightAsInt
		} else {
			log.Println(reflect.TypeOf(left), "not comparable to", reflect.TypeOf(right))
			panic(ASSERT_FAILED)
		}
	} else {
		log.Println(reflect.TypeOf(left), "not comparable to", reflect.TypeOf(right))
		panic(ASSERT_FAILED)
	}
	return false
}

func assertGte(left interface{}, right interface{}) {
	if reflect.DeepEqual(left, right) {
		return
	}
	assertGt(left, right)
}

func assertLt(left interface{}, right interface{}) {
	if reflect.DeepEqual(left, right) || _assertGt(left, right) {
		log.Println(left, "should <", right)
		panic(ASSERT_FAILED)
	}
}

func assertLte(left interface{}, right interface{}) {
	if _assertGt(left, right) {
		log.Println(left, "should <=", right)
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

