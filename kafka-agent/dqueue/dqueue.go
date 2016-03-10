package dchannel

import (
	"os"
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	"errors"
)

const HEADER_SIZE = 13 // 1 for version 4 for viewSize 4 for nextWriteAt 4 for nextReadAt
const MAX_READ_PACKET_COUNT = 1024
const MAX_PACKET_SIZE = 1024 * 16

type DQueue struct {
	fileObj *os.File
	mappedFile mmap.MMap
	nextReadAt uint32
	header header
	body []byte
	/*
	view covers the readable range of body
	to avoid split packet in two parts to wrap around
	[--- header ---][--- body ---]
	                [--- view --]
	 */
	view []byte
	readBuffers [][]byte
	readBufferItself [][]byte
}

type header []byte

func (header header) setVersion() {
	header[0] = 1
}

func (header header) setViewSize(viewSize uint32) {
	binary.BigEndian.PutUint32(header[1:], viewSize)
}

func (header header) getViewSize() uint32 {
	return binary.BigEndian.Uint32(header[1:])
}

func (header header) setNextWriteAt(nextWriteAt uint32) {
	binary.BigEndian.PutUint32(header[5:], nextWriteAt)
}

func (header header) getNextWriteAt() uint32 {
	return binary.BigEndian.Uint32(header[5:])
}

func (mappedBytes header) setNextReadAt(nextReadAt uint32) {
	binary.BigEndian.PutUint32(mappedBytes[9:], nextReadAt)
}

func (mappedBytes header) getNextReadAt() uint32 {
	return binary.BigEndian.Uint32(mappedBytes[9:])
}

func Open(filePath string, nkiloBytes int) (*DQueue, error) {
	fileObj, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			fileObj, err = os.OpenFile(filePath, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0644)
			if err != nil {
				return nil, err
			}
			emptyBytes := make([]byte, 1024)
			for i := 0; i < nkiloBytes; i++ {
				_, err = fileObj.Write(emptyBytes[:])
				if err != nil {
					return nil, err
				}
			}
			err = fileObj.Close()
			if err != nil {
				return nil, err
			}
			fileObj, err = os.OpenFile(filePath, os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	mappedFile, err := mmap.Map(fileObj, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	header := header(mappedFile[:HEADER_SIZE])
	header.setVersion()
	header.setNextWriteAt(0)
	header.setNextReadAt(0)
	body := mappedFile[HEADER_SIZE:]
	view := body[:0]
	header.setViewSize(uint32(len(view)))
	readBuffers := make([][]byte, MAX_READ_PACKET_COUNT)
	readBufferItself := make([][]byte, MAX_READ_PACKET_COUNT)
	for i := 0; i < MAX_READ_PACKET_COUNT; i++ {
		readBuffers[i] = make([]byte, 1024 * 256)
	}
	return &DQueue{
		fileObj: fileObj,
		mappedFile: mappedFile,
		header: header,
		body: body,
		view: view,
		readBuffers: readBuffers,
		readBufferItself: readBufferItself,
	}, nil
}

func (q *DQueue) Close() error {
	err := q.mappedFile.Unmap()
	if err != nil {
		return err
	}
	err = q.fileObj.Close()
	if err != nil {
		return err
	}
	return nil
}

func (q *DQueue) Pop() ([][]byte, error) {
	nextWriteAt := q.header.getNextWriteAt()
	nextReadAt := q.nextReadAt
	q.header.setNextReadAt(nextReadAt)
	packetsCount := 0
	fallBehind := false
	if nextReadAt == nextReadAt && nextWriteAt == uint32(len(q.view)) {
		// at tail
	} else if nextReadAt >= nextWriteAt {
		fallBehind = true
	}
	pos := nextReadAt
	for ; packetsCount < len(q.readBufferItself); packetsCount++ {
		if !fallBehind && pos >= nextWriteAt {
			break
		}
		viewSize := uint32(len(q.view))
		if pos >= viewSize {
			pos = 0 // wrap around
			fallBehind = false
			// the region between [nextWriteAt, tail) is now invalid
			q.view = q.body[:nextWriteAt]
		}
		packetSize := binary.BigEndian.Uint16(q.view[pos:pos+2])
		if packetSize > MAX_PACKET_SIZE {
			return nil, errors.New("packet is too large")
		}
		pos += 2
		nextPos := pos + uint32(packetSize)
		readBuffer := q.readBuffers[packetsCount][:packetSize]
		copy(readBuffer, q.view[pos:nextPos])
		q.readBufferItself[packetsCount] = readBuffer
		pos = nextPos
	}
	q.nextReadAt = pos
	return q.readBufferItself[:packetsCount], nil
}

func (q *DQueue) Push(packets [][]byte) error {
	pos := q.header.getNextWriteAt()
	if pos > uint32(len(q.body)) {
		return errors.New("internal error: nextWriteAt is invalid")
	}
	for _, packet := range packets {
		packetSize := uint16(len(packet))
		if packetSize > MAX_PACKET_SIZE {
			return errors.New("packet is too large")
		}
		viewSize := uint32(len(q.view))
		willWriteTo := pos + 2 + uint32(packetSize)
		// write range is [pos, willWriteTo)
		if q.nextReadAt > pos && willWriteTo > q.nextReadAt {
			// overflow the read
			q.nextReadAt = willWriteTo
			q.header.setNextReadAt(q.nextReadAt)
		}
		if willWriteTo > viewSize {
			// overflow the view
			if willWriteTo > uint32(len(q.body)) {
				// overflow the body, shrink the view
				q.view = q.body[:pos]
				pos = 0
			} else {
				// grow the view to cover
				q.view = q.body[:willWriteTo]
			}
		}
		binary.BigEndian.PutUint16(q.view[pos:pos+2], packetSize)
		pos += 2
		nextPos := pos + uint32(packetSize)
		copy(q.view[pos:nextPos], packet)
		pos = nextPos
	}
	q.header.setNextWriteAt(pos)
	q.header.setViewSize(uint32(len(q.view)))
	return nil
}