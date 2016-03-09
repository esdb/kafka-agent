package dchannel

import (
	"os"
	"github.com/edsrzf/mmap-go"
)

type DQueue struct {
	fileObj *os.File
	mappedFile mmap.MMap
}

func Open(filePath string, nMegaBytes int) (*DQueue, error) {
	fileObj, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			fileObj, err = os.OpenFile(filePath, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0644)
			if err != nil {
				return nil, err
			}
			emptyBytes := [1024 * 1024]byte{}
			for i := 0; i < nMegaBytes; i++ {
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
	return &DQueue{
		fileObj: fileObj,
		mappedFile: mappedFile,
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