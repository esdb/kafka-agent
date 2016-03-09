Disk based queue
================

the element type is []byte

* Push: [][]byte into the queue
* Pop: mark last pop as done, then read as many as possible []byte in the form [][]byte from the queue