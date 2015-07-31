package s3gof3r

import (
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"sync"
	"syscall"
	"time"
)

const (
	qWaitSz = 2
)

type getter struct {
	url   url.URL
	b     *Bucket
	bufsz int64
	err   error
	wg    sync.WaitGroup

	chunkID    int
	rChunk     *chunk
	contentLen int64
	bytesRead  int64
	chunkTotal int
	chunkCounter int
	chunkWg sync.WaitGroup

	readCh chan *chunk
	getCh  chan *chunk
	quit   chan struct{}
	qWait  map[int]*chunk

	sp *bp

	closed bool
	c      *Config

	md5  hash.Hash
	cIdx int64

}

type chunk struct {
	id int // The chunk number for the file being retrieved
	start int64 // The position in the requested file at which this chunk's data begins
	size int64 // Number of bytes contained in this chunk
	fileSize int64 // Total size of the requested file
	b []byte // The bytes retrieved from S3. Length is set by cfg.PartSize, but only contains ChunkSize bytes
	path string // S3 file path this chunk comes from
	error error // contains the error that occurred, if any, while retrieving this chunk
	header     http.Header
	response   *http.Response
	url        url.URL
}

func newBatchGetter( c *Config, b *Bucket) (*getter, error) {
	g := new(getter)
	g.c = c
	g.bufsz = max64(c.PartSize, 1)
	g.c.NTry = max(c.NTry, 1)
	g.c.Concurrency = max(c.Concurrency, 1)

	g.getCh = make(chan *chunk, c.Concurrency)
	g.readCh = make(chan *chunk, c.Concurrency)
	g.quit = make(chan struct{})
	g.qWait = make(map[int]*chunk)
	g.b = b
	g.md5 = md5.New()
	g.chunkTotal = 0
	g.chunkCounter = 0


	g.sp = bufferPool(g.bufsz)

	for i := 0; i < g.c.Concurrency; i++ {
		go g.worker()
	}

	return g, nil
}

func newGetter(getURL url.URL, c *Config, b *Bucket) (io.ReadCloser, http.Header, error) {

	g, err := newBatchGetter(c, b)
	if err != nil {
		return g, nil, err
	}

	header, err := g.queueFile(&getURL)

	go func(){
		g.chunkWg.Wait()
		close(g.getCh)
		g.wg.Wait()
		close(g.readCh)
	}()

	return g, header, err
}

func (g *getter) retryRequest(method, urlStr string, body io.ReadSeeker) (resp *http.Response, err error) {
	for i := 0; i < g.c.NTry; i++ {
		var req *http.Request
		req, err = http.NewRequest(method, urlStr, body)
		if err != nil {
			return
		}
		g.b.Sign(req)
		resp, err = g.c.Client.Do(req)
		if err == nil {
			return
		}
		logger.debugPrintln(err)
		if body != nil {
			if _, err = body.Seek(0, 0); err != nil {
				return
			}
		}
	}
	return
}

func (g *getter) queueFile(url *url.URL) (http.Header, error) {

	g.chunkWg.Add(1)
	resp, err := g.retryRequest("GET", url.String(), nil)
	if err != nil {
		return resp.Header, err
	}

	if resp.StatusCode != 200 {
		return resp.Header, fmt.Errorf("Bad status for HTTP response: %d", resp.StatusCode)
	}

	// Golang changes content-length to -1 when chunked transfer encoding / EOF close response detected
	if resp.ContentLength == -1 {
		return resp.Header, fmt.Errorf("Retrieving objects with undefined content-length " +
			" responses (chunked transfer encoding / EOF close) is not supported")
	}

	g.contentLen += resp.ContentLength
	g.chunkTotal += int((resp.ContentLength + g.bufsz - 1) / g.bufsz) // round up, integer division

	logger.debugPrintf("object size: %3.2g MB", float64(resp.ContentLength)/float64((1*mb)))
	go g.initChunks(resp, url.String())
	return resp.Header, nil
}

func (g *getter) initChunks(resp *http.Response, path string) {
	for i := int64(0); i < resp.ContentLength; {
		for len(g.qWait) >= qWaitSz {
			// Limit growth of qWait
			time.Sleep(100 * time.Millisecond)
		}
		size := min64(g.bufsz, resp.ContentLength-i)
		c := &chunk{
			id: g.chunkCounter,
			header: http.Header{
				"Range": {fmt.Sprintf("bytes=%d-%d",
					i, i+size-1)},
			},
			start:     i,
			size:	   size,
			b:         nil,
			url:       *resp.Request.URL,
			path:      path,
			fileSize:  resp.ContentLength,
		}

		//Re-use the response for the first chunk
		if i == 0 {
			c.response = resp
		}
		i += size
		g.chunkCounter++
		g.wg.Add(1)
		g.getCh <- c
	}
	g.chunkWg.Done()
}

func (g *getter) worker() {
	for c := range g.getCh {
		g.retryGetChunk(c)
	}

}

func (g *getter) retryGetChunk(c *chunk) {
	defer g.wg.Done()
	var err error

	c.b = <-g.sp.get

	for i := 0; i < g.c.NTry; i++ {
		time.Sleep(time.Duration(math.Exp2(float64(i))) * 100 * time.Millisecond) // exponential back-off
		err = g.getChunk(c)
		if err == nil {
			return
		}
		logger.debugPrintf("error on attempt %d: retrying chunk: %v, error: %s", i, c.id, err)
		//If there is a re-used response in this chunk, it should be discarded since an error was returned
		c.response = nil
	}
	g.err = err
	c.error = err
	g.readCh <- c //expose error to the chunk handler
	close(g.quit) // out of tries, ensure quit by closing channel
}

func (g *getter) getChunk(c *chunk) error {
	var resp *http.Response

	//The first chunk will have the initial response in it to re-use
	if c.response == nil {

		r, err := http.NewRequest("GET", c.url.String(), nil)
		if err != nil {
			return err
		}
		r.Header = c.header
		g.b.Sign(r)
		respTmp, err := g.c.Client.Do(r)
		resp = respTmp //Necessary so 'resp' doesn't turn nil
		if err != nil {
			return err
		}
		if resp.StatusCode != 206 {
			return newRespError(resp)
		}
	} else {
		resp = c.response
	}

	var err error
	defer checkClose(resp.Body, &err)

	n, err := io.ReadAtLeast(resp.Body, c.b, int(c.size))
	if err != nil {
		return err
	}
	if int64(n) != c.size {
		return fmt.Errorf("chunk %d: Expected %d bytes, received %d",
				c.id, c.size, n)
	}
	g.readCh <- c
	return nil
}

func (g *getter) Read(p []byte) (int, error) {
	var err error
	if g.closed {
		return 0, syscall.EINVAL
	}
	if g.err != nil {
		return 0, g.err
	}
	nw := 0
	for nw < len(p) {
		if g.bytesRead == g.contentLen {
			return nw, io.EOF
		} else if g.bytesRead > g.contentLen {
			// Here for robustness / completeness
			// Should not occur as golang uses LimitedReader up to content-length
			return nw, fmt.Errorf("Expected %d bytes, received %d (too many bytes)",
				g.contentLen, g.bytesRead)
		}

		// If for some reason no more chunks to be read and bytes are off, error, incomplete result
		if g.chunkID >= g.chunkTotal {
			return nw, fmt.Errorf("Expected %d bytes, received %d and chunkID %d >= chunkTotal %d (no more chunks remaining)",
				g.contentLen, g.bytesRead, g.chunkID, g.chunkTotal)
		}

		if g.rChunk == nil {
			g.rChunk, err = g.nextChunk()
			if err != nil {
				return 0, err
			}
			g.cIdx = 0
		}

		n := copy(p[nw:], g.rChunk.b[g.cIdx:g.rChunk.size])
		g.cIdx += int64(n)
		nw += n
		g.bytesRead += int64(n)

		if g.cIdx >= g.rChunk.size { // chunk complete
			g.sp.give <- g.rChunk.b
			g.chunkID++
			g.rChunk = nil
		}
	}
	return nw, nil

}

func (g *getter) WriteToWriterAt(w io.WriterAt) (int, error){
	fileOffsetMap := make(map[string]int64)
	filePosition := int64(0)
	totalWritten := int(0)

	for chunk := range g.readCh {
		fileOffset, present := fileOffsetMap[chunk.path]
		if !present {
			fileOffset = filePosition
			fileOffsetMap[chunk.path] = filePosition
			filePosition += chunk.fileSize
		}

		var chunkWritten int = 0
		for (int64(chunkWritten) < chunk.size){
			n, err := w.WriteAt(chunk.b[:chunk.size], fileOffset + chunk.start)
			chunkWritten += n
			totalWritten += n
			if err != nil {
				return totalWritten, err
			}
		}

		g.sp.give <- chunk.b
	}
	return totalWritten, nil
}

func (g *getter) WriteTo(w io.Writer) (int64, error) {
	// use WriteAt if present so chunks can be written out of order and release memory faster
	if wa, ok := w.(io.WriterAt); ok {
		nw, err := g.WriteToWriterAt(wa)
		return int64(nw), err
	}

	var err error
	if g.closed {
		return 0, syscall.EINVAL
	}
	if g.err != nil {
		return 0, g.err
	}
	nw := int64(0)

	for g.chunkID < g.chunkTotal {
		if g.bytesRead == g.contentLen {
			return nw, io.EOF
		} else if g.bytesRead > g.contentLen {
			// Here for robustness / completeness
			// Should not occur as golang uses LimitedReader up to content-length
			return nw, fmt.Errorf("Expected %d bytes, received %d (too many bytes)",
				g.contentLen, g.bytesRead)
		}

		// If for some reason no more chunks to be read and bytes are off, error, incomplete result
		if g.chunkID >= g.chunkTotal {
			return nw, fmt.Errorf("Expected %d bytes, received %d and chunkID %d >= chunkTotal %d (no more chunks remaining)",
				g.contentLen, g.bytesRead, g.chunkID, g.chunkTotal)
		}

		if g.rChunk == nil {
			g.rChunk, err = g.nextChunk()
			if err != nil {
				return 0, err
			}
			g.cIdx = 0
		}

		n, writeError := w.Write(g.rChunk.b[g.cIdx:g.rChunk.size])
		g.cIdx += int64(n)
		nw += int64(n)
		g.bytesRead += int64(n)

		if writeError != nil {
			return nw, err
		}

		if g.cIdx >= g.rChunk.size { // chunk complete
			g.sp.give <- g.rChunk.b
			g.chunkID++
			g.rChunk = nil
		}
	}
	return nw, nil

}

func (g *getter) nextChunk() (*chunk, error) {
	for {

		// first check qWait
		c := g.qWait[g.chunkID]
		if c != nil {
			delete(g.qWait, g.chunkID)
			if g.c.Md5Check {
				if _, err := g.md5.Write(c.b[:c.size]); err != nil {
					return nil, err
				}
			}
			return c, nil
		}
		// if next chunk not in qWait, read from channel
		select {
		case c := <-g.readCh:
			g.qWait[c.id] = c
		case <-g.quit:
			return nil, g.err // fatal error, quit.
		}
	}
}

func (g *getter) Close() error {
	if g.closed {
		return syscall.EINVAL
	}
	if g.err != nil {
		return g.err
	}
	g.wg.Wait()
	g.closed = true
	close(g.sp.quit)
	if g.bytesRead != g.contentLen {
		return fmt.Errorf("read error: %d bytes read. expected: %d", g.bytesRead, g.contentLen)
	}
	if g.c.Md5Check {
		if err := g.checkMd5(); err != nil {
			return err
		}
	}
	return nil
}

func (g *getter) checkMd5() (err error) {
	calcMd5 := fmt.Sprintf("%x", g.md5.Sum(nil))
	md5Path := fmt.Sprint(".md5", g.url.Path, ".md5")
	md5Url, err := g.b.url(md5Path, g.c)
	if err != nil {
		return err
	}

	logger.debugPrintln("md5: ", calcMd5)
	logger.debugPrintln("md5Path: ", md5Path)
	resp, err := g.retryRequest("GET", md5Url.String(), nil)
	if err != nil {
		return
	}
	defer checkClose(resp.Body, &err)
	if resp.StatusCode != 200 {
		return fmt.Errorf("MD5 check failed: %s not found: %s", md5Url.String(), newRespError(resp))
	}
	givenMd5, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if calcMd5 != string(givenMd5) {
		return fmt.Errorf("MD5 mismatch. given:%s calculated:%s", givenMd5, calcMd5)
	}
	return
}
