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
	b     *Bucket
	bufsz int64
	err   error
	wg    sync.WaitGroup

	chunkID    int
	rChunk     *Chunk
	bytesRead  int64
	chunkTotal int

	readCh chan *Chunk
	getCh  chan *Chunk
	quit   chan struct{}

	sp *bp

	closed bool
	c      *Config

	md5  hash.Hash
	cIdx int64
}

type singleGetter struct {
	getter
	url        url.URL
	contentLen int64
	qWait      map[int]*Chunk
}

//New struct to hold methods that only make sense for multi-file downloads
type batchGetter struct {
	getter
}

type Chunk struct {
	Id         int // The chunk number for the file being retrieved
	Start      int64 // The position in the requested file at which this chunk's data begins
	ChunkSize  int64 // Number of bytes contained in this chunk
	FileSize   int64 // Total size of the requested file
	Bytes      []byte // The bytes retrieved from S3. Length is set by cfg.PartSize, but only contains ChunkSize bytes
	Path       string // S3 file path this chunk comes from
	Error      error // contains the error that occurred, if any, while retrieving this chunk
	header     http.Header
	chunkTotal int64
	response   *http.Response
	url        url.URL
}

func newBatchGetter(c *Config, b *Bucket) *batchGetter {
	g := new(batchGetter)
	g.c = c
	g.bufsz = max64(c.PartSize, 1)
	g.c.NTry = max(c.NTry, 1)
	g.c.Concurrency = max(c.Concurrency, 1)

	g.getCh = make(chan *Chunk, 2*c.Concurrency)
	g.readCh = make(chan *Chunk, 2*c.Concurrency)
	g.quit = make(chan struct{})
	g.b = b
	g.md5 = md5.New()

	g.sp = bufferPool(g.bufsz)

	for i := 0; i < g.c.Concurrency; i++ {
		go g.worker()
	}
	return g
}

func newGetter(getURL url.URL, c *Config, b *Bucket) (io.ReadCloser, http.Header, error) {
	g := new(singleGetter)
	g.url = getURL
	g.c = c
	g.bufsz = max64(c.PartSize, 1)
	g.c.NTry = max(c.NTry, 1)
	g.c.Concurrency = max(c.Concurrency, 1)

	g.getCh = make(chan *Chunk)
	g.readCh = make(chan *Chunk)
	g.quit = make(chan struct{})
	g.qWait = make(map[int]*Chunk)
	g.b = b
	g.md5 = md5.New()

	//find out the total size of the requested file
	resp, err := g.retryRequest("GET", getURL.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != 200 {
		return nil, nil, newRespError(resp)
	}

	// Golang changes content-length to -1 when chunked transfer encoding / EOF close response detected
	if resp.ContentLength == -1 {
		return nil, nil, fmt.Errorf("Retrieving objects with undefined content-length " +
			" responses (chunked transfer encoding / EOF close) is not supported")
	}

	g.contentLen = resp.ContentLength
	g.chunkTotal = int((g.contentLen + g.bufsz - 1) / g.bufsz) // round up, integer division
	logger.debugPrintf("object size: %3.2g MB", float64(g.contentLen)/float64((1*mb)))

	g.sp = bufferPool(g.bufsz)

	for i := 0; i < g.c.Concurrency; i++ {
		go g.worker()
	}
	go g.initChunks(resp)
	return g, resp.Header, nil
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

func (bg *batchGetter) queueFile(path string) error {
	url, err := bg.b.url(path, bg.c)

	resp, err := bg.retryRequest("GET", url.String(), nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("Bad status for HTTP response: %d", resp.StatusCode)
	}

	// Golang changes content-length to -1 when chunked transfer encoding / EOF close response detected
	if resp.ContentLength == -1 {
		return fmt.Errorf("Retrieving objects with undefined content-length " +
			" responses (chunked transfer encoding / EOF close) is not supported")
	}

	logger.debugPrintf("object size: %3.2g MB", float64(resp.ContentLength)/float64((1*mb)))
	bg.initChunks(resp, path)
	return nil
}

func (bg *batchGetter) initChunks(resp *http.Response, path string) {
	id := 0
	for i := int64(0); i < resp.ContentLength; {
		size := min64(bg.bufsz, resp.ContentLength-i)
		c := &Chunk{
			Id: id,
			header: http.Header{
				"Range": {fmt.Sprintf("bytes=%d-%d",
					i, i+size-1)},
			},
			Start:     i,
			ChunkSize: size,
			Bytes:     nil,
			url:       *resp.Request.URL,
			Path:      path,
			FileSize:  resp.ContentLength,
		}

		//Re-use the response for the first chunk
		if i == 0 {
			c.response = resp
		}
		i += size
		id++
		bg.wg.Add(1)
		bg.getCh <- c
	}
}


func (g *singleGetter) initChunks(resp *http.Response) {
	id := 0
	for i := int64(0); i < g.contentLen; {
		for len(g.qWait) >= qWaitSz {
			// Limit growth of qWait
			time.Sleep(100 * time.Millisecond)
		}
		size := min64(g.bufsz, g.contentLen-i)
		c := &Chunk{
			Id: id,
			header: http.Header{
				"Range": {fmt.Sprintf("bytes=%d-%d",
					i, i+size-1)},
			},
			Start: i,
			ChunkSize:  size,
			Bytes:     nil,
			url:   *resp.Request.URL,
		}

		//Re-use the response for the first chunk
		if i == 0 {
			c.response = resp
		}

		i += size
		id++
		g.wg.Add(1)
		g.getCh <- c
	}
	close(g.getCh)
}

func (g *getter) worker() {
	for c := range g.getCh {
		g.retryGetChunk(c)
	}
}

func (g *getter) retryGetChunk(c *Chunk) {
	defer g.wg.Done()
	var err error
	c.Bytes = <-g.sp.get
	for i := 0; i < g.c.NTry; i++ {
		time.Sleep(time.Duration(math.Exp2(float64(i))) * 100 * time.Millisecond) // exponential back-off
		err = g.getChunk(c)
		if err == nil {
			return
		}
		logger.debugPrintf("error on attempt %d: retrying chunk: %v, error: %s", i, c.Id, err)
		//If there is a re-used response in this chunk, it should be discarded since an error was returned
		c.response = nil
	}
	g.err = err
	c.Error = err
	g.readCh <- c //expose error to the chunk handler
	close(g.quit) // out of tries, ensure quit by closing channel
}

func (g *getter) getChunk(c *Chunk) error {
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

	n, err := io.ReadAtLeast(resp.Body, c.Bytes, int(c.ChunkSize))
	if err != nil {
		return err
	}
	if int64(n) != c.ChunkSize {
		return fmt.Errorf("chunk %d: Expected %d bytes, received %d",
		c.Id, c.ChunkSize, n)
	}
	g.readCh <- c
	return nil
}

func (g *singleGetter) Read(p []byte) (int, error) {
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

		n := copy(p[nw:], g.rChunk.Bytes[g.cIdx:g.rChunk.ChunkSize])
		g.cIdx += int64(n)
		nw += n
		g.bytesRead += int64(n)

		if g.cIdx >= g.rChunk.ChunkSize { // chunk complete
			g.sp.give <- g.rChunk.Bytes
			g.chunkID++
			g.rChunk = nil
		}
	}
	return nw, nil

}

func (g *singleGetter) nextChunk() (*Chunk, error) {
	for {

		// first check qWait
		c := g.qWait[g.chunkID]
		if c != nil {
			delete(g.qWait, g.chunkID)
			if g.c.Md5Check {
				if _, err := g.md5.Write(c.Bytes[:c.ChunkSize]); err != nil {
					return nil, err
				}
			}
			return c, nil
		}
		// if next chunk not in qWait, read from channel
		select {
		case c := <-g.readCh:
			g.qWait[c.Id] = c
		case <-g.quit:
			return nil, g.err // fatal error, quit.
		}
	}
}

func (g *singleGetter) Close() error {
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

func (g *singleGetter) checkMd5() (err error) {
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
