package service

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/gorilla/handlers"
)

const (
	ExpectedContentType = "application/json"
	ExpectedResourcePath = "bob"
)

func TestAddAdminHandlers(t *testing.T) {
	s := &mockS3Client{}
	r := mux.NewRouter()
	AddAdminHandlers(r, s, "bucketName")

	t.Run(status.PingPath, func(t *testing.T) {
		assertRequestAndResponse(t, status.PingPath, 200, "pong")
	})

	t.Run(status.PingPathDW, func(t *testing.T) {
		assertRequestAndResponse(t, status.PingPathDW, 200, "pong")
	})

	t.Run(status.BuildInfoPath, func(t *testing.T) {
		assertRequestAndResponse(t, status.BuildInfoPath, 200, "")
	})

	t.Run(status.BuildInfoPathDW, func(t *testing.T) {
		assertRequestAndResponse(t, status.BuildInfoPathDW, 200, "")
	})

	t.Run("/__health good", func(t *testing.T) {
		rec := assertRequestAndResponse(t, "/__health", 200, "")
		assert.Equal(t, "bucketName", *s.headBucketInput.Bucket)
		body := rec.Body.String()
		log.Infof("Body was %v", body)
		assert.Contains(t, body, "\"S3 Bucket check\",\"ok\":true")
	})

	t.Run("/__gtg good", func(t *testing.T) {
		assertRequestAndResponse(t, "/__gtg", 200, "OK")
	})

	t.Run("/__health bad", func(t *testing.T) {
		s.s3error = errors.New("S3 error")
		rec := assertRequestAndResponse(t, "/__health", 200, "")
		assert.Equal(t, "bucketName", *s.headBucketInput.Bucket)
		body := rec.Body.String()
		log.Infof("Body was %v", body)
		assert.Contains(t, body, "\"S3 Bucket check\",\"ok\":false")
	})

	t.Run("/__gtg bad", func(t *testing.T) {
		errMsg := "Head request to S3 failed"
		s.s3error = errors.New("head failed")
		rec := assertRequestAndResponse(t, "/__gtg", 503, "")
		body := rec.Body.String()
		assert.Contains(t, body, errMsg)
	})
}

func TestRequestUrlMatchesResourcePathShouldHaveSuccessfulResponse(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{}
	wh := NewWriterHandler(mw, mr)
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleContentWrite),
		"GET":    http.HandlerFunc(rh.HandleContentGet),
		"DELETE": http.HandlerFunc(wh.HandleContentDelete),
	}
	Handlers(r, conceptMethodHandler, "", "/{filename}")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", "/22f53313-85c6-46b2-94e7-cfde9322f26c?date=2017-10-20", "PAYLOAD"))
	assert.Equal(t, 200, rec.Code)
}

func TestRequestUrlDoesNotMatchResourcePathShouldHaveNotFoundResponse(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{}
	wh := NewWriterHandler(mw, mr)
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleContentWrite),
		"GET":    http.HandlerFunc(rh.HandleContentGet),
		"DELETE": http.HandlerFunc(wh.HandleContentDelete),
	}
	Handlers(r, conceptMethodHandler, "/test", "/{filename}")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", "/22f53313-85c6-46b2-94e7-cfde9322f26c", "PAYLOAD"))
	assert.Equal(t, 404, rec.Code)
}

func TestWriteHandlerNewConceptReturnsCreated(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{}
	wh := NewWriterHandler(mw, mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleConceptWrite),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", withExpectedResourcePath("/organisations"), "PAYLOAD"))

	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, "{\"message\":\"UPDATED\"}", rec.Body.String())
}

func TestWriteHandlerNewContentReturnsCreated(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{}
	wh := NewWriterHandler(mw, mr)
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleContentWrite),
		"GET":    http.HandlerFunc(rh.HandleContentGet),
		"DELETE": http.HandlerFunc(wh.HandleContentDelete),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c?date=2017-09-10"), "PAYLOAD"))

	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, "PAYLOAD", mw.payload)
	assert.Equal(t, "22f53313-85c6-46b2-94e7-cfde9322f26c", mw.name)
	assert.Equal(t, ExpectedContentType, mw.ct)
	assert.Equal(t, "{\"message\":\"UPDATED\"}", rec.Body.String())
}

func TestWriteHandlerUpdateContentReturnsOK(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{found: true}
	wh := NewWriterHandler(mw, mr)
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleContentWrite),
		"GET":    http.HandlerFunc(rh.HandleContentGet),
		"DELETE": http.HandlerFunc(wh.HandleContentDelete),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", withExpectedResourcePath("/89d15f70-640d-11e4-9803-0800200c9a66?date=2017-09-10"), "PAYLOAD"))

	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, "PAYLOAD", mw.payload)
	assert.Equal(t, "89d15f70-640d-11e4-9803-0800200c9a66", mw.name)
	assert.Equal(t, ExpectedContentType, mw.ct)
	assert.Equal(t, "{\"message\":\"UPDATED\"}", rec.Body.String())
}

func TestWriteHandlerUpdateContentReturnsBadRequest(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{found: true}
	wh := NewWriterHandler(mw, mr)
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleContentWrite),
		"GET":    http.HandlerFunc(rh.HandleContentGet),
		"DELETE": http.HandlerFunc(wh.HandleContentDelete),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", withExpectedResourcePath("/89d15f70-640d-11e4-9803-0800200c9a66"), "PAYLOAD"))

	assert.Equal(t, 400, rec.Code)
	assert.Equal(t, "{\"message\":\"Required query param 'date' was not provided.\"}", rec.Body.String())
}

func TestWriterHandlerFailReadingBody(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{}
	wh := NewWriterHandler(mw, mr)
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleContentWrite),
		"GET":    http.HandlerFunc(rh.HandleContentGet),
		"DELETE": http.HandlerFunc(wh.HandleContentDelete),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequestBodyFail("PUT", withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c?date=2017-09-10")))
	assert.Equal(t, 500, rec.Code)
	assert.Equal(t, "{\"message\":\"Unknown internal error\"}", rec.Body.String())
}

func TestWriterHandlerFailWrite(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{returnError: errors.New("error writing")}
	mr := &mockReader{}
	wh := NewWriterHandler(mw, mr)
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleContentWrite),
		"GET":    http.HandlerFunc(rh.HandleContentGet),
		"DELETE": http.HandlerFunc(wh.HandleContentDelete),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("PUT", withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c?date=2017-09-10"), "PAYLOAD"))
	assert.Equal(t, 503, rec.Code)
	assert.Equal(t, "{\"message\":\"Service currently unavailable\"}", rec.Body.String())
}

func TestWriterConceptHandlerDeleteReturnsOK(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{}
	wh := NewWriterHandler(mw, mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"DELETE": http.HandlerFunc(wh.HandleConceptDelete),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("DELETE", withExpectedResourcePath("/fileName.txt"), ""))
	assert.Equal(t, "fileName.txt", mw.name)
	assert.Equal(t, 204, rec.Code)
	assert.Empty(t, rec.Body.String())
}

func TestWriterHandlerDeleteReturnsOK(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{}
	mr := &mockReader{}
	wh := NewWriterHandler(mw, mr)
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleContentWrite),
		"GET":    http.HandlerFunc(rh.HandleContentGet),
		"DELETE": http.HandlerFunc(wh.HandleContentDelete),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("DELETE", withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c"), ""))
	assert.Equal(t, "22f53313-85c6-46b2-94e7-cfde9322f26c", mw.name)
	assert.Equal(t, 204, rec.Code)
	assert.Empty(t, rec.Body.String())
}

func TestWriterHandlerDeleteFailsReturns503(t *testing.T) {
	r := mux.NewRouter()
	mw := &mockWriter{returnError: errors.New("Some error from writer")}
	mr := &mockReader{}
	wh := NewWriterHandler(mw, mr)
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleContentWrite),
		"GET":    http.HandlerFunc(rh.HandleContentGet),
		"DELETE": http.HandlerFunc(wh.HandleContentDelete),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("DELETE", withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c"), ""))
	assert.Equal(t, 503, rec.Code)
	assert.Equal(t, "{\"message\":\"Service currently unavailable\"}", rec.Body.String())
}

func TestReadHandlerForUUID(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{payload: "Some content", returnCT: "return/type"}
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"GET":    http.HandlerFunc(rh.HandleContentGet),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")
	assertRequestAndResponseFromRouter(t, r, withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c?date=2017-10-20"), 200, "Some content", "return/type")
}

func TestReadHandlerForUUIDAndNoContentType(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{payload: "Some content"}
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"GET":    http.HandlerFunc(rh.HandleContentGet),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")
	assertRequestAndResponseFromRouter(t, r, withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c?date=2017-10-20"), 200, "Some content", "")
}

func TestReadHandlerForUUIDNotFound(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{}
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"GET":    http.HandlerFunc(rh.HandleContentGet),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")
	assertRequestAndResponseFromRouter(t, r, withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c?date=2017-10-20"), 404, "{\"message\":\"Item not found\"}", ExpectedContentType)
}

func TestReadConceptHandlerForErrorFromReader(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{payload: "something came back but", returnError: errors.New("Some error from reader though")}
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"GET":    http.HandlerFunc(rh.HandleConceptGet),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")
	assertRequestAndResponseFromRouter(t, r, withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c?date=2017-10-20"), 503, "{\"message\":\"Service currently unavailable\"}", ExpectedContentType)
}

func TestReadHandlerForErrorFromReader(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{payload: "something came back but", returnError: errors.New("Some error from reader though")}
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"GET":    http.HandlerFunc(rh.HandleContentGet),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")
	assertRequestAndResponseFromRouter(t, r, withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c?date=2017-10-20"), 503, "{\"message\":\"Service currently unavailable\"}", ExpectedContentType)
}

func TestReadHandlerForErrorReadingBody(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{rc: &mockReaderCloser{err: errors.New("Some error")}}
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"GET":    http.HandlerFunc(rh.HandleContentGet),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")

	assertRequestAndResponseFromRouter(t, r, withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c?date=2017-10-20"), 502, "{\"message\":\"Error while communicating to other service\"}", ExpectedContentType)
}

func TestReadHandlerForMissingPublishedDate(t *testing.T) {
	r := mux.NewRouter()
	mr := &mockReader{payload: "Some content"}
	rh := NewReaderHandler(mr)
	conceptMethodHandler := &handlers.MethodHandler{
		"GET":    http.HandlerFunc(rh.HandleContentGet),
	}
	Handlers(r, conceptMethodHandler, ExpectedResourcePath, "/{filename}")
	assertRequestAndResponseFromRouter(t, r, withExpectedResourcePath("/22f53313-85c6-46b2-94e7-cfde9322f26c"), 400, "{\"message\":\"Required query param 'date' was not provided.\"}", ExpectedContentType)
}

func assertRequestAndResponseFromRouter(t testing.TB, r *mux.Router, url string, expectedStatus int, expectedBody string, expectedContentType string) *httptest.ResponseRecorder {

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", url, ""))
	assert.Equal(t, expectedStatus, rec.Code)
	if expectedBody != "" {
		assert.Equal(t, expectedBody, rec.Body.String())
	}
	ct, ok := rec.HeaderMap["Content-Type"]
	assert.True(t, ok)
	assert.Equal(t, expectedContentType, ct[0])

	return rec
}

func assertRequestAndResponse(t testing.TB, url string, expectedStatus int, expectedBody string) *httptest.ResponseRecorder {

	rec := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(rec, newRequest("GET", url, ""))
	assert.Equal(t, expectedStatus, rec.Code)
	if expectedBody != "" {
		assert.Equal(t, expectedBody, rec.Body.String())
	}

	return rec
}

type mockReaderCloser struct {
	err error
	n   int
}

func (mr *mockReaderCloser) Read(p []byte) (int, error) {
	return mr.n, mr.err
}

func (mr *mockReaderCloser) Close() error {
	return mr.err
}

func newRequestBodyFail(method, url string) *http.Request {
	mr := &mockReaderCloser{err: errors.New("Badbody")}
	r := io.Reader(mr)
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		panic(err)
	}
	return req
}

func newRequest(method, url string, body string) *http.Request {
	var payload io.Reader
	if body != "" {
		payload = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, url, payload)
	req.Header = map[string][]string{
		"Content-Type": {ExpectedContentType},
	}
	if err != nil {
		panic(err)
	}
	return req
}

type mockReader struct {
	sync.Mutex
	found       bool
	name        string
	payload     string
	rc          io.ReadCloser
	returnError error
	returnCT    string
}

func (r *mockReader) GetPublishDateForUUID(uuid string) (string, bool, error) {
	return "", true, nil
}

func (r *mockReader) GetContent(uuid, publishedDate string) (bool, io.ReadCloser, *string, error) {
	r.Lock()
	defer r.Unlock()
	log.Infof("Got request for uuid: %v", uuid)
	r.name = uuid
	var body io.ReadCloser

	if r.payload != "" {
		body = ioutil.NopCloser(strings.NewReader(r.payload))
	}

	if r.rc != nil {
		body = r.rc
	}

	return r.payload != "" || r.rc != nil, body, &r.returnCT, r.returnError
}

func (r *mockReader) processPipe() (io.PipeReader, error) {
	pv, pw := io.Pipe()
	go func(p *io.PipeWriter) {
		if r.payload != "" {
			p.Write([]byte(r.payload))
		}
		p.Close()
	}(pw)
	return *pv, r.returnError
}

type mockWriter struct {
	sync.Mutex
	name        string
	payload     string
	returnError error
	deleteError error
	ct          string
	tid         string
	writeCalled bool
}

func (mw *mockWriter) DeleteConcept(fileName string) error {
	mw.Lock()
	defer mw.Unlock()
	mw.name = fileName
	if mw.returnError != nil {
		return mw.returnError
	}
	return mw.deleteError
}
func (mw *mockWriter) WriteConcept(fileName string, b *[]byte, ct string, tid string) error {
	mw.Lock()
	defer mw.Unlock()
	mw.name = fileName
	mw.payload = string((*b)[:])
	mw.ct = ct
	mw.tid = tid
	mw.writeCalled = true
	return mw.returnError
}

func (r *mockReader) GetConcept(fileName string) (bool, io.ReadCloser, *string, error) {
	r.Lock()
	defer r.Unlock()
	log.Infof("Got request for fileName: %v", fileName)
	r.name = fileName
	var body io.ReadCloser

	if r.payload != "" {
		body = ioutil.NopCloser(strings.NewReader(r.payload))
	}

	if r.rc != nil {
		body = r.rc
	}

	return true, body, &r.returnCT, r.returnError
}

func (mw *mockWriter) DeleteContent(uuid, publishedDate string) error {
	mw.Lock()
	defer mw.Unlock()
	mw.name = uuid
	if mw.returnError != nil {
		return mw.returnError
	}
	return mw.deleteError
}

func (mw *mockWriter) WriteContent(uuid, publishedDate string, b *[]byte, ct string, tid string) error {
	mw.Lock()
	defer mw.Unlock()
	mw.name = uuid
	mw.payload = string((*b)[:])
	mw.ct = ct
	mw.tid = tid
	mw.writeCalled = true
	return mw.returnError
}

func withExpectedResourcePath(endpoint string) string {
	return "/" + ExpectedResourcePath + endpoint
}
